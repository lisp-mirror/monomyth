(defpackage monomyth/rmq-node
  (:use :cl :monomyth/node :cl-rabbit :stmx)
  (:shadow :closer-mop)
  (:export rmq-node
           setup-connection
           build-rmq-message
           rmq-message-body
           rmq-message-delivery-tag
           send-message
           get-message
           ack-message
           nack-message))
(in-package :monomyth/rmq-node)

(defparameter *get-timeout* 100)
(defparameter *channel* 1)

(transactional
    (defclass rmq-node (node)
      ((conn :initform (error "connection must be supplied")
             :initarg :conn
             :reader rmq-node/conn
             :transactional nil
             :documentation "the rmq connection, there should only be one per machine.
Due to the library we are using, there will be one per node")
       (exchange :initarg :exchange
                 :initform ""
                 :transactional nil
                 :reader rmq-node/exchange)
       (source-queue :initarg :source
                     :transactional nil
                     :initform (error "source queue must be set")
                     :reader rmq-node/source-queue)
       (dest-queue :initarg :dest
                   :initform nil
                   :transactional nil
                   :reader rmq-node/dest-queue)
       (fail-queue :initarg :fail
                   :transactional nil
                   :initform (error "failure queue must be set")
                   :reader rmq-node/fail-queue))
      (:documentation "a node type specially designed to work with rabbit mq")))

(defstruct (rmq-message (:constructor build-rmq-message))
  "central structure passed through the node"
  body (delivery-tag nil :read-only t))

(defun build-error-response (step items c)
  "helper function that constructs the error"
  (error 'node-error
         :message (format nil "rmq-error (~d): ~a"
                          (rabbitmq-library-error/error-code c)
                          (rabbitmq-library-error/error-description c))
         :step step :items items))

(defmacro rabbit-mq-call (step request &optional items)
  "wraps a rmq call to look for a standard error"
  `(handler-case ,request
     (rabbitmq-library-error (c) (build-error-response ,step ,items c))))

(defun setup-connection
    (&key (host "localhost") (port 5672) (username "guest") (password "guest")
       (vhost "/"))
  "builds a new connection, sets up the socket, and logs in
defaults are the local rabbit-mq defaults"
  (rabbit-mq-call
   :setup
   (let ((conn (new-connection)))
     (socket-open (tcp-socket-new conn) host port)
     (login-sasl-plain conn vhost username password)
     conn)))

(defmethod startup ((node rmq-node) &optional build-worker-thread)
  "opens a channel using the nodes connections after setting up the socket.
also ensures all three queues are up and sets up basic consume for the source queue"
  (declare (ignore build-worker-thread))
  (rabbit-mq-call
   :startup
   (progn
     (channel-open (rmq-node/conn node) *channel*)
     (queue-declare (rmq-node/conn node) *channel*
                    :queue (rmq-node/source-queue node))
     (when (rmq-node/dest-queue node)
       (queue-declare (rmq-node/conn node) *channel*
                      :queue (rmq-node/dest-queue node)))
     (queue-declare (rmq-node/conn node) *channel*
                    :queue (rmq-node/fail-queue node))
     (basic-consume (rmq-node/conn node) *channel*
                    (rmq-node/source-queue node)))))

(defmethod shutdown ((node rmq-node))
  "closes the channel and then destroys the connections.
note that this means that once an rmq-node is shutdown, it cannot be started up again"
  (rabbit-mq-call
   :shutdown
   (destroy-connection (rmq-node/conn node))))

(defun send-message (node queue message)
  "sends a message to the specified queue"
  (basic-publish (rmq-node/conn node) *channel*
                 :exchange (rmq-node/exchange node)
                 :routing-key queue
                 :body message))

(defun get-message (node)
  "gets a message off the source queue and changes the message to be a string
(as opposed to a byte array)
return :success t with the :result if things go well"
  (let ((msg (consume-message (rmq-node/conn node) :timeout *get-timeout*)))
    (build-rmq-message
     :body (babel:octets-to-string (message/body (envelope/message msg)) :encoding :utf-8)
     :delivery-tag (envelope/delivery-tag msg))))

(defun ack-message (node message)
  "acks a message as complete"
  (basic-ack (rmq-node/conn node) *channel*
             (rmq-message-delivery-tag message)))

(defun nack-message (node message requeue)
  "nacks a message as incomplete, re-queues message if asked to"
  (basic-nack (rmq-node/conn node) *channel*
              (rmq-message-delivery-tag message) :requeue requeue))

(defmethod pull-items ((node rmq-node))
  (iter:iterate
    (iter:finally (return items))
    (iter:repeat (node/batch-size node))
    (handler-case (iter:collect (get-message node) into items)
      (rabbitmq-library-error (c)
        (if (string= (rabbitmq-library-error/error-description c) "request timed out")
            (return-from pull-items items)
            (build-error-response :pull items c))))))

(defmethod transform-items ((node rmq-node) pulled)
  (handler-case
      (iter:iterate
        (iter:finally (return items))
        (iter:for item in pulled)
        (iter:collect (build-rmq-message
                       :body (transform-fn node (rmq-message-body item))
                       :delivery-tag (rmq-message-delivery-tag item))
          into items))
    (error (c)
      (error 'node-error :step :transform :items pulled
                         :message (format nil "~a" c)))))

(defmethod place-items ((node rmq-node) result)
  (iter:iterate
    (iter:for item in result)
    (iter:for i upfrom 0)
    (rabbit-mq-call
     :place
     (progn
       (when (node/place-destination node)
         (send-message node (rmq-node/dest-queue node) (rmq-message-body item)))
       (ack-message node item))
     (nthcdr i result))))

(defmethod handle-failure ((node rmq-node) step result)
  (if (member step '(:pull :transform :place))
      (iter:iterate
        (iter:for item in result)
        (handler-case
            (progn (send-message node (rmq-node/fail-queue node) (rmq-message-body item))
                   (nack-message node item nil))
          (rabbitmq-library-error (c)
            (v:error :node.event-loop "rmq-error- failed to place item (~d): ~a"
                       (rabbitmq-library-error/error-code c)
                       (rabbitmq-library-error/error-description c))
            (handler-case (nack-message node item t)
              (rabbitmq-library-error (c)
                (v:error :node.event-loop "rmq-error- failed to nack item (~d): ~a"
                           (rabbitmq-library-error/error-code c)
                           (rabbitmq-library-error/error-description c)))))))
      (error "unexpected step")))
