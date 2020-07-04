(defpackage monomyth/rmq-node
  (:use :cl :monomyth/node :cl-rabbit)
  (:export setup-connection
           make-rmq-node
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

(defclass rmq-node (node)
  ((conn :initform (error "connection must be supplied")
         :initarg :conn
         :reader rmq-node/conn
         :documentation "the rmq connection, there should only be one per machine.
Due to the library we are using, there will be one per node")
   (exchange :initarg :exchange
             :initform ""
             :reader rmq-node/exchange)
   (source-queue :initarg :source
                 :initform (error "source queue must be set")
                 :reader rmq-node/source-queue)
   (dest-queue :initarg :dest
               :initform (error "destination queue must be set")
               :reader rmq-node/dest-queue)
   (fail-queue :initarg :fail
               :initform (error "failure queue must be set")
               :reader rmq-node/fail-queue))
  (:documentation "a node type specially designed to work with rabbit mq"))

(defstruct (rmq-message (:constructor build-rmq-message))
  "central structure passed through the node"
  body (delivery-tag nil :read-only t))

(defun build-error-response (c)
  "helper function that constructs the error plist"
  `(:error ,(format nil "rmq-error (~d): ~a"
                    (rabbitmq-library-error/error-code c)
                    (rabbitmq-library-error/error-description c))))

(defmacro rabbit-mq-call (request no-error)
  "wraps a rmq call to look for a standard error"
  `(handler-case ,request
     (rabbitmq-library-error (c) (build-error-response c))
     ,no-error))

(defun setup-connection
    (&key (host "localhost") (port 5672) (username "guest") (password "guest") (vhost "/"))
  "builds a new connection, sets up the socket, and logs in
defaults are the local rabbit-mq defaults"
  (let ((conn (new-connection)))
     (socket-open (tcp-socket-new conn) host port)
     (login-sasl-plain conn vhost username password)
     conn))

(defun make-rmq-node
    (transform-fn type source-queue dest-queue fail-queue
     &key (host "localhost") (port 5672) (username "guest") (password "guest") (vhost "/")
       name exchange batch-size)
  (let ((args `(rmq-node :transform-fn ,transform-fn :type ,type
                         :conn ,(setup-connection :host host :port port :username username
                                                  :password password :vhost vhost)
                         :source ,source-queue :dest ,dest-queue :fail ,fail-queue)))
    (if name (setf args (append args `(:name ,name))))
    (if batch-size (setf args (append args `(:batch-size ,batch-size))))
    (if exchange (setf args (append args `(:exchange ,exchange))))
    (apply #'make-instance args)))

(defmethod startup ((node rmq-node) &optional build-worker-thread)
  "opens a channel using the nodes connections after setting up the socket.
also ensures all three queues are up and sets up basic consume for the source queue"
  (declare (ignore build-worker-thread))
  (rabbit-mq-call
   (progn
     (channel-open (rmq-node/conn node) *channel*)
     (queue-declare (rmq-node/conn node) *channel*
                    :queue (rmq-node/source-queue node))
     (queue-declare (rmq-node/conn node) *channel*
                    :queue (rmq-node/dest-queue node))
     (queue-declare (rmq-node/conn node) *channel*
                    :queue (rmq-node/fail-queue node))
     (basic-consume (rmq-node/conn node) *channel*
                    (rmq-node/source-queue node)))
   (:no-error (key) (declare (ignore key)) '(:success t))))

(defmethod shutdown ((node rmq-node))
  "closes the channel and then destroys the connections.
note that this means that once an rmq-node is shutdown, it cannot be started up again"
  (rabbit-mq-call
   (destroy-connection (rmq-node/conn node))
   (:no-error (res) (declare (ignore res)) '(:success t))))

(defun send-message (node queue message)
  "sends a message to the specified queue"
  (rabbit-mq-call
   (basic-publish (rmq-node/conn node) *channel*
                  :exchange (rmq-node/exchange node)
                  :routing-key queue
                  :body message)
   (:no-error (res) (if (eq res :amqp-status-ok) '(:success t) `(:error res)))))

(defun get-message (node)
  "gets a message off the source queue and changes the message to be a string
(as opposed to a byte array)
return :success t with the :result if things go well"
  (handler-case
      (let ((msg (consume-message (rmq-node/conn node) :timeout *get-timeout*)))
        (build-rmq-message
         :body (babel:octets-to-string (message/body (envelope/message msg)) :encoding :utf-8)
         :delivery-tag (envelope/delivery-tag msg)))
    (rabbitmq-library-error (c)
      (if (string= (rabbitmq-library-error/error-description c) "request timed out")
          `(:timeout t :success t)
          (build-error-response c)))
    (:no-error (msg) `(:result ,msg :success t))))

(defun ack-message (node message)
  "acks a message as complete"
  (rabbit-mq-call
   (basic-ack (rmq-node/conn node) *channel*
              (rmq-message-delivery-tag message))
   (:no-error (res) (declare (ignore res)) '(:success t))))

(defun nack-message (node message requeue)
  "nacks a message as incomplete, re-queues message if asked to"
  (rabbit-mq-call
   (basic-nack (rmq-node/conn node) *channel*
               (rmq-message-delivery-tag message) :requeue requeue)
   (:no-error (res) (if (eq res :amqp-status-ok) '(:success t) `(:error ,res)))))

(defmethod pull-items ((node rmq-node))
  `(:success t
    :items ,(iter:iterate
              (iter:repeat (node/batch-size node))
              (let ((result (get-message node)))
                (cond ((getf result :timeout) (iter:finish))
                      ((getf result :success) (iter:collect (getf result :result)))
                      (t (return-from pull-items result)))))))

(defmethod transform-items ((node rmq-node) pulled)
  (handler-case
      (iter:iterate
        (iter:for item in (getf pulled :items))
        (iter:collect (build-rmq-message
                       :body (funcall (node/trans-fn node) (rmq-message-body item))
                       :delivery-tag (rmq-message-delivery-tag item))))
    (error (c)
      (vom:error "unexpected error in transformation ~a" c)
      `(:error ,c :items ,(getf pulled :items)))
    (:no-error (res) `(:success t :items ,res))))

(defmethod place-items ((node rmq-node) result)
  (iter:iterate
    (iter:for item in (getf result :items))
    (let ((send-res (send-message node (rmq-node/dest-queue node) (rmq-message-body item))))
      (if (getf send-res :success)
          (let ((ack-res (ack-message node item)))
            (if (not (getf ack-res :success))
                (return-from place-items (append `(:items ,(getf result :items)) ack-res))))
          (return-from place-items (append `(:items ,(getf result :items)) send-res)))))
  `(:success t))

(defmethod handle-failure ((node rmq-node) step result)
  (flet ((place-messages ()
           (iter:iterate
             (iter:for item in (getf result :items))
             (let ((send-res (send-message node (rmq-node/fail-queue node)
                                           (rmq-message-body item))))
               (if (getf send-res :success)
                   (nack-message node item nil)
                   (progn
                     (vom:error "message failed to enter fail queue: ~a"
                                (getf send-res :error))
                     (nack-message node item t)))))))
    (vom:error "step ~a: ~a" step (getf result :error))
    (case step
      (:pull result)
      (:transform (place-messages) result)
      (:place (place-messages) result)
      (otherwise (error "unexpected step")))))
