(defpackage monomyth/control-api/main
  (:use :cl :ningle :clack :uuid :monomyth/mmop-control :monomyth/mmop
   :bordeaux-threads :jonathan :stmx)
  (:export run-control-server
           start-control-server
           stop-control-server))
(in-package :monomyth/control-api/main)

(defvar *zmq-context*)

(defparameter *server* (make-instance 'app))

(defun build-api-name ()
  "constructs a unique api identity to ensure that the response returns to the
right handler"
  (format nil "control-api:~a" (make-v4-uuid)))

(defun respond (body &key (type "application/json") (status 200) headers)
  "a helper function to construct clack responses"
  `(,status ,(append `(:conten-type ,type) headers) (,body)))

(defmacro with-control-socket ((socket uri) &rest forms)
  "a helper macro that build and connects a socket"
  `(pzmq:with-socket (,socket *zmq-context*) :dealer
     (pzmq:setsockopt ,socket :identity (build-api-name))
     (pzmq:connect ,socket ,uri)

     ,@forms))

(defun start-control-server (master-uri port &optional (new-thread t))
  "build all the endpoints for a control server and then returns the handler"
  (v:info :control-api "starting server on ~d" port)

  (setf *zmq-context* (pzmq:ctx-new))

  (with-control-socket (master master-uri)
    (send-msg master *mmop-v0* mmop-c:ping-v0)
    (adt:match received-mmop (pull-control-message master)
      ((pong-v0) (v:info :control-api "master server is up"))
      (_ (error "unexpected message in start up"))))

  (setf (route *server* "/ping" :method :GET)
        #'(lambda (params)
            (declare (ignore params))
            (with-control-socket (master master-uri)
              (send-msg master *mmop-v0* mmop-c:ping-v0)
              (adt:match received-mmop (pull-control-message master)
                ((pong-v0) (respond "pong" :type "plain/test"))
                (_ (v:error :control-api.ping "unexpected MMOP message")
                   (respond "{}" :status 500))))))

  (setf (route *server* "/start-node/:node-type" :method :POST)
        #'(lambda (params)
            (let ((node-type (cdr (assoc :node-type params))))
              (with-control-socket (master master-uri)
                (send-msg master *mmop-v0* (mmop-c:start-node-request-v0 node-type))
                (adt:match received-mmop (pull-control-message master)
                  ((start-node-request-success-v0)
                   (respond (to-json '(:|request_sent_to_worker| t)) :status 201))

                  ((start-node-request-failure-v0 msg code)
                   (respond
                    (to-json `(:|request_sent_to_worker| :false :|error_message| ,msg))
                    :status code))

                  (_ (v:error :control-api.ping "unexpected MMOP message")
                     (respond "{}" :status 500)))))))

  (setf (route *server* "/recipe-info" :method :GET)
        #'(lambda (params)
            (declare (ignore params))
            (with-control-socket (master master-uri)
              (send-msg master *mmop-v0* mmop-c:recipe-info-v0)
              (adt:match received-mmop (pull-control-message master)
                ((json-info-response-v0 json) (respond json))
                (_ (v:error :control-api.recipe-info "unexpected MMOP message")
                   (respond "{}" :status 500))))))

  (setf (route *server* "/worker-info" :method :GET)
        #'(lambda (params)
            (declare (ignore params))
            (with-control-socket (master master-uri)
              (send-msg master *mmop-v0* mmop-c:worker-info-v0)
              (adt:match received-mmop (pull-control-message master)
                ((json-info-response-v0 json) (respond json))
                (_ (v:error :control-api.worker-info "unexpected MMOP message")
                   (respond "{}" :status 500))))))

  (setf (route *server* "/stop-worker/:worker-id" :method :POST)
        #'(lambda (params)
            (let ((worker-id (cdr (assoc :worker-id params))))
              (with-control-socket (master master-uri)
                (send-msg master *mmop-v0* (mmop-c:stop-worker-request-v0 worker-id))
                (adt:match received-mmop (pull-control-message master)
                  ((stop-worker-request-success-v0)
                   (respond (to-json '(:|request_sent_to_worker| t))
                            :status 201))

                  ((stop-worker-request-failure-v0 msg code)
                   (respond
                    (to-json `(:|request_sent_to_worker| :false :|error_message| ,msg))
                    :status code))

                  (_ (v:error :control-api.ping "unexpected MMOP message")
                     (respond "{}" :status 500)))))))

  (clack:clackup *server* :server :woo
                          :port port
                          :address "0.0.0.0"
                          :use-thread new-thread
                          :use-default-middlewares nil))

(defun stop-control-server (handler)
  "stops the control server"
  (v:info :control-api "stopping server")
  (pzmq:ctx-destroy *zmq-context*)
  (setf *zmq-context* nil)
  (clack:stop handler))

(defun run-control-server (master-uri port)
  "starts a control server and then stops it on interrupt"
  (let ((handler))
    (handler-case
        (setf handler (start-control-server master-uri port nil))

      (sb-sys:interactive-interrupt ()
        (progn (stop-control-server handler)
               (uiop:quit)))

      (error (c)
        (progn (v:error :control-api "unexpected error: ~a" c)
               (when handler (stop-control-server handler))
               (uiop:quit 1))))))
