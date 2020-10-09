(defpackage monomyth/control-api/main
  (:use :cl :lucerne :uuid :monomyth/mmop-control :monomyth/mmop
   :bordeaux-threads :jonathan :stmx)
  (:import-from :lucerne.views :define-route)
  (:export run-server
           start-server
           stop-server))
(in-package :monomyth/control-api/main)

(defvar *zmq-context*)

(defapp server)

(defun build-api-name ()
  (format nil "control-api:~a" (make-v4-uuid)))

(defun start-server (master-uri port)
  (v:info :control-api "starting server on ~d" port)

  (setf *zmq-context* (pzmq:ctx-new))

  (pzmq:with-socket (master *zmq-context*) :dealer
    (pzmq:setsockopt master :identity (build-api-name))
    (pzmq:connect master master-uri)

    (send-msg master *mmop-v0* mmop-c:ping-v0)
    (adt:match received-mmop (pull-control-message master)
      ((pong-v0) (v:info :control-api "master server is up"))
      (_ (error "unexpected message in start up"))))

  (define-route server "/ping" :get
    (defview ping ()
      (pzmq:with-socket (master *zmq-context*) :dealer
        (pzmq:setsockopt master :identity (build-api-name))
        (pzmq:connect master master-uri)

        (send-msg master *mmop-v0* mmop-c:ping-v0)
        (adt:match received-mmop (pull-control-message master)
          ((pong-v0) (respond "pong"))
          (_ (v:error :control-api.ping "unexpected MMOP message")
             (respond nil :status 500))))))

  (define-route server "/start-node/:node-type" :post
    (defview start-node (node-type)
      (pzmq:with-socket (master *zmq-context*) :dealer
        (pzmq:setsockopt master :identity (build-api-name))
        (pzmq:connect master master-uri)

        (send-msg master *mmop-v0* (mmop-c:start-node-request-v0 node-type))
        (adt:match received-mmop (pull-control-message master)
          ((start-node-request-success-v0)
           (respond (to-json '(:|request_sent_to_worker| t))
                    :type "application/json"
                    :status 201))

          ((start-node-request-failure-v0 msg)
           (respond (to-json `(:|request_sent_to_worker| :false
                                :|error_message| ,msg))
                    :type "application/json"
                    :status 400))

          (_ (v:error :control-api.ping "unexpected MMOP message")
             (respond nil :status 500))))))

  (define-route server "/recipe-info" :get
    (defview recipe-info ()
      (pzmq:with-socket (master *zmq-context*) :dealer
        (pzmq:setsockopt master :identity (build-api-name))
        (pzmq:connect master master-uri)

        (send-msg master *mmop-v0* mmop-c:recipe-info-v0)
        (adt:match received-mmop (pull-control-message master)
          ((recipe-info-response-v0 json) (respond json))
          (_ (v:error :control-api.recipe-info "unexpected MMOP message")
             (respond nil :status 500))))))

  (define-route server "/stop-worker/:worker-id" :post
    (defview stop-worker (worker-id)
      (pzmq:with-socket (master *zmq-context*) :dealer
        (pzmq:setsockopt master :identity (build-api-name))
        (pzmq:connect master master-uri)

        (send-msg master *mmop-v0* (mmop-c:stop-worker-request-v0 worker-id))
        (adt:match received-mmop (pull-control-message master)
          ((stop-worker-request-success-v0)
           (respond (to-json '(:|request_sent_to_worker| t))
                    :type "application/json"
                    :status 201))
          ((stop-worker-request-failure-v0 msg code)
           (respond (to-json `(:|request_sent_to_worker| :false
                                :|error_message| ,msg))
                    :type "application/json"
                    :status code))
          (_ (v:error :control-api.stop-worker "unexpected MMOP message")
             (respond nil :status 500))))))

  (start server :server :woo :port port))

(defun stop-server ()
  "stops the control server"
  (v:info :control-api "stopping server")
  (pzmq:ctx-destroy *zmq-context*)
  (setf *zmq-context* nil)
  (stop server))

(defparameter *thread-name* "clack-handler-woo")

(defun run-server ()
  (start-server "localhost:7889" 17889)

  (handler-case
      (join-thread (find-if #'(lambda (th) (string= *thread-name* (thread-name th)))
                            (all-threads)))

    (sb-sys:interactive-interrupt ()
      (progn (stop-server)
             (uiop:quit)))

    (error (c)
      (progn (v:error :control-api "unexpected error: ~a" c)
             (stop-server)
             (uiop:quit 1)))))
