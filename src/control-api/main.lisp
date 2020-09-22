(defpackage monomyth/control-api/main
  (:use :cl :lucerne :uuid :monomyth/mmop-control :monomyth/mmop
   :bordeaux-threads)
  (:export run-server
           start-server
           stop-server))
(in-package :monomyth/control-api/main)

(defapp server)

(defun build-api-name ()
  (format nil "control-api:~a" (make-v4-uuid)))

(defun start-server (master-uri port)
  (v:info :control-api "starting server on ~d" port)

  (pzmq:with-context nil
    (pzmq:with-socket master :dealer
      (pzmq:setsockopt master :identity (build-api-name))
      (pzmq:connect master (format nil "tcp://~a" master-uri))

      (send-msg master *mmop-v0* mmop-c:ping-v0)
      (adt:match received-mmop (pull-control-message master)
        ((pong-v0) (v:info :control-api "master server is up"))
        (_ (error "unexpected message in start up")))))

  (start server :server :woo :port port))

(defun stop-server ()
  "stops the control server"
  (v:info :control-api "stopping server")
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
