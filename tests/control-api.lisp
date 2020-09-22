(defpackage monomyth/tests/control-api
  (:use :cl :rove :monomyth/control-api/main :bordeaux-threads :monomyth/mmop))
(in-package :monomyth/tests/control-api)

(defparameter *api-port* 8888)
(defparameter *master-port* 5000)
(defparameter *master-uri* (format nil "localhost:~a" *master-port*))

(deftest startup
  (make-thread
   #'(lambda ()
       (pzmq:with-context nil
         (pzmq:with-socket master :router
           (pzmq:bind master (format nil "tcp://*:~a" *master-port*))

           (adt:match mmop-m:received-mmop (mmop-m:pull-master-message master)
             ((mmop-m:ping-v0 client-id) (send-msg master *mmop-v0* (mmop-m:pong-v0 client-id)))
             (_ (fail "unexpected message type")))))))

  (start-server *master-uri* *api-port*)
  (stop-server)
  (pass "server shutdown"))
