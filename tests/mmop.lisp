(defpackage monomyth/tests/mmop
  (:use :cl :prove :monomyth/mmop))
(in-package :monomyth/tests/mmop)

(plan nil)

(subtest "msg-happy-path-to-router"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
        (test-frames '("1" "2" "3")))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client "ipc://test.ipc")
        (pzmq:bind server "ipc://test.ipc")

        (send-msg "mmop/test" client test-frames)
        (is (pull-msg server)
            (cons client-name test-frames))))))

(subtest "msg-happy-path-to-router-second-message"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
        (test-frames '("1" "2" "3")))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client "ipc://test.ipc")
        (pzmq:bind server "ipc://test.ipc")

        (send-msg "mmop/test" client test-frames)
        (send-msg "mmop/test" client '("test"))
        (is (pull-msg server)
            (cons client-name test-frames))))))

(subtest "msg-happy-path-to-dealer"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
        (test-frames '("1" "2" "3")))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client "ipc://test.ipc")
        (pzmq:bind server "ipc://test.ipc")

        (format t "a1~%")
        (send-msg "mmop/test" server (append `(,client-name nil) test-frames))
        (format t "a2~%")
        (is (pull-msg client) test-frames)
        (format t "a3~%")
        ))))

(finalize)
