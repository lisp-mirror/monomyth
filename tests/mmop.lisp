(defpackage monomyth/tests/mmop
  (:use :cl :prove :monomyth/mmop))
(in-package :monomyth/tests/mmop)

(plan nil)

(defun build-uri ()
  (format nil "ipc://test-~a.ipc" (uuid:make-v4-uuid)))

(subtest "msg-happy-path-to-router"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
        (test-frames '("1" "2" "3"))
        (uri (build-uri)))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client uri)
        (pzmq:bind server uri)

        (send-msg client "mmop/test" test-frames)
        (is (pull-msg server)
            (cons client-name test-frames))))))

(subtest "msg-happy-path-to-router-with-second-message"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
        (uri (build-uri))
        (test-frames '("1" "2" "3")))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client uri)
        (pzmq:bind server uri)

        (send-msg client "mmop/test" test-frames)
        (send-msg client "mmop/test" '("test"))
        (is (pull-msg server)
            (cons client-name test-frames))))))

(subtest "msg-happy-path-to-dealer"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
        (uri (build-uri))
        (test-frames '("1" "2" "3")))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client uri)
        (pzmq:bind server uri)

        (send-msg client "mmop/test" '("READY"))
        (pull-msg server)
        (send-msg server "mmop/test" (cons client-name test-frames))
        (is (pull-msg client) test-frames)))))

(subtest "msg-happy-path-to-dealer-with-second-msg"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
        (uri (build-uri))
        (test-frames '("1" "2" "3")))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client uri)
        (pzmq:bind server uri)

        (send-msg client "mmop/test" '("READY"))
        (pull-msg server)
        (send-msg server "mmop/test" (cons client-name test-frames))
        (send-msg server "mmop/test" `(,client-name "test"))
        (is (pull-msg client) test-frames)))))

(subtest "MMOP/0 worker-ready"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
        (uri (build-uri)))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client uri)
        (pzmq:bind server uri)

        (mmop-w:send-worker-message client (mmop-w:make-worker-ready-v0))
        (let ((res (mmop-m:pull-master-message server)))
          (is (type-of res) 'mmop-m:worker-ready-v0)
          (is (mmop-m:worker-ready-v0-client-id res) client-name))))))

(finalize)
