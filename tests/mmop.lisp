(defpackage monomyth/tests/mmop
  (:use :cl :prove :monomyth/mmop :monomyth/rmq-node-recipe :monomyth/node-recipe))
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
        (pzmq:connect client "tcp://localhost:55555")
        (pzmq:bind server "tcp://*:55555")

        (send-msg-frames client "mmop/test" test-frames)
        (is (pull-msg server)
            (cons client-name test-frames))))))

(subtest "msg-happy-path-to-router-with-second-message"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
        (test-frames '("1" "2" "3")))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client "tcp://localhost:55555")
        (pzmq:bind server "tcp://*:55555")

        (send-msg-frames client "mmop/test" test-frames)
        (send-msg-frames client "mmop/test" '("test"))
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
        (pzmq:connect client "tcp://localhost:55555")
        (pzmq:bind server "tcp://*:55555")

        (send-msg-frames client "mmop/test" '("READY"))
        (pull-msg server)
        (send-msg-frames server "mmop/test" (cons client-name test-frames))
        (is (pull-msg client) test-frames)))))

(subtest "msg-happy-path-to-dealer-with-second-msg"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
        (test-frames '("1" "2" "3")))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client "tcp://localhost:55555")
        (pzmq:bind server "tcp://*:55555")

        (send-msg-frames client "mmop/test" '("READY"))
        (pull-msg server)
        (send-msg-frames server "mmop/test" (cons client-name test-frames))
        (send-msg-frames server "mmop/test" `(,client-name "test"))
        (is (pull-msg client) test-frames)))))

(subtest "MMOP/0 worker-ready"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client "tcp://localhost:55555")
        (pzmq:bind server "tcp://*:55555")

        (send-msg client *mmop-v0* (mmop-w:make-worker-ready-v0))
        (let ((res (mmop-m:pull-master-message server)))
          (is-type res 'mmop-m:worker-ready-v0)
          (is (mmop-m:worker-ready-v0-client-id res) client-name))))))

(subtest "MMOP/0 start-node"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
        (recipe (build-rmq-node-recipe :test "#'(lambda (x) (1+ x))" "test-s" "test-d")))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client "tcp://localhost:55555")
        (pzmq:bind server "tcp://*:55555")

        (send-msg client *mmop-v0* (mmop-w:make-worker-ready-v0))
        (mmop-m:pull-master-message server)
        (send-msg server *mmop-v0* (mmop-m:make-start-node-v0 client-name recipe))
        (let ((res (mmop-w:pull-worker-message client)))
          (is-type res 'mmop-w:start-node-v0)
          (is (mmop-w:start-node-v0-type res) "TEST")
          (let ((got-res (mmop-w:start-node-v0-recipe res)))
            (is (node-recipe/type got-res) (node-recipe/type recipe))
            (is (node-recipe/transform-fn got-res) (node-recipe/transform-fn recipe))
            (is (rmq-node-recipe/source-queue got-res) (rmq-node-recipe/source-queue recipe))
            (is (rmq-node-recipe/dest-queue got-res) (rmq-node-recipe/dest-queue recipe))))))))

(subtest "MMOP/0 node-start-success"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client "tcp://localhost:55555")
        (pzmq:bind server "tcp://*:55555")

        (send-msg client *mmop-v0* (mmop-w:make-start-node-success-v0 "TEST"))
        (let ((res (mmop-m:pull-master-message server)))
          (is-type res 'mmop-m:start-node-success-v0)
          (is (mmop-m:start-node-success-v0-client-id res) client-name)
          (is (mmop-m:start-node-success-v0-type res) "TEST"))))))

(subtest "MMOP/0 node-start-failure"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client "tcp://localhost:55555")
        (pzmq:bind server "tcp://*:55555")

        (send-msg client *mmop-v0* (mmop-w:make-start-node-failure-v0
                                    "TEST" "test" "test-msg"))
        (let ((res (mmop-m:pull-master-message server)))
          (is-type res 'mmop-m:start-node-failure-v0)
          (is (mmop-m:start-node-failure-v0-client-id res) client-name)
          (is (mmop-m:start-node-failure-v0-type res) "TEST")
          (is (mmop-m:start-node-failure-v0-reason-cat res) "test")
          (is (mmop-m:start-node-failure-v0-reason-msg res) "test-msg"))))))

(subtest "MMOP/0 stop-worker"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-sockets ((server :router) (client :dealer))
        (pzmq:setsockopt server :identity server-name)
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client "tcp://localhost:55555")
        (pzmq:bind server "tcp://*:55555")

        (send-msg client *mmop-v0* (mmop-w:make-worker-ready-v0))
        (mmop-m:pull-master-message server)
        (send-msg server *mmop-v0* (mmop-m:make-shutdown-worker-v0 client-name))
        (is-type (mmop-w:pull-worker-message client) 'mmop-w:shutdown-worker-v0)))))

(finalize)
