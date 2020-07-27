(defpackage monomyth/tests/mmop
  (:use :cl :rove :monomyth/mmop :monomyth/rmq-node-recipe :monomyth/node-recipe))
(in-package :monomyth/tests/mmop)

(deftest to-router
  (testing "single message"
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
          (ok (equal (pull-msg server)
                     (cons client-name test-frames)))))))

  (testing "double messages"
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
          (ok (equal (pull-msg server)
                     (cons client-name test-frames))))))))

(deftest to-dealer
  (testing "single messages"
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
          (ok (equal (pull-msg client) test-frames))))))

  (testing "double messages"
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
          (ok (equal (pull-msg client) test-frames)))))))

(deftest MMOP/0
  (testing "worker-ready"
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
            (ok (eq (type-of res) 'mmop-m:worker-ready-v0))
            (ok (string= (mmop-m:worker-ready-v0-client-id res) client-name)))))))

  (testing "start-node"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
          (recipe (make-instance 'rmq-node-recipe :dest "test-d" :source "test-s"
                                                  :type :test :batch-size 7)))
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
            (ok (eq (type-of res) 'mmop-w:start-node-v0))
            (ok (string= (mmop-w:start-node-v0-type res) "TEST"))
            (let ((got-res (mmop-w:start-node-v0-recipe res)))
              (ok (eq (node-recipe/type got-res) (node-recipe/type recipe)))
              (ok (string= (rmq-node-recipe/source-queue got-res) (rmq-node-recipe/source-queue recipe)))
              (ok (string= (rmq-node-recipe/dest-queue got-res) (rmq-node-recipe/dest-queue recipe)))))))))

  (testing "node-start-success"
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
            (ok (eq (type-of res) 'mmop-m:start-node-success-v0))
            (ok (string= (mmop-m:start-node-success-v0-client-id res) client-name))
            (ok (string= (mmop-m:start-node-success-v0-type res) "TEST")))))))

  (testing "node-start-failure"
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
            (ok (eq (type-of res) 'mmop-m:start-node-failure-v0))
            (ok (string= (mmop-m:start-node-failure-v0-client-id res) client-name))
            (ok (string= (mmop-m:start-node-failure-v0-type res) "TEST"))
            (ok (string= (mmop-m:start-node-failure-v0-reason-cat res) "test"))
            (ok (string= (mmop-m:start-node-failure-v0-reason-msg res) "test-msg")))))))

  (testing "stop-worker"
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
          (ok (eq (type-of (mmop-w:pull-worker-message client)) 'mmop-w:shutdown-worker-v0)))))))
