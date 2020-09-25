(defpackage monomyth/tests/mmop
  (:use :cl :rove :monomyth/mmop :monomyth/rmq-node-recipe :monomyth/node-recipe
        :monomyth/tests/utils))
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
  (testing "ping"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (control :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt control :identity client-name)
          (pzmq:connect control "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg control *mmop-v0* mmop-c:ping-v0)
          (adt:match mmop-m:received-mmop (mmop-m:pull-master-message server)
            ((mmop-m:ping-v0 client-id) (ok (string= client-id client-name)))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "pong"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (control :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt control :identity client-name)
          (pzmq:connect control "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg control *mmop-v0* mmop-c:ping-v0)
          (mmop-m:pull-master-message server)
          (send-msg server *mmop-v0* (mmop-m:pong-v0 client-name))
          (adt:match mmop-c:received-mmop (mmop-c:pull-control-message control)
            ((mmop-c:pong-v0) (pass "message received"))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "recipe-info"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* mmop-c:recipe-info-v0)
          (adt:match mmop-m:received-mmop (mmop-m:pull-master-message server)
            ((mmop-m:recipe-info-v0 client-id) (ok (string= client-id client-name)))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "recipe-info-response"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
          (json-payload "{\"tests\":2}"))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* mmop-c:recipe-info-v0)
          (mmop-m:pull-master-message server)
          (send-msg server *mmop-v0*
                    (mmop-m:recipe-info-response-v0 client-name json-payload))
          (adt:match mmop-c:received-mmop (mmop-c:pull-control-message client)
            ((mmop-c:recipe-info-response-v0 payload) (ok (string= payload json-payload)))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "worker-ready"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* mmop-w:worker-ready-v0)
          (adt:match mmop-m:received-mmop (mmop-m:pull-master-message server)
            ((mmop-m:worker-ready-v0 client-id) (ok (string= client-id client-name)))
            (_ (fail "mmop message is of wrong type")))))))

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

          (send-msg client *mmop-v0* mmop-w:worker-ready-v0)
          (mmop-m:pull-master-message server)
          (send-msg server *mmop-v0* (mmop-m:start-node-v0 client-name recipe))
          (adt:match mmop-w:received-mmop (mmop-w:pull-worker-message client)
            ((mmop-w:start-node-v0 rtype got-res)
             (progn
               (ok (string= rtype "TEST"))
               (ok (eq (node-recipe/type got-res) (node-recipe/type recipe)))
               (ok (string= (rmq-node-recipe/source-queue got-res)
                            (rmq-node-recipe/source-queue recipe)))
               (ok (string= (rmq-node-recipe/dest-queue got-res)
                            (rmq-node-recipe/dest-queue recipe)))))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "node-start-success"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* (mmop-w:start-node-success-v0 "TEST"))
          (adt:match mmop-m:received-mmop (mmop-m:pull-master-message server)
            ((mmop-m:start-node-success-v0 client-id rtype)
             (ok (string= client-id client-name))
             (ok (string= rtype "TEST")))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "node-start-failure"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* (mmop-w:start-node-failure-v0
                                      "TEST" "test" "test-msg"))
          (adt:match mmop-m:received-mmop (mmop-m:pull-master-message server)
            ((mmop-m:start-node-failure-v0 client-id rtype rcat rmsg)
             (ok (string= client-id client-name))
             (ok (string= rtype "TEST"))
             (ok (string= rcat "test"))
             (ok (string= rmsg "test-msg")))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "stop-worker"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* mmop-w:worker-ready-v0)
          (mmop-m:pull-master-message server)
          (send-msg server *mmop-v0* (mmop-m:shutdown-worker-v0 client-name))
          (adt:match mmop-w:received-mmop (mmop-w:pull-worker-message client)
            (mmop-w:shutdown-worker-v0 (pass "mmop message is of correct type"))
            (_ (fail "mmop message is of wrong type"))))))))
