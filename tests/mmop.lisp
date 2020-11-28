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

  (testing "json-info-response/recipe"
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
                    (mmop-m:json-info-response-v0 client-name json-payload))
          (adt:match mmop-c:received-mmop (mmop-c:pull-control-message client)
            ((mmop-c:json-info-response-v0 payload) (ok (string= payload json-payload)))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "worker-info"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* mmop-c:worker-info-v0)
          (adt:match mmop-m:received-mmop (mmop-m:pull-master-message server)
            ((mmop-m:worker-info-v0 client-id) (ok (string= client-id client-name)))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "json-info-response/recipe"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
          (json-payload "{\"tests\":2}"))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* mmop-c:worker-info-v0)
          (mmop-m:pull-master-message server)
          (send-msg server *mmop-v0*
                    (mmop-m:json-info-response-v0 client-name json-payload))
          (adt:match mmop-c:received-mmop (mmop-c:pull-control-message client)
            ((mmop-c:json-info-response-v0 payload) (ok (string= payload json-payload)))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "start-node-request"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
          (req-type "test-type"))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 req-type))
          (adt:match mmop-m:received-mmop (mmop-m:pull-master-message server)
            ((mmop-m:start-node-request-v0 client-id sent-req-type)
             (ok (string= client-id client-name))
             (ok (string= sent-req-type req-type)))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "start-node-request-success"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
          (req-type "test-type"))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 req-type))
          (mmop-m:pull-master-message server)
          (send-msg server *mmop-v0* (mmop-m:start-node-request-success-v0 client-name))
          (adt:match mmop-c:received-mmop (mmop-c:pull-control-message client)
            ((mmop-c:start-node-request-success-v0) (pass "message received"))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "start-node-request-failure"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
          (req-type "test-type")
          (err-msg "test-error"))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 req-type))
          (mmop-m:pull-master-message server)
          (send-msg server *mmop-v0*
                    (mmop-m:start-node-request-failure-v0 client-name err-msg 409))
          (adt:match mmop-c:received-mmop (mmop-c:pull-control-message client)
            ((mmop-c:start-node-request-failure-v0 msg code)
             (ok (= code 409))
             (ok (string= msg err-msg)))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "stop-worker-request"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
          (req-id "test-id"))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* (mmop-c:stop-worker-request-v0 req-id))
          (adt:match mmop-m:received-mmop (mmop-m:pull-master-message server)
            ((mmop-m:stop-worker-request-v0 client-id req-type)
             (ok (string= client-id client-name))
             (ok (string= req-type req-id)))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "stop-worker-request-success"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
          (req-id "test-id"))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 req-id))
          (mmop-m:pull-master-message server)
          (send-msg server *mmop-v0*
                    (mmop-m:stop-worker-request-success-v0 client-name))
          (adt:match mmop-c:received-mmop (mmop-c:pull-control-message client)
            ((mmop-c:stop-worker-request-success-v0) (pass "message recieved"))
            (_ (fail "mmop message is of wrong type")))))))

  (testing "stop-worker-request-failure"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (server-name (format nil "server-~a" (uuid:make-v4-uuid)))
          (req-id "test-id")
          (error-msg "test-error"))
      (pzmq:with-context nil
        (pzmq:with-sockets ((server :router) (client :dealer))
          (pzmq:setsockopt server :identity server-name)
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client "tcp://localhost:55555")
          (pzmq:bind server "tcp://*:55555")

          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 req-id))
          (mmop-m:pull-master-message server)
          (send-msg server *mmop-v0*
                    (mmop-m:stop-worker-request-failure-v0 client-name error-msg 400))
          (adt:match mmop-c:received-mmop (mmop-c:pull-control-message client)
            ((mmop-c:stop-worker-request-failure-v0 msg code)
             (ok (string= msg error-msg))
             (ok (= code 400)))
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
          (recipe (make-instance 'rmq-node-recipe :type :test)))
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
               (ok (eq (node-recipe/type got-res) (node-recipe/type recipe)))))
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
