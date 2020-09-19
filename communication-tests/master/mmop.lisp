(defpackage monomyth/communication-tests-master/mmop
  (:use :cl :rove :monomyth/mmop :monomyth/rmq-node-recipe :monomyth/node-recipe
        :rutils.bind))
(in-package :monomyth/communication-tests-master/mmop)

(deftest to-router
  (testing "single message"
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (ok (equal (cdr (pull-msg client)) '("1" "2" "3"))))))

  (testing "double message"
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (ok (equal (cdr (pull-msg client)) '("1" "2" "3")))))))

(sleep .1)

(deftest to-dealer
  (testing "single message"
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (with (((id &rest _) (pull-msg client)))
          (send-msg-frames client "mmop/test" `(,id "1" "2" "3")))
        (pass "message sent"))))

  (sleep .1)

  (testing "double message"
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (with (((id &rest _) (pull-msg client)))
          (send-msg-frames client "mmop/test" `(,id "1" "2" "3"))
          (send-msg-frames client "mmop/test" `(,id "test"))
          (pass "messages sent"))))))

(deftest MMOP/0
  (testing "worker-ready"
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (ok (typep (mmop-m:pull-master-message client) 'mmop-m:worker-ready-v0)))))

  (testing "start-node"
    (let ((recipe (make-instance 'rmq-node-recipe :type :test :source "test-s" :dest "test-d")))
      (pzmq:with-context nil
        (pzmq:with-socket client :router
          (pzmq:bind client "tcp://*:55555")

          (adt:match mmop-m:received-mmop (mmop-m:pull-master-message client)
            ((mmop-m:worker-ready-v0 client-id)
             (send-msg client *mmop-v0* (mmop-m:start-node-v0 client-id recipe)))
            (_ (fail "unexpected message type")))
          (pass "message sent")))))

  (testing "node-start-success"
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (adt:match mmop-m:received-mmop (mmop-m:pull-master-message client)
          ((mmop-m:start-node-success-v0 _ node-type)
           (ok (string= node-type "TEST")))
          (_ (fail "unexpected message type"))))))

  (testing "node-start-failure"
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (adt:match mmop-m:received-mmop (mmop-m:pull-master-message client)
          ((mmop-m:start-node-failure-v0 _ node-type cat msg)
           (ok (string= node-type "TEST"))
           (ok (string= cat "test"))
           (ok (string= msg "test-msg")))
          (_ (fail "unexpected message type"))))))

  (testing "stop-worker"
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (adt:match mmop-m:received-mmop (mmop-m:pull-master-message client)
          ((mmop-m:worker-ready-v0 client-id)
           (send-msg client *mmop-v0* (mmop-m:shutdown-worker-v0 client-id)))
          (_ (fail "unexpected message type")))
        (pass "message sent")))))
