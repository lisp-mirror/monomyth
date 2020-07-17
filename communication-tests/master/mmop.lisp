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
    (let ((recipe (build-rmq-node-recipe :test "#'(lambda (x) (1+ x))" "test-s" "test-d")))
      (pzmq:with-context nil
        (pzmq:with-socket client :router
          (pzmq:bind client "tcp://*:55555")

          (send-msg client *mmop-v0*
                    (mmop-m:make-start-node-v0
                     (mmop-m:worker-ready-v0-client-id (mmop-m:pull-master-message client))
                     recipe))
          (pass "message sent")))))

  (testing "node-start-success"
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (let ((res (mmop-m:pull-master-message client)))
          (ok (typep res 'mmop-m:start-node-success-v0))
          (ok (string= (mmop-m:start-node-success-v0-type res) "TEST"))))))

  (testing "node-start-failure"
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (let ((res (mmop-m:pull-master-message client)))
          (ok (typep res 'mmop-m:start-node-failure-v0))
          (ok (string= (mmop-m:start-node-failure-v0-type res) "TEST"))
          (ok (string= (mmop-m:start-node-failure-v0-reason-cat res) "test"))
          (ok (string= (mmop-m:start-node-failure-v0-reason-msg res) "test-msg"))))))

  (testing "stop-worker"
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (send-msg client *mmop-v0*
                  (mmop-m:make-shutdown-worker-v0
                   (mmop-m:worker-ready-v0-client-id
                    (mmop-m:pull-master-message client))))
        (pass "message sent")))))
