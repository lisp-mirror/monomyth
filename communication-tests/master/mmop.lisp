(defpackage monomyth/communication-tests-master/mmop
  (:use :cl :prove :monomyth/mmop :monomyth/rmq-node-recipe :monomyth/node-recipe
        :rutils.bind))
(in-package :monomyth/communication-tests-master/mmop)

(plan nil)

(diag "Ready to start tests?")
(read-line)

(subtest "msg-happy-path-to-router"
  (pzmq:with-context nil
    (pzmq:with-socket client :router
      (pzmq:bind client "tcp://*:55555")

      (is (cdr (pull-msg client)) '("1" "2" "3")))))

(subtest "msg-happy-path-to-router-with-second-message"
  (pzmq:with-context nil
    (pzmq:with-socket client :router
      (pzmq:bind client "tcp://*:55555")

      (is (cdr (pull-msg client)) '("1" "2" "3")))))

(subtest "msg-happy-path-to-dealer"
  (pzmq:with-context nil
    (pzmq:with-socket client :router
      (pzmq:bind client "tcp://*:55555")

      (with (((id &rest _) (pull-msg client)))
        (send-msg-frames client "mmop/test" `(,id "1" "2" "3"))))))

(subtest "msg-happy-path-to-dealer-with-second-msg"
  (pzmq:with-context nil
    (pzmq:with-socket client :router
      (pzmq:bind client "tcp://*:55555")

      (with (((id &rest _) (pull-msg client)))
        (send-msg-frames client "mmop/test" `(,id "1" "2" "3"))
        (send-msg-frames client "mmop/test" `(,id "test"))))))

(subtest "MMOP/0 worker-ready"
  (pzmq:with-context nil
    (pzmq:with-socket client :router
      (pzmq:bind client "tcp://*:55555")

      (is-type (mmop-m:pull-master-message client) 'mmop-m:worker-ready-v0))))

(subtest "MMOP/0 start-node"
  (let ((recipe (build-rmq-node-recipe :test "#'(lambda (x) (1+ x))" "test-s" "test-d")))
    (pzmq:with-context nil
      (pzmq:with-socket client :router
        (pzmq:bind client "tcp://*:55555")

        (send-msg client *mmop-v0*
                  (mmop-m:make-start-node-v0
                   (mmop-m:worker-ready-v0-client-id (mmop-m:pull-master-message client))
                   recipe))))))

(subtest "MMOP/0 node-start-success"
  (pzmq:with-context nil
    (pzmq:with-socket client :router
      (pzmq:bind client "tcp://*:55555")

      (let ((res (mmop-m:pull-master-message client)))
        (is-type res 'mmop-m:start-node-success-v0)
        (is (mmop-m:start-node-success-v0-type res) "TEST")))))

(subtest "MMOP/0 node-start-failure"
  (pzmq:with-context nil
    (pzmq:with-socket client :router
      (pzmq:bind client "tcp://*:55555")

      (let ((res (mmop-m:pull-master-message client)))
        (is-type res 'mmop-m:start-node-failure-v0)
        (is (mmop-m:start-node-failure-v0-type res) "TEST")
        (is (mmop-m:start-node-failure-v0-reason-cat res) "test")
        (is (mmop-m:start-node-failure-v0-reason-msg res) "test-msg")))))

(subtest "MMOP/0 stop-worker"
  (pzmq:with-context nil
    (pzmq:with-socket client :router
      (pzmq:bind client "tcp://*:55555")

      (send-msg client *mmop-v0*
                (mmop-m:make-shutdown-worker-v0
                 (mmop-m:worker-ready-v0-client-id
                  (mmop-m:pull-master-message client)))))))

(finalize)
