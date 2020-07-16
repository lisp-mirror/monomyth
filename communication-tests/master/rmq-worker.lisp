(defpackage monomyth/communication-tests-worker/rmq-worker
  (:use :cl :rove :cl-rabbit :monomyth/rmq-worker :monomyth/worker :monomyth/mmop
        :monomyth/rmq-node-recipe :monomyth/rmq-node :monomyth/node))
(in-package :monomyth/communication-tests-worker/rmq-worker)

(vom:config t :info)
(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))
(defparameter *test-process-time* 5)
(defparameter queue-1 (format nil "process-test-~a-1" (get-universal-time)))
(defparameter queue-2 (format nil "process-test-~a-2" (get-universal-time)))
(defparameter queue-3 (format nil "process-test-~a-3" (get-universal-time)))
(defparameter queue-4 (format nil "process-test-~a-4" (get-universal-time)))

(deftest worker-starts-shutdown
  (pzmq:with-context nil
    (pzmq:with-socket worker :router
      (pzmq:bind worker "tcp://*:55555")
      (let ((msg (mmop-m:pull-master-message worker)))
        (ok (typep msg 'mmop-m:worker-ready-v0))
        (sleep .1)
        (send-msg worker *mmop-v0* (mmop-m:make-shutdown-worker-v0
                                    (mmop-m:worker-ready-v0-client-id msg)))
        (pass "message sent")))))

(deftest worker-can-start-node
  (let ((recipe (build-rmq-node-recipe :test "#'(lambda (x) (format nil \"test ~a\" x))"
                                       *source-queue* *dest-queue*)))
    (pzmq:with-context nil
      (pzmq:with-socket worker :router
        (pzmq:bind worker "tcp://*:55555")
        (let* ((msg (mmop-m:pull-master-message worker))
               (id (mmop-m:worker-ready-v0-client-id msg)))
          (ok (typep msg 'mmop-m:worker-ready-v0))
          (send-msg worker *mmop-v0* (mmop-m:make-start-node-v0 id recipe))
          (let ((res-msg (mmop-m:pull-master-message worker)))
            (ok (typep res-msg 'mmop-m:start-node-success-v0))
            (ok (string= (mmop-m:start-node-success-v0-type res-msg) "TEST")))
          (send-msg worker *mmop-v0* (mmop-m:make-shutdown-worker-v0 id))
          (pass "message sent"))))))

(deftest worker-catches-bad-fn
  (let ((recipe (build-rmq-node-recipe :test "#'(lambda (x) (format nil \"test ~a\" x)"
                                       *source-queue* *dest-queue*)))
    (pzmq:with-context nil
      (pzmq:with-socket worker :router
        (pzmq:bind worker "tcp://*:55555")
        (let* ((msg (mmop-m:pull-master-message worker))
               (id (mmop-m:worker-ready-v0-client-id msg)))
          (ok (typep msg 'mmop-m:worker-ready-v0))
          (send-msg worker *mmop-v0* (mmop-m:make-start-node-v0 id recipe))
          (let ((res-msg (mmop-m:pull-master-message worker)))
            (ok (typep res-msg 'mmop-m:start-node-failure-v0))
            (ok (string= (mmop-m:start-node-failure-v0-type res-msg) "TEST"))
            (ok (string= (mmop-m:start-node-failure-v0-reason-cat res-msg)
                         "function read"))
            (ok (string= (mmop-m:start-node-failure-v0-reason-msg res-msg)
                         "end of file (mismatched forms)")))
          (send-msg worker *mmop-v0* (mmop-m:make-shutdown-worker-v0 id))
          (pass "message sent")))))

  (skip "worker catches bad recipe type"))

(deftest worker-processes-data
  (testing "single node"
    (let ((recipe1 (build-rmq-node-recipe :test "#'(lambda (x) (format nil \"test ~a\" x))"
                                          *source-queue* *dest-queue* 5)))
      (pzmq:with-context nil
        (pzmq:with-socket worker :router
          (pzmq:bind worker "tcp://*:55555")
          (let* ((msg (mmop-m:pull-master-message worker))
                 (id (mmop-m:worker-ready-v0-client-id msg)))
            (ok (typep msg 'mmop-m:worker-ready-v0))
            (send-msg worker *mmop-v0* (mmop-m:make-start-node-v0 id recipe1))
            (let ((res-msg (mmop-m:pull-master-message worker)))
              (ok (typep res-msg 'mmop-m:start-node-success-v0))
              (ok (string= (mmop-m:start-node-success-v0-type res-msg) "TEST")))
            (sleep *test-process-time*)
            (send-msg worker *mmop-v0* (mmop-m:make-shutdown-worker-v0 id))
            (pass "message sent"))))))

  (testing "worker processes data - multiple nodes"
    (let ((recipe1 (build-rmq-node-recipe :test1 "#'(lambda (x) (format nil \"test1 ~a\" x))"
                                          queue-1 queue-2 5))
          (recipe2 (build-rmq-node-recipe :test2 "#'(lambda (x) (format nil \"test2 ~a\" x))"
                                          queue-2 queue-3))
          (recipe3 (build-rmq-node-recipe :test3 "#'(lambda (x) (format nil \"test3 ~a\" x))"
                                          queue-3 queue-4 4)))
      (pzmq:with-context nil
        (pzmq:with-socket worker :router
          (pzmq:bind worker "tcp://*:55555")
          (let* ((msg (mmop-m:pull-master-message worker))
                 (id (mmop-m:worker-ready-v0-client-id msg)))
            (ok (typep msg 'mmop-m:worker-ready-v0))
            (send-msg worker *mmop-v0* (mmop-m:make-start-node-v0 id recipe1))
            (let ((res-msg1 (mmop-m:pull-master-message worker)))
              (ok (typep res-msg1 'mmop-m:start-node-success-v0))
              (ok (string= (mmop-m:start-node-success-v0-type res-msg1) "TEST1")))
            (send-msg worker *mmop-v0* (mmop-m:make-start-node-v0 id recipe2))
            (let ((res-msg2 (mmop-m:pull-master-message worker)))
              (ok (typep res-msg2 'mmop-m:start-node-success-v0))
              (ok (string= (mmop-m:start-node-success-v0-type res-msg2) "TEST2")))
            (send-msg worker *mmop-v0* (mmop-m:make-start-node-v0 id recipe3))
            (let ((res-msg3 (mmop-m:pull-master-message worker)))
              (ok (typep res-msg3 'mmop-m:start-node-success-v0))
              (ok (string= (mmop-m:start-node-success-v0-type res-msg3) "TEST3")))
            (sleep *test-process-time*)
            (send-msg worker *mmop-v0* (mmop-m:make-shutdown-worker-v0 id))
            (pass "message sent")))))))
