(defpackage monomyth/communication-tests-worker/rmq-worker
  (:use :cl :rove :cl-rabbit :monomyth/rmq-worker :monomyth/worker :monomyth/mmop
        :monomyth/rmq-node-recipe :monomyth/rmq-node :monomyth/node))
(in-package :monomyth/communication-tests-worker/rmq-worker)

(vom:config t :info)
(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))
(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))
(defparameter *test-process-time* 2)
(defparameter queue-1 (format nil "process-test-~a-1" (get-universal-time)))
(defparameter queue-2 (format nil "process-test-~a-2" (get-universal-time)))
(defparameter queue-3 (format nil "process-test-~a-3" (get-universal-time)))
(defparameter queue-4 (format nil "process-test-~a-4" (get-universal-time)))

(teardown
  (let ((conn (setup-connection :host (uiop:getenv "TEST_RMQ"))))
    (with-channel (conn 1)
      (queue-delete conn 1 *source-queue*)
      (queue-delete conn 1 *dest-queue*)
      (queue-delete conn 1 "TEST-fail")
      (queue-delete conn 1 queue-1)
      (queue-delete conn 1 queue-2)
      (queue-delete conn 1 queue-3)
      (queue-delete conn 1 queue-4))
    (destroy-connection conn)))

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
    (let ((work-node (make-rmq-node nil (format nil "worknode-~d" (get-universal-time))
                                    *source-queue* *dest-queue* *dest-queue*
                                    :host *rmq-host*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed"))
          (recipe1 (build-rmq-node-recipe :test "#'(lambda (x) (format nil \"test ~a\" x))"
                                          *source-queue* *dest-queue* 5)))

      (startup work-node nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node *source-queue* item))
      (shutdown work-node)

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
            (pass "message sent"))))

      (setf work-node (make-rmq-node nil (format nil "worknode-~d" (get-universal-time))
                                     *dest-queue* *dest-queue* *dest-queue*
                                     :host *rmq-host*))
      (startup work-node nil)
      (labels ((get-msg-w-restart ()
                 (handler-case (get-message work-node)
                   (rabbitmq-error (c)
                     (declare (ignore c))
                     (sleep .1)
                     (get-msg-w-restart)))))
        (iter:iterate
          (iter:for item in items)
          (iter:for got = (get-msg-w-restart))
          (ok (string= (rmq-message-body got) (format nil "test ~a" item)))
          (ack-message work-node got)))
      (shutdown work-node)))

  (testing "worker processes data - multiple nodes"
    (let ((work-node (make-rmq-node nil (format nil "worknode-~d" (get-universal-time))
                                    queue-1 queue-2 queue-3 :host *rmq-host*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed"))
          (recipe1 (build-rmq-node-recipe :test1 "#'(lambda (x) (format nil \"test1 ~a\" x))"
                                          queue-1 queue-2 5))
          (recipe2 (build-rmq-node-recipe :test2 "#'(lambda (x) (format nil \"test2 ~a\" x))"
                                          queue-2 queue-3))
          (recipe3 (build-rmq-node-recipe :test3 "#'(lambda (x) (format nil \"test3 ~a\" x))"
                                          queue-3 queue-4 4)))

      (startup work-node nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node queue-1 item))
      (shutdown work-node)

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
            (pass "message sent"))))

      (setf work-node (make-rmq-node nil (format nil "worknode-~d" (get-universal-time))
                                     queue-4 queue-4 queue-4 :host *rmq-host*))
      (startup work-node nil)
      (labels ((get-msg-w-restart ()
                 (handler-case (get-message work-node)
                   (rabbitmq-error (c)
                     (declare (ignore c))
                     (sleep .1)
                     (get-msg-w-restart)))))
        (iter:iterate
          (iter:for item in items)
          (iter:for got = (get-msg-w-restart))
          (ok (string= (rmq-message-body got) (format nil "test3 test2 test1 ~a" item)))
          (ack-message work-node got))
        (shutdown work-node)))))
