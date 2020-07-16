(defpackage monomyth/communication-tests-worker/rmq-worker
  (:use :cl :rove :cl-rabbit :monomyth/rmq-worker :monomyth/worker :monomyth/mmop
        :monomyth/rmq-node-recipe :monomyth/rmq-node :monomyth/node))
(in-package :monomyth/communication-tests-worker/rmq-worker)

(vom:config t :info)
(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))
(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))
(defparameter *test-process-time* 5)
(defparameter queue-1 (format nil "process-test-~a-1" (get-universal-time)))
(defparameter queue-2 (format nil "process-test-~a-2" (get-universal-time)))
(defparameter queue-3 (format nil "process-test-~a-3" (get-universal-time)))
(defparameter queue-4 (format nil "process-test-~a-4" (get-universal-time)))

(defun get-master-ip ()
  (let ((env-value (uiop:getenv "TEST_MASTER_IP")))
    (if env-value env-value
        (progn
          (format t "Please supply the master ip address:~%")
          (read-line)))))

(defparameter *master-uri* (format nil "tcp://~a:55555" (get-master-ip)))

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
  (let ((wrkr (build-rmq-worker :host *rmq-host*)))
    (start-worker wrkr *master-uri*)
    (run-worker wrkr)
    (stop-worker wrkr)
    (pass "worker stopped")))

(deftest worker-can-start-node
  (let ((wrkr (build-rmq-worker :host *rmq-host*)))
    (start-worker wrkr *master-uri*)
    (run-worker wrkr)
    (stop-worker wrkr)
    (pass "worker stopped")))

(deftest worker-catches-bad-fn
  (let ((wrkr (build-rmq-worker :host *rmq-host*)))
    (start-worker wrkr *master-uri*)
    (run-worker wrkr)
    (stop-worker wrkr)
    (pass "worker stopped"))
  (skip "worker catches bad recipe type"))

(deftest worker-processes-data
  (testing "single node"
    (let ((work-node (make-rmq-node nil (format nil "worknode-~d" (get-universal-time))
                                    *source-queue* *dest-queue* *dest-queue*
                                    :host *rmq-host*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed")))
      (startup work-node nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node *source-queue* item))
      (shutdown work-node)
      (let ((wrkr (build-rmq-worker :host *rmq-host*)))
        (start-worker wrkr *master-uri*)
        (run-worker wrkr)
        (stop-worker wrkr)
        (pass "worker stopped"))

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
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed")))
      (startup work-node nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node queue-1 item))
      (shutdown work-node)
      (let ((wrkr (build-rmq-worker :host *rmq-host*)))
        (start-worker wrkr *master-uri*)
        (run-worker wrkr)
        (stop-worker wrkr)
        (pass "worker stopped"))

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
