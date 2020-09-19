(defpackage monomyth/communication-tests-worker/rmq-worker
  (:use :cl :rove :cl-rabbit :monomyth/rmq-worker :monomyth/worker :monomyth/mmop
        :monomyth/rmq-node-recipe :monomyth/rmq-node :monomyth/node
        :monomyth/tests/utils))
(in-package :monomyth/communication-tests-worker/rmq-worker)

(v:output-here *terminal-io*)
(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))
(defparameter *rmq-user* (uiop:getenv "TEST_RMQ_DEFAULT_USER"))
(defparameter *rmq-pass* (uiop:getenv "TEST_RMQ_DEFAULT_PASS"))
(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))
(defparameter *test-process-time* 2)
(defparameter queue-1 (format nil "process-test-~a-1" (get-universal-time)))
(defparameter queue-2 (format nil "process-test-~a-2" (get-universal-time)))
(defparameter queue-3 (format nil "process-test-~a-3" (get-universal-time)))
(defparameter queue-4 (format nil "process-test-~a-4" (get-universal-time)))

(teardown
  (let ((conn (setup-connection :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
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
      (adt:match mmop-m:received-mmop (mmop-m:pull-master-message worker)
        ((mmop-m:worker-ready-v0 client-id)
         (send-msg worker *mmop-v0* (mmop-m:shutdown-worker-v0 client-id))
         (pass "message sent"))
        (_ (fail "unexpected message type"))))))

(deftest worker-can-start-node
  (let ((recipe (build-test-recipe *source-queue* *dest-queue*)))
    (pzmq:with-context nil
      (pzmq:with-socket worker :router
        (pzmq:bind worker "tcp://*:55555")
        (adt:match mmop-m:received-mmop (mmop-m:pull-master-message worker)
          ((mmop-m:worker-ready-v0 client-id)
           (send-msg worker *mmop-v0* (mmop-m:start-node-v0 client-id recipe))
           (adt:match mmop-m:received-mmop (mmop-m:pull-master-message worker)
             ((mmop-m:start-node-success-v0 _ node-type)
              (ok (string= node-type "TEST"))
              (send-msg worker *mmop-v0* (mmop-m:shutdown-worker-v0 client-id))
              (pass "message sent"))
             (_ (fail "unexpected message type"))))
          (_ (fail "unexpected message type")))))))

(defclass test-bad-recipe (rmq-node-recipe) ())

(defun build-bad-test-recipe ()
  (make-instance 'test-bad-recipe :source "doesnt-matter" :dest "at-all" :type :test))

(deftest worker-catches-bad-recipe
  (let ((recipe (build-bad-test-recipe)))
    (pzmq:with-context nil
      (pzmq:with-socket worker :router
        (pzmq:bind worker "tcp://*:55555")
        (adt:match mmop-m:received-mmop (mmop-m:pull-master-message worker)
          ((mmop-m:worker-ready-v0 client-id)
           (send-msg worker *mmop-v0* (mmop-m:start-node-v0 client-id recipe))
           (adt:match mmop-m:received-mmop (mmop-m:pull-master-message worker)
             ((mmop-m:start-node-failure-v0 _ node-type cat msg)
              (ok (string= node-type "TEST"))
              (ok (string= cat "recipe build"))
              (ok (string= msg "worker cannot handle recipe type"))
              (send-msg worker *mmop-v0* (mmop-m:shutdown-worker-v0 client-id))
              (pass "message sent"))
             (_ (fail "unexpected message type"))))
          (_ (fail "unexpected message type"))))))

  (skip "worker catches bad recipe type"))

(deftest worker-processes-data
  (testing "single node"
    (let ((work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             *source-queue* *dest-queue* *dest-queue* 10 *rmq-host*
                             *rmq-user* *rmq-pass*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed"))
          (recipe1 (build-test-recipe *source-queue* *dest-queue*)))

      (startup work-node nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node *source-queue* item))
      (shutdown work-node)

      (pzmq:with-context nil
        (pzmq:with-socket worker :router
          (pzmq:bind worker "tcp://*:55555")
          (adt:match mmop-m:received-mmop (mmop-m:pull-master-message worker)
            ((mmop-m:worker-ready-v0 client-id)
             (send-msg worker *mmop-v0* (mmop-m:start-node-v0 client-id recipe1))
             (adt:match mmop-m:received-mmop (mmop-m:pull-master-message worker)
               ((mmop-m:start-node-success-v0 _ node-type)
                (ok (string= node-type "TEST")))
               (_ (fail "unexpected message type")))
             (sleep *test-process-time*)
             (send-msg worker *mmop-v0* (mmop-m:shutdown-worker-v0 client-id))
             (pass "message sent"))
            (_ (fail "unexpected message type")))))

      (setf work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             *dest-queue* *dest-queue* *dest-queue* 10 *rmq-host*
                             *rmq-user* *rmq-pass*))
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
    (let ((work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             queue-1 queue-2 queue-3 10 *rmq-host*
                             *rmq-user* *rmq-pass*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed"))
          (recipe1 (build-test-recipe1 queue-1 queue-2 5))
          (recipe2 (build-test-recipe2 queue-2 queue-3 10))
          (recipe3 (build-test-recipe3 queue-3 queue-4 4)))

      (startup work-node nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node queue-1 item))
      (shutdown work-node)

      (pzmq:with-context nil
        (pzmq:with-socket worker :router
          (pzmq:bind worker "tcp://*:55555")
          (adt:match mmop-m:received-mmop (mmop-m:pull-master-message worker)
            ((mmop-m:worker-ready-v0 client-id)

             (send-msg worker *mmop-v0* (mmop-m:start-node-v0 client-id recipe1))
             (adt:match mmop-m:received-mmop (mmop-m:pull-master-message worker)
               ((mmop-m:start-node-success-v0 _ node-type)
                (ok (string= node-type "TEST1")))
               (_ (fail "unexpected message type")))

             (send-msg worker *mmop-v0* (mmop-m:start-node-v0 client-id recipe2))
             (adt:match mmop-m:received-mmop (mmop-m:pull-master-message worker)
               ((mmop-m:start-node-success-v0 _ node-type)
                (ok (string= node-type "TEST2")))
               (_ (fail "unexpected message type")))

             (send-msg worker *mmop-v0* (mmop-m:start-node-v0 client-id recipe3))
             (adt:match mmop-m:received-mmop (mmop-m:pull-master-message worker)
               ((mmop-m:start-node-success-v0 _ node-type)
                (ok (string= node-type "TEST3")))
               (_ (fail "unexpected message type")))

             (sleep *test-process-time*)
             (send-msg worker *mmop-v0* (mmop-m:shutdown-worker-v0 client-id))
             (pass "message sent"))

            (_ (fail "unexpected message type")))))

      (setf work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             queue-4 queue-4 queue-4 10 *rmq-host*
                             *rmq-user* *rmq-pass*))
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
