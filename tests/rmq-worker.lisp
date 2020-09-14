(defpackage monomyth/tests/rmq-worker
  (:use :cl :rove :cl-rabbit :monomyth/rmq-worker :monomyth/worker :monomyth/mmop
        :monomyth/rmq-node-recipe :monomyth/rmq-node :monomyth/node :stmx
        :monomyth/node-recipe :monomyth/tests/utils)
  (:shadow :closer-mop))
(in-package :monomyth/tests/rmq-worker)

(defparameter *test-process-time* 3)
(defparameter queue-1 (format nil "process-test-~a-1" (get-universal-time)))
(defparameter queue-2 (format nil "process-test-~a-2" (get-universal-time)))
(defparameter queue-3 (format nil "process-test-~a-3" (get-universal-time)))
(defparameter queue-4 (format nil "process-test-~a-4" (get-universal-time)))
(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))
(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))
(defparameter *rmq-user* (uiop:getenv "TEST_RMQ_DEFAULT_USER"))
(defparameter *rmq-pass* (uiop:getenv "TEST_RMQ_DEFAULT_PASS"))
(v:output-here *terminal-io*)

(teardown
 (let ((conn (setup-connection :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
   (with-channel (conn 1)
     (queue-delete conn 1 *source-queue*)
     (queue-delete conn 1 *dest-queue*)
     (queue-delete conn 1 "TEST-fail")
     (queue-delete conn 1 "TEST1-fail")
     (queue-delete conn 1 "TEST2-fail")
     (queue-delete conn 1 "TEST3-fail")
     (queue-delete conn 1 queue-1)
     (queue-delete conn 1 queue-2)
     (queue-delete conn 1 queue-3)
     (queue-delete conn 1 queue-4))
   (destroy-connection conn)))

(deftest worker-starts-shutdown
  (bt:make-thread
   #'(lambda ()
       (pzmq:with-context nil
         (pzmq:with-socket master :router
           (pzmq:bind master "tcp://*:55555")
           (let ((msg (mmop-m:pull-master-message master)))
             (ok (eq (type-of msg) 'mmop-m:worker-ready-v0))
             (sleep .1)
             (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0
                                         (mmop-m:worker-ready-v0-client-id msg))))))))
  (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
    (start-worker wrkr "tcp://localhost:55555")
    (run-worker wrkr)
    (stop-worker wrkr)
    (pass "worker stopped")))

(deftest worker-can-start-node
  (let ((recipe (build-test-recipe *source-queue* *dest-queue*)))
    (bt:make-thread
     #'(lambda ()
         (pzmq:with-context nil
           (pzmq:with-socket master :router
             (pzmq:bind master "tcp://*:55555")
             (let* ((msg (mmop-m:pull-master-message master))
                    (id (mmop-m:worker-ready-v0-client-id msg)))
               (ok (eq (type-of msg) 'mmop-m:worker-ready-v0))
               (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe))
               (let ((res-msg (mmop-m:pull-master-message master)))
                 (ok (eq (type-of res-msg) 'mmop-m:start-node-success-v0))
                 (ok (string= (mmop-m:start-node-success-v0-type res-msg) "TEST")))
               (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0 id)))))))
    (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
      (start-worker wrkr "tcp://localhost:55555")
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped"))))

(defclass test-bad-recipe (rmq-node-recipe) ())

(defun build-bad-test-recipe ()
  (make-instance 'test-bad-recipe :source "doesnt-matter" :dest "at-all" :type :test))

(deftest worker-catches-bad-recipe
  (let ((recipe (build-bad-test-recipe)))
    (bt:make-thread
     #'(lambda ()
         (pzmq:with-context nil
           (pzmq:with-socket master :router
             (pzmq:bind master "tcp://*:55555")
             (let* ((msg (mmop-m:pull-master-message master))
                    (id (mmop-m:worker-ready-v0-client-id msg)))
               (ok (eq (type-of msg) 'mmop-m:worker-ready-v0))
               (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe))
               (let ((res-msg (mmop-m:pull-master-message master)))
                 (ok (eq (type-of res-msg) 'mmop-m:start-node-failure-v0))
                 (ok (string= (mmop-m:start-node-failure-v0-type res-msg) "TEST"))
                 (ok (string= (mmop-m:start-node-failure-v0-reason-cat res-msg)
                              "recipe build"))
                 (ok (string= (mmop-m:start-node-failure-v0-reason-msg res-msg)
                              "worker cannot handle recipe type")))
               (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0 id)))))))
    (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
      (start-worker wrkr "tcp://localhost:55555")
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped"))))

(deftest worker-processes-data
  (testing "single node"
    (let ((recipe1 (build-test-recipe *source-queue* *dest-queue*))
          (work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             *source-queue* *dest-queue* *dest-queue* 10 *rmq-host*
                             *rmq-user* *rmq-pass*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed")))
      (startup work-node nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node *source-queue* item))
      (shutdown work-node)
      (bt:make-thread
       #'(lambda ()
           (pzmq:with-context nil
             (pzmq:with-socket master :router
               (pzmq:bind master "tcp://*:55555")
               (let* ((msg (mmop-m:pull-master-message master))
                      (id (mmop-m:worker-ready-v0-client-id msg)))
                 (ok (eq (type-of msg) 'mmop-m:worker-ready-v0))
                 (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe1))
                 (let ((res-msg (mmop-m:pull-master-message master)))
                   (ok (eq (type-of res-msg) 'mmop-m:start-node-success-v0))
                   (ok (string= (mmop-m:start-node-success-v0-type res-msg) "TEST")))
                 (sleep *test-process-time*)
                 (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0 id)))))))
      (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
        (start-worker wrkr "tcp://localhost:55555")
        (run-worker wrkr)
        (stop-worker wrkr)
        (pass "worker stopped"))

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
          (ack-message work-node got))
        (shutdown work-node))))

  (testing "multiple nodes"
    (let ((recipe1 (build-test-recipe1 queue-1 queue-2 5))
          (recipe2 (build-test-recipe2 queue-2 queue-3 10))
          (recipe3 (build-test-recipe3 queue-3 queue-4 4))
          (work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             queue-1 queue-2 queue-3 10 *rmq-host*
                             *rmq-user* *rmq-pass*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed")))
      (startup work-node nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node queue-1 item))
      (shutdown work-node)
      (bt:make-thread
       #'(lambda ()
           (pzmq:with-context nil
             (pzmq:with-socket master :router
               (pzmq:bind master "tcp://*:55555")
               (let* ((msg (mmop-m:pull-master-message master))
                      (id (mmop-m:worker-ready-v0-client-id msg)))
                 (ok (eq (type-of msg) 'mmop-m:worker-ready-v0))
                 (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe1))
                 (let ((res-msg1 (mmop-m:pull-master-message master)))
                   (ok (eq (type-of res-msg1) 'mmop-m:start-node-success-v0))
                   (ok (string= (mmop-m:start-node-success-v0-type res-msg1) "TEST1")))
                 (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe2))
                 (let ((res-msg2 (mmop-m:pull-master-message master)))
                   (ok (eq (type-of res-msg2) 'mmop-m:start-node-success-v0))
                   (ok (string= (mmop-m:start-node-success-v0-type res-msg2) "TEST2")))
                 (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe3))
                 (let ((res-msg3 (mmop-m:pull-master-message master)))
                   (ok (eq (type-of res-msg3) 'mmop-m:start-node-success-v0))
                   (ok (string= (mmop-m:start-node-success-v0-type res-msg3) "TEST3")))
                 (sleep *test-process-time*)
                 (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0 id)))))))
      (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
        (start-worker wrkr "tcp://localhost:55555")
        (run-worker wrkr)
        (stop-worker wrkr)
        (pass "worker stopped"))

      (setf work-node (build-test-node (format nil "worknode-~d" (get-universal-time))
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
