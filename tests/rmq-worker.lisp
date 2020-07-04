(defpackage monomyth/tests/rmq-worker
  (:use :cl :prove :cl-rabbit :monomyth/rmq-worker :monomyth/worker :monomyth/mmop
        :monomyth/rmq-node-recipe :monomyth/rmq-node :monomyth/node))
(in-package :monomyth/tests/rmq-worker)

(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))

(plan nil)
(vom:config t :info)

(subtest "worker starts/shutdown"
  (bt:make-thread
   #'(lambda ()
       (pzmq:with-context nil
         (pzmq:with-socket master :router
           (pzmq:bind master "tcp://*:55555")
           (let ((msg (mmop-m:pull-master-message master)))
             (is-type msg 'mmop-m:worker-ready-v0)
             (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0
                                         (mmop-m:worker-ready-v0-client-id msg))))))))
  (let ((wrkr (build-rmq-worker :host *rmq-host*)))
    (start-worker wrkr "tcp://localhost:55555")
    (run-worker wrkr)
    (stop-worker wrkr)
    (pass "worker stopped")))

(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))

(subtest "worker can start node"
  (let ((recipe (build-rmq-node-recipe :test "#'(lambda (x) (format nil \"test ~a\" x))"
                                       *source-queue* *dest-queue*)))
    (bt:make-thread
     #'(lambda ()
         (pzmq:with-context nil
           (pzmq:with-socket master :router
             (pzmq:bind master "tcp://*:55555")
             (let* ((msg (mmop-m:pull-master-message master))
                    (id (mmop-m:worker-ready-v0-client-id msg)))
               (is-type msg 'mmop-m:worker-ready-v0)
               (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe))
               (let ((res-msg (mmop-m:pull-master-message master)))
                 (is-type res-msg 'mmop-m:start-node-success-v0)
                 (is (mmop-m:start-node-success-v0-type res-msg) "TEST"))
               (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0 id)))))))
    (let ((wrkr (build-rmq-worker :host *rmq-host*)))
      (start-worker wrkr "tcp://localhost:55555")
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped"))))

(subtest "worker catches bad fn"
  (let ((recipe (build-rmq-node-recipe :test "#'(lambda (x) (format nil \"test ~a\" x)"
                                       *source-queue* *dest-queue*)))
    (bt:make-thread
     #'(lambda ()
         (pzmq:with-context nil
           (pzmq:with-socket master :router
             (pzmq:bind master "tcp://*:55555")
             (let* ((msg (mmop-m:pull-master-message master))
                    (id (mmop-m:worker-ready-v0-client-id msg)))
               (is-type msg 'mmop-m:worker-ready-v0)
               (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe))
               (let ((res-msg (mmop-m:pull-master-message master)))
                 (is-type res-msg 'mmop-m:start-node-failure-v0)
                 (is (mmop-m:start-node-failure-v0-type res-msg) "TEST")
                 (is (mmop-m:start-node-failure-v0-reason-cat res-msg)
                     "function read")
                 (is (mmop-m:start-node-failure-v0-reason-msg res-msg)
                     "end of file (mismatched forms)"))
               (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0 id)))))))
    (let ((wrkr (build-rmq-worker :host *rmq-host*)))
      (start-worker wrkr "tcp://localhost:55555")
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped"))))

(skip 1 "worker catches bad recipe type")

(subtest "worker processes data - single node"
  (let* ((recipe1 (build-rmq-node-recipe :test "#'(lambda (x) (format nil \"test ~a\" x))"
                                         *source-queue* *dest-queue* 5))
         (conn (let ((conn (setup-connection :host (uiop:getenv "TEST_RMQ"))))
                 (if (getf conn :success)
                     (getf conn :conn)
                     (error (getf conn :error)))))
         (work-node (make-rmq-node nil (format nil "worknode-~d" (get-universal-time))
                                   conn 10 *source-queue* *dest-queue* *dest-queue*))
         (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed")))
    (startup work-node nil)
    (iter:iterate
      (iter:for item in items)
      (send-message work-node *source-queue* item))
    (shutdown work-node)
    (destroy-connection conn)
    (bt:make-thread
     #'(lambda ()
         (pzmq:with-context nil
           (pzmq:with-socket master :router
             (pzmq:bind master "tcp://*:55555")
             (let* ((msg (mmop-m:pull-master-message master))
                    (id (mmop-m:worker-ready-v0-client-id msg)))
               (is-type msg 'mmop-m:worker-ready-v0)
               (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe1))
               (let ((res-msg (mmop-m:pull-master-message master)))
                 (is-type res-msg 'mmop-m:start-node-success-v0)
                 (is (mmop-m:start-node-success-v0-type res-msg) "TEST"))
               (sleep 1)
               (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0 id)))))))
    (let ((wrkr (build-rmq-worker :host *rmq-host*)))
      (start-worker wrkr "tcp://localhost:55555")
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped"))

    (setf conn (let ((conn (setup-connection :host (uiop:getenv "TEST_RMQ"))))
                 (if (getf conn :success)
                     (getf conn :conn)
                     (error (getf conn :error))))
          work-node (make-rmq-node nil (format nil "worknode-~d" (get-universal-time))
                                   conn 10 *dest-queue* *dest-queue* *dest-queue*))
    (startup work-node nil)
    (iter:iterate
      (iter:for item in items)
      (iter:for got = (get-message work-node))
      (iter:for inner = (getf got :result))
      (ok (getf got :success))
      (ok (not (getf got :timeout)))
      (is (rmq-message-body inner) (format nil "test ~a" item))
      (ok (getf (ack-message work-node inner) :success)))
    (shutdown work-node)
    (destroy-connection conn)))

(subtest "worker processes data - multiple nodes"
  (let* ((queue-1 (format nil "process-test-~a-1" (get-universal-time)))
         (queue-2 (format nil "process-test-~a-2" (get-universal-time)))
         (queue-3 (format nil "process-test-~a-3" (get-universal-time)))
         (queue-4 (format nil "process-test-~a-4" (get-universal-time)))
         (recipe1 (build-rmq-node-recipe :test1 "#'(lambda (x) (format nil \"test1 ~a\" x))"
                                         queue-1 queue-2 5))
         (recipe2 (build-rmq-node-recipe :test2 "#'(lambda (x) (format nil \"test2 ~a\" x))"
                                         queue-2 queue-3))
         (recipe3 (build-rmq-node-recipe :test3 "#'(lambda (x) (format nil \"test3 ~a\" x))"
                                         queue-3 queue-4 4))
         (conn (let ((conn (setup-connection :host (uiop:getenv "TEST_RMQ"))))
                 (if (getf conn :success)
                     (getf conn :conn)
                     (error (getf conn :error)))))
         (work-node (make-rmq-node nil (format nil "worknode-~d" (get-universal-time))
                                   conn 10 queue-1 queue-2 queue-3))
         (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed")))
    (startup work-node nil)
    (iter:iterate
      (iter:for item in items)
      (send-message work-node queue-1 item))
    (shutdown work-node)
    (destroy-connection conn)
    (bt:make-thread
     #'(lambda ()
         (pzmq:with-context nil
           (pzmq:with-socket master :router
             (pzmq:bind master "tcp://*:55555")
             (let* ((msg (mmop-m:pull-master-message master))
                    (id (mmop-m:worker-ready-v0-client-id msg)))
               (is-type msg 'mmop-m:worker-ready-v0)
               (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe1))
               (let ((res-msg1 (mmop-m:pull-master-message master)))
                 (is-type res-msg1 'mmop-m:start-node-success-v0)
                 (is (mmop-m:start-node-success-v0-type res-msg1) "TEST1"))
               (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe2))
               (let ((res-msg2 (mmop-m:pull-master-message master)))
                 (is-type res-msg2 'mmop-m:start-node-success-v0)
                 (is (mmop-m:start-node-success-v0-type res-msg2) "TEST2"))
               (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe3))
               (let ((res-msg3 (mmop-m:pull-master-message master)))
                 (is-type res-msg3 'mmop-m:start-node-success-v0)
                 (is (mmop-m:start-node-success-v0-type res-msg3) "TEST3"))
               (sleep 1000)
               (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0 id)))))))
    (let ((wrkr (build-rmq-worker :host *rmq-host*)))
      (start-worker wrkr "tcp://localhost:55555")
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped"))

    (setf conn (let ((conn (setup-connection :host (uiop:getenv "TEST_RMQ"))))
                 (if (getf conn :success)
                     (getf conn :conn)
                     (error (getf conn :error))))
          work-node (make-rmq-node nil (format nil "worknode-~d" (get-universal-time))
                                   conn 10 queue-4 queue-4 queue-4))
    (startup work-node nil)
    (iter:iterate
      (iter:for item in items)
      (iter:for got = (get-message work-node))
      (iter:for inner = (getf got :result))
      (ok (getf got :success))
      (ok (not (getf got :timeout)))
      (is (rmq-message-body inner) (format nil "test ~a" item))
      (ok (getf (ack-message work-node inner) :success)))
    (delete-queue work-node queue-1)
    (delete-queue work-node queue-2)
    (delete-queue work-node queue-3)
    (delete-queue work-node queue-4)
    (shutdown work-node)
    (destroy-connection conn)))

(defparameter *conn* (let ((conn (setup-connection :host (uiop:getenv "TEST_RMQ"))))
                       (if (getf conn :success)
                           (getf conn :conn)
                           (error (getf conn :error)))))
(with-channel (*conn* 1)
  (queue-delete *conn* 1 *source-queue*)
  (queue-delete *conn* 1 *dest-queue*)
  (queue-delete *conn* 1 "TEST-fail"))
(destroy-connection *conn*)

(finalize)
