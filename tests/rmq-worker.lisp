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
(v:output-here *terminal-io*)

(teardown
 (let ((conn (setup-connection :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
   (with-channel (conn 1)
     (queue-delete conn 1 *source-queue*)
     (queue-delete conn 1 *dest-queue*)
     (queue-delete conn 1 "TEST-NODE-fail")
     (queue-delete conn 1 "TEST-NODE1-fail")
     (queue-delete conn 1 "TEST-NODE2-fail")
     (queue-delete conn 1 "TEST-NODE3-fail")
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
           (adt:match mmop-m:received-mmop (mmop-m:pull-master-message master)
             ((mmop-m:worker-ready-v0 client-id)
              (send-msg master *mmop-v0*
                        (mmop-m:shutdown-worker-v0 client-id)))
             (_ (fail "unexpected message type")))))))
  (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                :password *rmq-pass*)))
    (start-worker wrkr "tcp://localhost:55555")
    (run-worker wrkr)
    (stop-worker wrkr)
    (pass "worker stopped")))

(deftest worker-can-start-node
  (let ((recipe (build-test-node-recipe)))
    (bt:make-thread
     #'(lambda ()
         (pzmq:with-context nil
           (pzmq:with-socket master :router
             (pzmq:bind master "tcp://*:55555")
             (adt:match mmop-m:received-mmop (mmop-m:pull-master-message master)
               ((mmop-m:worker-ready-v0 client-id)
                (send-msg master *mmop-v0*
                          (mmop-m:start-node-v0 client-id recipe))
                (adt:match mmop-m:received-mmop
                  (mmop-m:pull-master-message master)
                  ((mmop-m:start-node-success-v0 new-id msg)
                   (ok (string= new-id client-id))
                   (ok (string= msg "TEST-NODE")))
                  (_ (fail "unexpected message type")))
                (send-msg master *mmop-v0* (mmop-m:shutdown-worker-v0 client-id)))
               (_ (fail "unexpected message type")))))))
    (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                  :password *rmq-pass*)))
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
             (adt:match mmop-m:received-mmop (mmop-m:pull-master-message master)
               ((mmop-m:worker-ready-v0 client-id)
                (send-msg master *mmop-v0*
                          (mmop-m:start-node-v0 client-id recipe))
                (adt:match mmop-m:received-mmop
                  (mmop-m:pull-master-message master)
                  ((mmop-m:start-node-failure-v0 new-id node-type cat msg)
                   (ok (string= new-id client-id))
                   (ok (string= node-type "TEST"))
                   (ok (string= cat "recipe build"))
                   (ok (string= msg "worker cannot handle recipe type")))
                  (_ (fail "unexpected message type")))
                (send-msg master *mmop-v0* (mmop-m:shutdown-worker-v0 client-id)))
               (_ (fail "unexpected message type")))))))
    (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
      (start-worker wrkr "tcp://localhost:55555")
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped"))))

(deftest worker-processes-data
  (testing "single node"
    (let ((recipe1 (build-test-node-recipe))
          (work-node
            (build-test-node
             (format nil "worknode-~d" (get-universal-time))
             *dest-queue* *source-queue* *source-queue* :test 10 *rmq-host*
             *rmq-port* *rmq-user* *rmq-pass*))
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
               (adt:match mmop-m:received-mmop (mmop-m:pull-master-message master)
                 ((mmop-m:worker-ready-v0 client-id)
                  (send-msg master *mmop-v0*
                            (mmop-m:start-node-v0 client-id recipe1))
                  (adt:match mmop-m:received-mmop
                    (mmop-m:pull-master-message master)
                    ((mmop-m:start-node-success-v0 new-id msg)
                     (ok (string= new-id client-id))
                     (ok (string= msg "TEST-NODE")))
                    (_ (fail "unexpected message type")))
                  (sleep *test-process-time*)
                  (send-msg master *mmop-v0* (mmop-m:shutdown-worker-v0 client-id)))
                 (_ (fail "unexpected message type")))))))
      (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
        (start-worker wrkr "tcp://localhost:55555")
        (run-worker wrkr)
        (stop-worker wrkr)
        (pass "worker stopped"))

      (setf work-node
            (build-test-node
             (format nil "worknode-~d" (get-universal-time))
             *dest-queue* *source-queue* *source-queue* :test 10 *rmq-host*
             *rmq-port* *rmq-user* *rmq-pass*))
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
    (let ((recipe1 (build-test-node1-recipe))
          (recipe2 (build-test-node2-recipe))
          (recipe3 (build-test-node3-recipe))
          (work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             queue-1 queue-2 queue-3 :work 10 *rmq-host*
                             *rmq-port* *rmq-user* *rmq-pass*))
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
               (adt:match mmop-m:received-mmop (mmop-m:pull-master-message master)
                 ((mmop-m:worker-ready-v0 client-id)

                  (send-msg master *mmop-v0*
                            (mmop-m:start-node-v0 client-id recipe1))
                  (adt:match mmop-m:received-mmop
                    (mmop-m:pull-master-message master)
                    ((mmop-m:start-node-success-v0 new-id msg)
                     (ok (string= new-id client-id))
                     (ok (string= msg "TEST-NODE1")))
                    (_ (fail "unexpected message type")))

                  (send-msg master *mmop-v0*
                            (mmop-m:start-node-v0 client-id recipe2))
                  (adt:match mmop-m:received-mmop
                    (mmop-m:pull-master-message master)
                    ((mmop-m:start-node-success-v0 new-id msg)
                     (ok (string= new-id client-id))
                     (ok (string= msg "TEST-NODE2")))
                    (_ (fail "unexpected message type")))

                  (send-msg master *mmop-v0*
                            (mmop-m:start-node-v0 client-id recipe3))
                  (adt:match mmop-m:received-mmop
                    (mmop-m:pull-master-message master)
                    ((mmop-m:start-node-success-v0 new-id msg)
                     (ok (string= new-id client-id))
                     (ok (string= msg "TEST-NODE3")))
                    (_ (fail "unexpected message type")))

                  (sleep *test-process-time*)
                  (send-msg master *mmop-v0* (mmop-m:shutdown-worker-v0 client-id)))
                 (_ (fail "unexpected message type")))))))
      (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
        (start-worker wrkr "tcp://localhost:55555")
        (run-worker wrkr)
        (stop-worker wrkr)
        (pass "worker stopped"))

      (setf work-node (build-test-node (format nil "worknode-~d" (get-universal-time))
                                       queue-4 queue-4 queue-4 :work 10 *rmq-host*
                                       *rmq-port* *rmq-user* *rmq-pass*))
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
