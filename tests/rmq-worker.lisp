(defpackage monomyth/tests/rmq-worker
  (:use :cl :rove :cl-rabbit :monomyth/rmq-worker :monomyth/worker :monomyth/mmop
        :monomyth/rmq-node-recipe :monomyth/rmq-node :monomyth/node :stmx
        :monomyth/node-recipe :monomyth/tests/utils :monomyth/dsl
        :lparallel)
  (:shadow :closer-mop))
(in-package :monomyth/tests/rmq-worker)

(v:output-here *terminal-io*)
(defvar *test-context*)

(setup
  (setf *test-context* (pzmq:ctx-new)))

(teardown
  (pzmq:ctx-destroy *test-context*)
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
  (make-instance 'test-bad-recipe :type :test))

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

(define-rmq-node finished-node
    #'(lambda (node val) (declare (ignore val)) (complete-task node))
  10)

(deftest worker-handles-complete-task
  (bt:make-thread
   #'(lambda ()
       (pzmq:with-context nil
         (pzmq:with-socket master :router
           (pzmq:bind master "tcp://*:55555")
           (adt:match mmop-m:received-mmop (mmop-m:pull-master-message master)
             ((mmop-m:worker-ready-v0 client-id)
              (send-msg
               master *mmop-v0*
               (mmop-m:start-node-v0 client-id (build-finished-node-recipe)))
              (adt:match mmop-m:received-mmop (mmop-m:pull-master-message master)
                ((mmop-m:start-node-success-v0 _ ntype)
                 (pass "node started")

                 (adt:match mmop-m:received-mmop
                   (mmop-m:pull-master-message master)
                   ((mmop-m:worker-task-completed-v0 id node-type)
                    (ok (string= client-id id))
                    (ok (string= ntype node-type)))

                   (_ (fail "unexpected message type"))))

                (_ (fail "unexpected message type")))
              (send-msg master *mmop-v0* (mmop-m:shutdown-worker-v0 client-id)))

             (_ (fail "unexpected message type")))))))

  (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                :password *rmq-pass*)))
    (start-worker wrkr "tcp://localhost:55555")
    (run-worker wrkr)
    (stop-worker wrkr)
    (ok (zerop (hash-table-count (worker/nodes wrkr))))
    (pass "worker stopped")))

(define-rmq-node final-work-node nil 10 :source-queue queue-4 :dest-queue queue-4)

(defun fn1 (node item)
  (declare (ignore node))
  (format nil "test1 ~a" item))

(defun fn2 (node item)
  (declare (ignore node))
  (format nil "test2 ~a" item))

(defun fn3 (node item)
  (declare (ignore node))
  (format nil "test3 ~a" item))

(define-system ()
    (:name test-node1 :fn #'fn1 :batch-size 5)
    (:name test-node2 :fn #'fn2 :batch-size 10)
    (:name test-node3 :fn #'fn3 :batch-size 4))

(deftest worker-processes-data
  (testing "single node"
    (let ((recipe1 (build-test-node-recipe))
          (work-node
            (build-work-node
             (format nil "worknode-~d" (get-universal-time))
             *source-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed"))
          (i 0)
          (p (promise)))
      (define-rmq-node checking-node
          #'(lambda (node item)
              (declare (ignore node))
              (ok (string= (format nil "test ~a" (nth i items)) item))
              (incf i)
              (when (= i (length items))
                (fulfill p t)))
        1 :source-queue *dest-queue*)

      (let ((check-node (build-checking-node
                         "checker" *source-queue* :checker
                         *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))

        (startup work-node *test-context* "inproc://test" nil)
        (startup check-node *test-context* "inproc://test")
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
                    (force p)
                    (send-msg master *mmop-v0* (mmop-m:shutdown-worker-v0 client-id)))
                   (_ (fail "unexpected message type")))))))

        (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
          (start-worker wrkr "tcp://localhost:55555")
          (run-worker wrkr)
          (sleep .5)
          (stop-worker wrkr)
          (pass "worker stopped"))

        (setf work-node
              (build-work-node
               (format nil "worknode-~d" (get-universal-time))
               *source-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (startup work-node *test-context* "inproc://test" nil)
        (ng (pull-items work-node))

        (shutdown work-node)
        (shutdown check-node))))

  (testing "multiple nodes"
    (let ((recipe1 (build-test-node1-recipe))
          (recipe2 (build-test-node2-recipe))
          (recipe3 (build-test-node3-recipe))
          (work-node
            (build-final-work-node
             (format nil "worknode-~d" (get-universal-time))
             queue-1 :work *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed"))
          (i 0)
          (p (promise)))
      (define-rmq-node checking-node
          #'(lambda (node item)
              (declare (ignore node))
              (ok (string= (format nil "test3 test2 test1 ~a" (nth i items)) item))
              (incf i)
              (when (= i (length items))
                (fulfill p t)))
        1 :source-queue queue-4)

      (let ((check-node (build-checking-node
                         "checker" *source-queue* :checker
                         *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
        (startup work-node *test-context* "inproc://test" nil)
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

                    (force p)
                    (send-msg master *mmop-v0* (mmop-m:shutdown-worker-v0 client-id)))
                   (_ (fail "unexpected message type")))))))

        (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                      :password *rmq-pass*)))
          (startup check-node *test-context* "inproc://test")
          (start-worker wrkr "tcp://localhost:55555")
          (run-worker wrkr)
          (sleep .5)
          (stop-worker wrkr)
          (pass "worker stopped"))

        (setf work-node
              (build-final-work-node
               (format nil "worknode-~d" (get-universal-time))
               queue-4 :work *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (startup work-node *test-context* "inproc://test" nil)
        (ng (pull-items work-node))

        (shutdown check-node)
        (shutdown work-node)))))
