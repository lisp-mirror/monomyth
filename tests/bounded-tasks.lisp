(defpackage monomyth/tests/bounded-tasks
  (:use :cl :rove :monomyth/tests/utils :cl-rabbit  :monomyth/rmq-node :monomyth/dsl
        :monomyth/node :monomyth/rmq-worker :monomyth/master :lparallel :monomyth/mmop
        :monomyth/worker :stmx.util)
  (:shadow :closer-mop))
(in-package :monomyth/tests/bounded-tasks)

(v:output-here *terminal-io*)
(defvar *test-context*)

(setup
  (setf *test-context* (pzmq:ctx-new)))

(teardown
  (pzmq:ctx-destroy *test-context*)
  (let ((conn (setup-connection :host *rmq-host* :username *rmq-user*
                                :password *rmq-pass*)))
    (with-channel (conn 1)
      (queue-delete conn 1 queue-2)
      (queue-delete conn 1 queue-3)
      (queue-delete conn 1 queue-4)
      (queue-delete conn 1 "TEST-NODE1-fail")
      (queue-delete conn 1 "TEST-NODE2-fail")
      (queue-delete conn 1 "TEST-NODE3-fail")
    (destroy-connection conn))))

(defvar *iter-count* 0)
(defparameter *num-items* 7)
(defvar *fn1-done* (promise))
(defvar *fn2-done* (promise))
(defvar *fn3-done* (promise))

(defun fn1 (node item)
  (declare (ignore item))
  (if (< *iter-count* *num-items*)
      (format nil "~a" (incf *iter-count*))
      (complete-task node)))

(defun fn2 (node item)
  (declare (ignore node))
  (format nil "~a" (1+ (parse-integer item))))

(defun fn3 (node item)
  (declare (ignore node))
  (format nil "~a" (expt (parse-integer item) 2)))

(define-system (:pull-first nil)
  (:name test-node1 :fn #'fn1 :batch-size 1
   :stop-fn #'(lambda () (fulfill *fn1-done* t)))
  (:name test-node2 :fn #'fn2 :batch-size 1
   :stop-fn #'(lambda () (fulfill *fn2-done* t)))
  (:name test-node3 :fn #'fn3 :batch-size 1
   :stop-fn #'(lambda () (fulfill *fn3-done* t))))

(defun calculate-results ()
  (iter:iterate
    (iter:for i from 1 to *num-items*)
    (iter:collect (expt (1+ i) 2))))

(deftest bounded-tasks
  (let* ((results (calculate-results))
         (client-port 55555)
         (client-name (format nil "test-client-~a" (uuid:make-v4-uuid)))
         (uri (format nil "tcp://localhost:~a" client-port))
         (master (start-master 2 client-port))
         (worker1 (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                    :password *rmq-pass*))
         (worker2 (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                    :password *rmq-pass*))
         (i 0)
         (p (promise)))

    ;; TODO: This should not be necessary (wtf?!)
    (add-recipe master (build-test-node1-recipe))
    (add-recipe master (build-test-node2-recipe))
    (add-recipe master (build-test-node3-recipe))

    (define-rmq-node checking-node
        #'(lambda (node item)
            (declare (ignore node))
            (ok (member (parse-integer item) results :test #'=))
            (when (= i (length results))
              (fulfill p t)))
      1 :source-queue queue-4)

    (let ((check-node (build-checking-node
                       "check node" queue-4 :check
                       *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
      (bt:make-thread #'(lambda ()
                          (start-worker worker1 uri)
                          (run-worker worker1)
                          (sleep .1)
                          (stop-worker worker1)
                          (pass "worker1-stopped")))
      (bt:make-thread #'(lambda ()
                          (start-worker worker2 uri)
                          (run-worker worker2)
                          (sleep .1)
                          (stop-worker worker2)
                          (pass "worker2-stopped")))

      (sleep .1)
      (startup check-node *test-context* "inproc://test")

      (pzmq:with-context nil
        (pzmq:with-socket client :dealer
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client uri)

          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE2"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE1"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE2"))
          (test-request-success client)

          (force p)
          (force *fn1-done*)
          (force *fn2-done*)
          (force *fn3-done*)

          (ok (ghash-table-empty? (worker/nodes worker1)))
          (ok (ghash-table-empty? (worker/nodes worker2)))

          (fail "test")

          (send-msg client *mmop-v0* (mmop-c:stop-worker-request-v0 (worker/name worker1)))
          (test-shutdown-success client)
          (send-msg client *mmop-v0* (mmop-c:stop-worker-request-v0 (worker/name worker2)))
          (test-shutdown-success client)))

      (shutdown check-node)
      (stop-master master))))
