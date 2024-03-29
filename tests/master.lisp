(defpackage monomyth/tests/master
  (:use :cl :rove :monomyth/master :monomyth/mmop :monomyth/rmq-node-recipe :stmx.util
        :monomyth/node-recipe :monomyth/worker :cl-rabbit :monomyth/rmq-node :monomyth/node
   :monomyth/rmq-worker :stmx :monomyth/tests/utils :monomyth/dsl
   :lparallel)
  (:shadow :closer-mop))
(in-package :monomyth/tests/master)

(v:output-here *terminal-io*)
(defvar *test-context*)

(defun fn1 (node item)
  (declare (ignore node))
  (format nil "test1 ~a" item))

(defun fn2 (node item)
  (declare (ignore node))
  (format nil "test2 ~a" item))

(defun fn3 (node item)
  (declare (ignore node))
  (format nil "test3 ~a" item))

(define-system master-system ()
    (:name test-node1 :fn #'fn1 :batch-size 5)
    (:name test-node2 :fn #'fn2 :batch-size 10)
    (:name test-node3 :fn #'fn3 :batch-size 4))

(define-rmq-node final-work-node nil 10 :source-queue queue-4 :dest-queue queue-4)

(define-rmq-node final-work-node1 nil 10 :source-queue queue-3 :dest-queue queue-3)

(setup
  (setf *test-context* (pzmq:ctx-new)))

(teardown
  (pzmq:ctx-destroy *test-context*)
  (let ((conn (setup-connection :host *rmq-host* :username *rmq-user*
                                :password *rmq-pass*)))
    (with-channel (conn 1)
      (queue-delete conn 1 *source-queue*)
      (queue-delete conn 1 *dest-queue*)
      (queue-delete conn 1 queue-1)
      (queue-delete conn 1 queue-2)
      (queue-delete conn 1 queue-3)
      (queue-delete conn 1 queue-4)
      (queue-delete conn 1 "TEST-NODE1-fail")
      (queue-delete conn 1 "TEST-NODE2-fail")
      (queue-delete conn 1 "TEST-NODE3-fail")
      (queue-delete conn 1 "TEST-NODE-fail"))
    (destroy-connection conn)))

(deftest start-stop
  (let ((master (start-master 4 55555)))
    (add-master-system-recipes master)
    (sleep .1)
    (stop-master master)
    (pass "master-stopped"))
  (skip "delete threads"))

(deftest can-handle-worker-messages
  (let* ((master (start-master 2 55555))
         (uri "tcp://localhost:55555")
         (client1-name (format nil "client-~a" (uuid:make-v4-uuid)))
         (client2-name (format nil "client-~a" (uuid:make-v4-uuid)))
         (client3-name (format nil "client-~a" (uuid:make-v4-uuid)))
         (clients `(,client1-name ,client2-name ,client3-name))
         (recipe1 (build-test-node1-recipe))
         (recipe2 (build-test-node2-recipe)))

    (add-master-system-recipes master)

    (pzmq:with-sockets (((client1 (master-context master)) :dealer)
                        ((client2 (master-context master)) :dealer)
                        ((client3 (master-context master)) :dealer))
      (pzmq:setsockopt client1 :identity client1-name)
      (pzmq:setsockopt client2 :identity client2-name)
      (pzmq:setsockopt client3 :identity client3-name)
      (pzmq:connect client1 uri)
      (pzmq:connect client2 uri)
      (pzmq:connect client3 uri)
      (sleep .1)

      (testing "start-node no workers"
        (send-msg client1 *mmop-v0* (mmop-c:start-node-request-v0 "test"))
        (adt:match mmop-c:received-mmop (mmop-c:pull-control-message client1)
          ((mmop-c:start-node-request-failure-v0 msg code)
           (ok (= code 503))
           (ok (string= msg "no active worker servers")))
          (_ (fail "unexpected message type"))))

      (testing "worker-ready-v0"
        (send-msg client1 *mmop-v0* mmop-w:worker-ready-v0)
        (send-msg client2 *mmop-v0* mmop-w:worker-ready-v0)
        (send-msg client3 *mmop-v0* mmop-w:worker-ready-v0)
        (sleep .1)

        (iter:iterate
          (iter:for client in (ghash-keys (master-workers master)))
          (ok (member client clients :test #'string=)))
        (ok (= 3 (ghash-table-count (master-workers master)))))

      (testing "recipes added"
        (ok (= 3 (ghash-table-count (master-recipes master))))
        (iter:iterate
          (iter:for type-id in (ghash-keys (master-recipes master)))
          (ok (member type-id '("TEST-NODE1" "TEST-NODE2" "TEST-NODE3")
                      :test #'string=))))

      (let ((c1-reqs nil)
            (c2-reqs nil)
            (c3-reqs nil)
            (client-name (format nil "node-client-~a" (uuid:make-v4-uuid))))
        (testing "asking to start node"
          (pzmq:with-poll-items items (client1 client2 client3)
            (labels ((test-clients-got-message (type-id recipe)
                       (pzmq:poll items)
                       (cond
                         ((member :pollin (pzmq:revents items 0))
                          (progn
                            (push type-id c1-reqs)
                            (test-client-recieves-start-node client1 type-id recipe)))

                         ((member :pollin (pzmq:revents items 1))
                          (progn
                            (push type-id c2-reqs)
                            (test-client-recieves-start-node client2 type-id recipe)))

                         ((member :pollin (pzmq:revents items 2))
                          (progn
                            (push type-id c3-reqs)
                            (test-client-recieves-start-node client3 type-id recipe)))

                         (t (fail "message not received")))))

              (pzmq:with-context nil
                (pzmq:with-socket client :dealer
                  (pzmq:setsockopt client :identity client-name)
                  (pzmq:connect client uri)

                  (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE1"))
                  (sleep .1)
                  (test-clients-got-message "TEST-NODE1" recipe1)
                  (test-request-success client)
                  (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE1"))
                  (sleep .1)
                  (test-clients-got-message "TEST-NODE1" recipe1)
                  (test-request-success client)
                  (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE1"))
                  (sleep .1)
                  (test-clients-got-message "TEST-NODE1" recipe1)
                  (test-request-success client)
                  (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE1"))
                  (sleep .1)
                  (test-clients-got-message "TEST-NODE1" recipe1)
                  (test-request-success client)
                  (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE2"))
                  (sleep .1)
                  (test-clients-got-message "TEST-NODE2" recipe2)
                  (test-request-success client)
                  (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE4"))
                  (adt:match mmop-c:received-mmop (mmop-c:pull-control-message client)
                    ((mmop-c:start-node-request-failure-v0 _)
                     (pass "request succeeded message"))
                    (_ (fail "unexpected message type")))))

              (testing "node balance"
                (ok (member '("TEST-NODE1" "TEST-NODE1") `(,c1-reqs ,c2-reqs ,c3-reqs)
                            :test #'equal))
                (ok (member '("TEST-NODE1") `(,c1-reqs ,c2-reqs ,c3-reqs) :test #'equal))
                (ok (member '("TEST-NODE2" "TEST-NODE1") `(,c1-reqs ,c2-reqs ,c3-reqs)
                            :test #'equal)))

              (testing "master state"
                (test-master-state-after-asks master client1-name c1-reqs)
                (test-master-state-after-asks master client2-name c2-reqs)
                (test-master-state-after-asks master client3-name c3-reqs)))))

        (testing "response messages"
          (let ((c1-expected-results (make-hash-table :test #'equal))
                (c2-expected-results (make-hash-table :test #'equal))
                (c3-expected-results (make-hash-table :test #'equal)))

            (bt:make-thread
             #'(lambda () (respond-to-reqs client1 c1-reqs c1-expected-results)))
            (bt:make-thread
             #'(lambda () (respond-to-reqs client2 c2-reqs c2-expected-results)))
            (bt:make-thread
             #'(lambda () (respond-to-reqs client3 c3-reqs c3-expected-results)))

            (sleep 1)

            (test-resonses master client1-name c1-expected-results)
            (test-resonses master client2-name c2-expected-results)
            (test-resonses master client3-name c3-expected-results)))

        (testing "task complete"
          (iter:iterate
            (iter:for name in (find-active-workers master))
            (iter:for client = (get-socket `(,client1 ,client2 ,client3) name))
            (test-task-complete master client name)))

        (testing "complete task message sent"
          (test-complete-task-msg client1)
          (test-complete-task-msg client2)
          (test-complete-task-msg client3))

        (testing "task complete-failed"
          (flet ((get-count ()
                   (atomic
                    (get-ghash (worker-info-type-counts
                                (get-ghash (master-workers master) client3-name))
                               "TEST-NODE4" :none))))
            (let ((old-count (get-count)))
              (send-msg client1 *mmop-v0*
                        (mmop-w:worker-task-completed-v0 "TEST-NODE4"))
              (sleep .1)

              (ok (eq old-count (get-count)))
              (ok (eq :none (get-count))))))

        (testing "stop worker"
          (pzmq:with-context nil
            (pzmq:with-socket client :dealer
              (pzmq:setsockopt client :identity client-name)
              (pzmq:connect client uri)

              (iter:iterate
                (iter:for client-id in clients)
                (send-msg client *mmop-v0* (mmop-c:stop-worker-request-v0 client-id))
                (test-shutdown-success client))

              (ok (typep (mmop-w:pull-worker-message client1) 'mmop-w:shutdown-worker-v0))
              (ok (typep (mmop-w:pull-worker-message client2) 'mmop-w:shutdown-worker-v0))
              (ok (typep (mmop-w:pull-worker-message client3) 'mmop-w:shutdown-worker-v0))

              (send-msg client *mmop-v0* (mmop-c:stop-worker-request-v0 "fail-test"))
              (adt:match mmop-c:received-mmop (mmop-c:pull-control-message client)
                ((mmop-c:stop-worker-request-failure-v0 msg code)
                 (ok (string= msg "could not shutdown unrecognized worker fail-test"))
                 (ok (= code 400)))
                (_ (fail "unexpected message type"))))))))

    (stop-master master)))

(defun get-socket (sockets name)
  (find-if
   #'(lambda (val) (string= name (pzmq:getsockopt val :identity)))
   sockets))

(defun test-task-complete (master client name)
  (flet ((get-count ()
           (atomic
            (get-ghash (worker-info-type-counts
                        (get-ghash (master-workers master) name))
                       "TEST-NODE2"))))
    (let ((old-count (get-count)))
      (send-msg client *mmop-v0*
                (mmop-w:worker-task-completed-v0 "TEST-NODE2"))
      (sleep .1)

      (ok (= (1- old-count) (get-count)))
      (ok (= 1 (atomic
                (get-ghash (worker-info-tasks-completed
                            (get-ghash (master-workers master) name))
                           "TEST-NODE2")))))))

(defun test-complete-task-msg (client)
  (adt:match mmop-w:received-mmop (mmop-w:pull-worker-message client)
    ((mmop-w:complete-task-v0 sent-node-id)
     (ok (string= sent-node-id "TEST-NODE3")))
    (_ (fail "unexpected message type"))))

(defun find-active-workers (master)
  (remove-if
   #'(lambda (val)
       (zerop (atomic (get-ghash
                       (worker-info-type-counts
                        (get-ghash (master-workers master) val))
                       "TEST-NODE2" 0))))
   (atomic (ghash-keys (master-workers master)))))

(defun test-client-recieves-start-node (socket type-id recipe)
  (adt:match mmop-w:received-mmop (mmop-w:pull-worker-message socket)
    ((mmop-w:start-node-v0 node-type got-recipe)
     (ok (string= type-id node-type))
     (ok (eq (node-recipe/type recipe) (node-recipe/type got-recipe))))
    (_ (fail "unexpected message type"))))

(defun test-master-state-after-asks (master client-id reqs)
  (let ((proper-counts
          (iter:iterate
            (iter:with counts = (make-hash-table :test #'equal))
            (iter:for req in reqs)
            (incf (gethash req counts 0))
            (iter:finally (return counts))))
        (got-counts (worker-info-outstanding-request-counts
                     (get-ghash (master-workers master) client-id))))
    (ok (= (hash-table-count proper-counts) (ghash-table-count got-counts)))
    (iter:iterate
      (iter:for (req proper-count) in-hashtable proper-counts)
      (ok (= proper-count (get-ghash got-counts req))))))

(defun respond-to-reqs (socket reqs results-table)
  (iter:iterate
    (iter:for req in reqs)
    (iter:for msg = (cond
                      ((string= "TEST-NODE2" req)
                       (progn
                         (incf (gethash req results-table 0))
                         (mmop-w:start-node-success-v0 req)))

                      ((zerop (random 2))
                       (progn
                         (incf (gethash req results-table 0))
                         (mmop-w:start-node-success-v0 req)))

                      (t (mmop-w:start-node-failure-v0 req "test" "test"))))
    (send-msg socket *mmop-v0* msg)))

(defun test-resonses (master client-id expected-results)
  (let ((worker (get-ghash (master-workers master) client-id)))
    (iter:iterate
      (iter:for outstanding in
                (ghash-values (worker-info-outstanding-request-counts worker)))
      (ok (zerop outstanding)))

    (iter:iterate
      (iter:for (req running) in-hashtable expected-results)
      (ok (= running (get-ghash (worker-info-type-counts worker) req 0))))))

(deftest process-data-rmq
  (testing "one worker - one node"
    (let* ((work-node
             (build-test-node
              (format nil "worknode-~d" (get-universal-time))
              *dest-queue* :work *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
           (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed"))
           (client-port 55555)
           (uri (format nil "tcp://localhost:~a" client-port))
           (client-name (format nil "node-client-~a" (uuid:make-v4-uuid)))
           (recipe1 (build-test-node-recipe))
           (i 0)
           (p (promise))
           (master (start-master 2 client-port))
           (worker (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                     :password *rmq-pass*)))

      (define-rmq-node checking-node
          #'(lambda (node item)
              (declare (ignore node))
              (ok (string= (format nil "test ~a" (nth i items)) item))
              (incf i)
              (when (= i (length items))
                (fulfill p t)))
        1 :source-queue *dest-queue*)

      (start-node work-node *test-context* "inproc://test" nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node *source-queue* item))
      (stop-node work-node)

      (let ((check-node (build-checking-node
                         "check node" queue-4 :check
                         *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
        (bt:make-thread #'(lambda ()
                            (start-worker worker uri)
                            (run-worker worker)
                            (sleep .1)
                            (stop-worker worker)
                            (pass "worker-stopped")))

        (sleep .1)
        (add-recipe master recipe1)
        (start-node check-node *test-context* "inproc://test")

        (pzmq:with-context nil
          (pzmq:with-socket client :dealer
            (pzmq:setsockopt client :identity client-name)
            (pzmq:connect client uri)
            (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE"))
            (test-request-success client)

            (force p)

            (send-msg client *mmop-v0* (mmop-c:stop-worker-request-v0 (worker/name worker)))
            (test-shutdown-success client)))

        (stop-master master)
        (stop-node check-node))

      (setf work-node
            (build-work-node
             (format nil "worknode-~d" (get-universal-time))
             *dest-queue* :work *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
      (start-node work-node *test-context* "inproc://test" nil)
      (ng (pull-items work-node))
      (stop-node work-node)))

  (testing "one worker - two nodes"
    (let* ((work-node
             (build-test-node
              (format nil "worknode-~d" (get-universal-time))
              queue-1 :work *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
           (items '("1" "3" "testing" "is" "boring" "these" "should" "all"
                    "be processed"))
           (i 0)
           (p (promise))
           (client-port 55555)
           (uri (format nil "tcp://localhost:~a" client-port))
           (client-name (format nil "test-client-~a" (uuid:make-v4-uuid)))
           (master (start-master 2 client-port))
           (worker (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                     :password *rmq-pass*)))

      (add-master-system-recipes master)

      (define-rmq-node checking-node
          #'(lambda (node item)
              (declare (ignore node))
              (ok (string= (format nil "test2 test1 ~a" (nth i items)) item))
              (incf i)
              (when (= i (length items))
                (fulfill p t)))
        1 :source-queue queue-3)

      (start-node work-node *test-context* "inproc://test" nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node queue-1 item))
      (stop-node work-node)

      (let ((check-node (build-checking-node
                         "check node" queue-4 :check
                         *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
        (bt:make-thread #'(lambda ()
                            (start-worker worker uri)
                            (run-worker worker)
                            (sleep .1)
                            (stop-worker worker)
                            (pass "worker-stopped")))

        (sleep .1)
        (start-node check-node *test-context* "inproc://test")

        (pzmq:with-context nil
          (pzmq:with-socket client :dealer
            (pzmq:setsockopt client :identity client-name)
            (pzmq:connect client uri)

            (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE1"))
            (test-request-success client)
            (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE2"))
            (test-request-success client)

            (force p)

            (send-msg client *mmop-v0* (mmop-c:stop-worker-request-v0 (worker/name worker)))
            (test-shutdown-success client)))

        (stop-node check-node)
        (stop-master master))

      (setf work-node
            (build-final-work-node1
             (format nil "worknode-~d" (get-universal-time))
             queue-3 :work *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
      (start-node work-node *test-context* "inproc://test" nil)
      (ng (pull-items work-node))
      (stop-node work-node)))

  (testing "two workers"
    (let* ((work-node
             (build-test-node
              (format nil "worknode-~d" (get-universal-time))
              queue-1 :work *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
           (items '("1" "3" "testing" "is" "boring" "these" "should" "all"
                    "be processed"))
           (results (iter:iterate
                      (iter:for item in items)
                      (iter:collect (format nil "test3 test2 test1 ~a" item))))
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

      (add-master-system-recipes master)

      (define-rmq-node checking-node
          #'(lambda (node item)
              (declare (ignore node))
              (ok (member item results :test #'string=))
              (incf i)
              (when (= i (length items))
                (fulfill p t)))
        1 :source-queue queue-4)

      (start-node work-node *test-context* "inproc://test" nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node queue-1 item))
      (stop-node work-node)

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
        (start-node check-node *test-context* "inproc://test")

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

            (send-msg client *mmop-v0* (mmop-c:stop-worker-request-v0 (worker/name worker1)))
            (test-shutdown-success client)
            (send-msg client *mmop-v0* (mmop-c:stop-worker-request-v0 (worker/name worker2)))
            (test-shutdown-success client)))

        (stop-node check-node)
        (stop-master master))

      (setf work-node
            (build-final-work-node
             (format nil "worknode-~d" (get-universal-time))
             queue-4 :work *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
      (start-node work-node *test-context* "inproc://test" nil)
      (ng (pull-items work-node))
      (stop-node work-node))))
