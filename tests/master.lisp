(defpackage monomyth/tests/master
  (:use :cl :rove :monomyth/master :monomyth/mmop :monomyth/rmq-node-recipe :stmx.util
        :monomyth/node-recipe :monomyth/worker :cl-rabbit :monomyth/rmq-node :monomyth/node
   :monomyth/rmq-worker :stmx :monomyth/tests/utils)
  (:shadow :closer-mop))
(in-package :monomyth/tests/master)

(defparameter *test-process-time* 3)
(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))
(defparameter queue-1 (format nil "process-test-~a-1" (get-universal-time)))
(defparameter queue-2 (format nil "process-test-~a-2" (get-universal-time)))
(defparameter queue-3 (format nil "process-test-~a-3" (get-universal-time)))
(defparameter queue-4 (format nil "process-test-~a-4" (get-universal-time)))
(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))
(defparameter *rmq-user* (uiop:getenv "TEST_RMQ_DEFAULT_USER"))
(defparameter *rmq-pass* (uiop:getenv "TEST_RMQ_DEFAULT_PASS"))
(v:output-here *terminal-io*)

(teardown
  (let ((conn (setup-connection :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
    (with-channel (conn 1)
      (queue-delete conn 1 *source-queue*)
      (queue-delete conn 1 *dest-queue*)
      (queue-delete conn 1 queue-1)
      (queue-delete conn 1 queue-2)
      (queue-delete conn 1 queue-3)
      (queue-delete conn 1 queue-4)
      (queue-delete conn 1 "TEST1-fail")
      (queue-delete conn 1 "TEST2-fail")
      (queue-delete conn 1 "TEST3-fail")
      (queue-delete conn 1 "TEST-fail"))
    (destroy-connection conn)))

(deftest start-stop
  (let ((master (start-master 4 55555)))
    (sleep .1)
    (stop-master master)
    (pass "master-stopped"))
  (skip "delete threads"))

(deftest can-handle-worker-messages
  (let* ((master (start-master 2 55555))
         (client1-name (format nil "client-~a" (uuid:make-v4-uuid)))
         (client2-name (format nil "client-~a" (uuid:make-v4-uuid)))
         (client3-name (format nil "client-~a" (uuid:make-v4-uuid)))
         (clients `(,client1-name ,client2-name ,client3-name))
         (recipe1 (build-test-recipe1 "test1" "test2" 10))
         (recipe2 (build-test-recipe2 "test2" "test3" 10)))

    (pzmq:with-sockets (((client1 (master-context master)) :dealer)
                        ((client2 (master-context master)) :dealer)
                        ((client3 (master-context master)) :dealer))
      (pzmq:setsockopt client1 :identity client1-name)
      (pzmq:setsockopt client2 :identity client2-name)
      (pzmq:setsockopt client3 :identity client3-name)
      (pzmq:connect client1 "tcp://localhost:55555")
      (pzmq:connect client2 "tcp://localhost:55555")
      (pzmq:connect client3 "tcp://localhost:55555")
      (sleep .1)

      (testing "worker-ready-v0"
        (send-msg client1 *mmop-v0* mmop-w:worker-ready-v0)
        (send-msg client2 *mmop-v0* mmop-w:worker-ready-v0)
        (send-msg client3 *mmop-v0* mmop-w:worker-ready-v0)
        (sleep .1)

        (iter:iterate
          (iter:for client in (ghash-keys (master-workers master)))
          (ok (member client clients :test #'string=)))
        (ok (= 3 (ghash-table-count (master-workers master)))))

      (testing "add recipes"
        (add-recipe master recipe1)
        (add-recipe master recipe2)

        (ok (= 2 (ghash-table-count (master-recipes master))))
        (iter:iterate
          (iter:for type-id in (ghash-keys (master-recipes master)))
          (ok (member type-id '("TEST1" "TEST2") :test #'string=))))

      (let ((c1-reqs nil)
            (c2-reqs nil)
            (c3-reqs nil))
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

              (ok (ask-to-start-node master "TEST1"))
              (sleep .1)
              (test-clients-got-message "TEST1" recipe1)
              (ok (ask-to-start-node master "TEST1"))
              (sleep .1)
              (test-clients-got-message "TEST1" recipe1)
              (ok (ask-to-start-node master "TEST1"))
              (sleep .1)
              (test-clients-got-message "TEST1" recipe1)
              (ok (ask-to-start-node master "TEST1"))
              (sleep .1)
              (test-clients-got-message "TEST1" recipe1)
              (ok (ask-to-start-node master "TEST2"))
              (sleep .1)
              (test-clients-got-message "TEST2" recipe2)
              (ng (ask-to-start-node master "TEST3"))

              (test-master-state-after-asks master client1-name c1-reqs)
              (test-master-state-after-asks master client2-name c2-reqs)
              (test-master-state-after-asks master client3-name c3-reqs))))

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

        (testing "stop-worker"
          (iter:iterate
            (iter:for client in clients)
            (ask-to-shutdown-worker master client))

          (ok (typep (mmop-w:pull-worker-message client1) 'mmop-w:shutdown-worker-v0))
          (ok (typep (mmop-w:pull-worker-message client2) 'mmop-w:shutdown-worker-v0))
          (ok (typep (mmop-w:pull-worker-message client3) 'mmop-w:shutdown-worker-v0)))))

    (stop-master master)))

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
    (iter:for msg = (if (zerop (random 2))
                        (progn
                          (incf (gethash req results-table 0))
                          (mmop-w:start-node-success-v0 req))
                        (mmop-w:start-node-failure-v0 req "test" "test")))
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
    (let ((work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             *source-queue* *dest-queue* *dest-queue* 10 *rmq-host*
                             *rmq-user* *rmq-pass*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed")))
      (startup work-node nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node *source-queue* item))
      (shutdown work-node)

      (let* ((client-port 55555)
             (recipe1 (build-test-recipe *source-queue* *dest-queue*))
             (master (start-master 2 client-port))
             (worker (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
        (bt:make-thread #'(lambda ()
                            (start-worker worker (format nil "tcp://localhost:~a" client-port))
                            (run-worker worker)
                            (stop-worker worker)
                            (pass "worker-stopped")))

        (sleep .1)
        (add-recipe master recipe1)
        (ok (ask-to-start-node master "TEST"))
        (sleep *test-process-time*)
        (ok (ask-to-shutdown-worker master (worker/name worker)))
        (stop-master master))

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

  (testing "one worker - two nodes"
    (let ((work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             queue-1 queue-2 queue-3 10 *rmq-host*
                             *rmq-user* *rmq-pass*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed")))
      (startup work-node nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node queue-1 item))
      (shutdown work-node)

      (let* ((client-port 55555)
             (recipe1 (build-test-recipe1 queue-1 queue-2 5))
             (recipe2 (build-test-recipe2 queue-2 queue-3 10))
             (master (start-master 2 client-port))
             (worker (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
        (bt:make-thread #'(lambda ()
                            (start-worker worker (format nil "tcp://localhost:~a" client-port))
                            (run-worker worker)
                            (stop-worker worker)
                            (pass "worker-stopped")))

        (sleep .1)
        (add-recipe master recipe1)
        (add-recipe master recipe2)
        (ok (ask-to-start-node master "TEST1"))
        (ok (ask-to-start-node master "TEST2"))
        (sleep *test-process-time*)
        (ok (ask-to-shutdown-worker master (worker/name worker)))
        (stop-master master))

      (setf work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             queue-3 *dest-queue* *dest-queue* 10 *rmq-host*
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
          (ok (string= (rmq-message-body got) (format nil "test2 test1 ~a" item)))
          (ack-message work-node got))
        (shutdown work-node))))

  (testing "two workers"
    (let ((work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             *source-queue* queue-1 *dest-queue* 10 *rmq-host*
                             *rmq-user* *rmq-pass*))
          (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed")))
      (startup work-node nil)
      (iter:iterate
        (iter:for item in items)
        (send-message work-node queue-1 item))
      (shutdown work-node)

      (let* ((client-port 55555)
             (recipe1 (build-test-recipe1 queue-1 queue-2 5))
             (recipe2 (build-test-recipe2 queue-2 queue-3 10))
             (recipe3 (build-test-recipe3 queue-3 queue-4 4))
             (master (start-master 2 client-port))
             (worker1 (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*))
             (worker2 (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
        (bt:make-thread #'(lambda ()
                            (start-worker worker1 (format nil "tcp://localhost:~a" client-port))
                            (run-worker worker1)
                            (stop-worker worker1)
                            (pass "worker1-stopped")))
        (bt:make-thread #'(lambda ()
                            (start-worker worker2 (format nil "tcp://localhost:~a" client-port))
                            (run-worker worker2)
                            (stop-worker worker2)
                            (pass "worker2-stopped")))

        (sleep .1)
        (add-recipe master recipe1)
        (add-recipe master recipe2)
        (add-recipe master recipe3)
        (ok (ask-to-start-node master "TEST3"))
        (ok (ask-to-start-node master "TEST3"))
        (ok (ask-to-start-node master "TEST2"))
        (ok (ask-to-start-node master "TEST1"))
        (ok (ask-to-start-node master "TEST2"))
        (sleep *test-process-time*)
        (ok (ask-to-shutdown-worker master (worker/name worker1)))
        (ok (ask-to-shutdown-worker master (worker/name worker2)))
        (stop-master master))

      (setf work-node
            (build-test-node (format nil "worknode-~d" (get-universal-time))
                             queue-4 *dest-queue* *dest-queue* 10 *rmq-host*
                             *rmq-user* *rmq-pass*))
      (startup work-node nil)

      (let ((results (iter:iterate
                       (iter:for item in items)
                       (iter:collect (format nil "test3 test2 test1 ~a" item)))))
        (labels ((get-msg-w-restart ()
                   (handler-case (get-message work-node)
                     (rabbitmq-error (c)
                       (declare (ignore c))
                       (sleep .1)
                       (get-msg-w-restart)))))
          (iter:iterate
            (iter:for item in items)
            (iter:for got = (get-msg-w-restart))
            (ok (member (rmq-message-body got) results :test #'string=))
            (ack-message work-node got))
          (shutdown work-node))))))
