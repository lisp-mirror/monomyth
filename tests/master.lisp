(defpackage monomyth/tests/master
  (:use :cl :rove :monomyth/master :monomyth/mmop :monomyth/rmq-node-recipe :stmx.util
        :monomyth/node-recipe))
(in-package :monomyth/tests/master)

(v:output-here *terminal-io*)

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
         (recipe1 (build-rmq-node-recipe
                   :test1 "#'(lambda (x) (format nil \"test1 ~a\" x))"
                   "test1" "test2" 5))
         (recipe2 (build-rmq-node-recipe
                   :test2 "#'(lambda (x) (format nil \"test2 ~a\" x))"
                   "test2" "test3")))

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
        (send-msg client1 *mmop-v0* (mmop-w:make-worker-ready-v0))
        (send-msg client2 *mmop-v0* (mmop-w:make-worker-ready-v0))
        (send-msg client3 *mmop-v0* (mmop-w:make-worker-ready-v0))
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
            (test-resonses master client3-name c3-expected-results)))))

    (stop-master master)))

(defun test-client-recieves-start-node (socket type-id recipe)
  (let ((msg (mmop-w:pull-worker-message socket)))
    (ok (typep msg 'mmop-w:start-node-v0))
    (ok (string= type-id (mmop-w:start-node-v0-type msg)))
    (let ((got-recipe (mmop-w:start-node-v0-recipe msg)))
      (ok (eq (node-recipe/type recipe) (node-recipe/type got-recipe))))))

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
                          (mmop-w:make-start-node-success-v0 req))
                        (mmop-w:make-start-node-failure-v0 req "test" "test")))
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
