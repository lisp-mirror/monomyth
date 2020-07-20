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

      (testing "ask to start node"
        (let ((c1-reqs nil)
              (c2-reqs nil)
              (c3-reqs nil))
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

              (print c1-reqs)
              (print c2-reqs)
              (print c3-reqs)
            )))))

    (stop-master master)))

(defun test-client-recieves-start-node (socket type-id recipe)
  (let ((msg (mmop-w:pull-worker-message socket)))
    (ok (typep msg 'mmop-w:start-node-v0))
    (ok (string= type-id (mmop-w:start-node-v0-type msg)))
    (let ((got-recipe (mmop-w:start-node-v0-recipe msg)))
      (ok (eq (node-recipe/type recipe) (node-recipe/type got-recipe))))))
