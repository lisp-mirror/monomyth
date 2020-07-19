(defpackage monomyth/tests/master
  (:use :cl :rove :monomyth/master :monomyth/mmop :stmx.util))
(in-package :monomyth/tests/master)

(vom:config t :info)

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
         (clients `(,client1-name ,client2-name ,client3-name)))
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
        (ok (= 3 (ghash-table-count (master-workers master))))))

    (stop-master master)))
