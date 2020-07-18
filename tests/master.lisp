(defpackage monomyth/tests/master
  (:use :cl :rove :monomyth/master))
(in-package :monomyth/tests/master)

(vom:config t :info)

(deftest start-stop
  (stop-master (start-master 4 55555))
  (pass "master-stopped"))

(deftest can-handle-worker-messages
  (let ((master (start-master 2 55555))
        (client1-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (client2-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (client3-name (format nil "client-~a" (uuid:make-v4-uuid))))
    (pzmq:with-sockets (((client1 (master-context master)) :dealer)
                        ((client2 (master-context master)) :dealer)
                        ((client3 (master-context master)) :dealer))
      (pzmq:setsockopt client1 :identity client1-name)
      (pzmq:setsockopt client2 :identity client2-name)
      (pzmq:setsockopt client3 :identity client3-name)
      (pzmq:connect client1 "tcp://localhost:55555")
      (pzmq:connect client2 "tcp://localhost:55555")
      (pzmq:connect client3 "tcp://localhost:55555")

      (testing "worker-ready-v0"
        ))))
