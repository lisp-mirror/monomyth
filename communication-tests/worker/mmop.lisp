(defpackage monomyth/communication-tests-worker/mmop
  (:use :cl :rove :monomyth/mmop :monomyth/rmq-node-recipe :monomyth/node-recipe))
(in-package :monomyth/communication-tests-worker/mmop)

(defun get-master-ip ()
  (let ((env-value (uiop:getenv "TEST_MASTER_IP")))
    (if env-value env-value
        (progn
          (format t "Please supply the master ip address:~%")
          (read-line)))))

(defparameter *master-uri* (format nil "tcp://~a:55555" (get-master-ip)))

(deftest to-router
  (testing "single message"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-socket server :dealer
          (pzmq:setsockopt server :identity client-name)
          (pzmq:connect server *master-uri*)

          (send-msg-frames server "mmop/test" '("1" "2" "3"))
          (pass "message sent")))))

  (sleep .1)

  (testing "double messages"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-socket server :dealer
          (pzmq:setsockopt server :identity client-name)
          (pzmq:connect server *master-uri*)

          (send-msg-frames server "mmop/test" '("1" "2" "3"))
          (send-msg-frames server "mmop/test" '("test"))
          (pass "messages sent"))))))

(sleep .1)

(deftest to-dealer
  (testing "single message"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-socket server :dealer
          (pzmq:setsockopt server :identity client-name)
          (pzmq:connect server *master-uri*)

          (send-msg-frames server "mmop/test" '("READY"))
          (ok (equal (pull-msg server) '("1" "2" "3")))))))

  (sleep .1)

  (testing "multiple messages"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-socket server :dealer
          (pzmq:setsockopt server :identity client-name)
          (pzmq:connect server *master-uri*)

          (send-msg-frames server "mmop/test" '("READY"))
          (ok (equal (pull-msg server) '("1" "2" "3"))))))))

(sleep .1)

(deftest MMOP/0
  (testing "worker-ready"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-socket server :dealer
          (pzmq:setsockopt server :identity client-name)
          (pzmq:connect server *master-uri*)

          (send-msg server *mmop-v0* (mmop-w:make-worker-ready-v0))))))

  (sleep .1)

  (testing "start-node"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
          (recipe (make-instance 'rmq-node-recipe :type :test :source "test-s" :dest "test-d")))
      (pzmq:with-context nil
        (pzmq:with-socket server :dealer
          (pzmq:setsockopt server :identity client-name)
          (pzmq:connect server *master-uri*)

          (send-msg server *mmop-v0* (mmop-w:make-worker-ready-v0))
          (let ((res (mmop-w:pull-worker-message server)))
            (ok (typep res 'mmop-w:start-node-v0))
            (ok (string= (mmop-w:start-node-v0-type res) "TEST"))
            (let ((got-res (mmop-w:start-node-v0-recipe res)))
              (ok (eq (node-recipe/type got-res) (node-recipe/type recipe)))
              (ok (string= (rmq-node-recipe/source-queue got-res) (rmq-node-recipe/source-queue recipe)))
              (ok (string= (rmq-node-recipe/dest-queue got-res) (rmq-node-recipe/dest-queue recipe)))))))))

  (sleep .1)

  (testing "node-start-success"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-socket server :dealer
          (pzmq:setsockopt server :identity client-name)
          (pzmq:connect server *master-uri*)

          (send-msg server *mmop-v0* (mmop-w:make-start-node-success-v0 "TEST"))
          (pass "message sent")))))

  (sleep .1)

  (testing "node-start-failure"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-socket server :dealer
          (pzmq:setsockopt server :identity client-name)
          (pzmq:connect server *master-uri*)

          (send-msg server *mmop-v0* (mmop-w:make-start-node-failure-v0
                                      "TEST" "test" "test-msg"))
          (pass "send message")))))

  (sleep .1)

  (testing "stop-worker"
    (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
      (pzmq:with-context nil
        (pzmq:with-socket server :dealer
          (pzmq:setsockopt server :identity client-name)
          (pzmq:connect server *master-uri*)

          (send-msg server *mmop-v0* (mmop-w:make-worker-ready-v0))
          (ok (typep (mmop-w:pull-worker-message server) 'mmop-w:shutdown-worker-v0)))))))
