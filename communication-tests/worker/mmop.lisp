(defpackage monomyth/communication-tests-worker/mmop
  (:use :cl :prove :monomyth/mmop :monomyth/rmq-node-recipe :monomyth/node-recipe))
(in-package :monomyth/communication-tests-worker/mmop)

(plan nil)

(defun get-master-ip ()
  (let ((env-value (uiop:getenv "TEST_MASTER_IP")))
    (if env-value env-value
        (progn
          (format t "Please supply the master ip address:~%")
          (read-line)))))

(defparameter *master-uri* (format nil "tcp://~a:55555" (get-master-ip)))

(subtest "msg-happy-path-to-router"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-socket server :dealer
        (pzmq:setsockopt server :identity client-name)
        (pzmq:connect server *master-uri*)

        (send-msg-frames server "mmop/test" '("1" "2" "3"))))))

(subtest "msg-happy-path-to-router"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-socket server :dealer
        (pzmq:setsockopt server :identity client-name)
        (pzmq:connect server *master-uri*)

        (send-msg-frames server "mmop/test" '("1" "2" "3"))
        (send-msg-frames server "mmop/test" '("test"))))))

(subtest "msg happy path to dealer"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-socket server :dealer
        (pzmq:setsockopt server :identity client-name)
        (pzmq:connect server *master-uri*)

        (send-msg-frames server "mmop/test" '("READY"))
        (is (pull-msg server) '("1" "2" "3"))))))

(subtest "msg happy path to dealer - second message"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-socket server :dealer
        (pzmq:setsockopt server :identity client-name)
        (pzmq:connect server *master-uri*)

        (send-msg-frames server "mmop/test" '("READY"))
        (is (pull-msg server) '("1" "2" "3"))))))

(subtest "MMOP/0 worker-ready"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-socket server :dealer
        (pzmq:setsockopt server :identity client-name)
        (pzmq:connect server *master-uri*)

        (send-msg server *mmop-v0* (mmop-w:make-worker-ready-v0))))))

(subtest "MMOP/0 start-node"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid)))
        (recipe (build-rmq-node-recipe :test "#'(lambda (x) (1+ x))" "test-s" "test-d")))
    (pzmq:with-context nil
      (pzmq:with-socket server :dealer
        (pzmq:setsockopt server :identity client-name)
        (pzmq:connect server *master-uri*)

        (send-msg server *mmop-v0* (mmop-w:make-worker-ready-v0))
        (let ((res (mmop-w:pull-worker-message server)))
          (is-type res 'mmop-w:start-node-v0)
          (is (mmop-w:start-node-v0-type res) "TEST")
          (let ((got-res (mmop-w:start-node-v0-recipe res)))
            (is (node-recipe/type got-res) (node-recipe/type recipe))
            (is (node-recipe/transform-fn got-res) (node-recipe/transform-fn recipe))
            (is (rmq-node-recipe/source-queue got-res) (rmq-node-recipe/source-queue recipe))
            (is (rmq-node-recipe/dest-queue got-res) (rmq-node-recipe/dest-queue recipe))))))))

(subtest "MMOP/0 node-start-success"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-socket server :dealer
        (pzmq:setsockopt server :identity client-name)
        (pzmq:connect server *master-uri*)

        (send-msg server *mmop-v0* (mmop-w:make-start-node-success-v0 "TEST"))))))

(subtest "MMOP/0 node-start-failure"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-socket server :dealer
        (pzmq:setsockopt server :identity client-name)
        (pzmq:connect server *master-uri*)

        (send-msg server *mmop-v0* (mmop-w:make-start-node-failure-v0
                                    "TEST" "test" "test-msg"))))))

(subtest "MMOP/0 stop-worker"
  (let ((client-name (format nil "client-~a" (uuid:make-v4-uuid))))
    (pzmq:with-context nil
      (pzmq:with-socket server :dealer
        (pzmq:setsockopt server :identity client-name)
        (pzmq:connect server *master-uri*)

        (send-msg server *mmop-v0* (mmop-w:make-worker-ready-v0))
        (is-type (mmop-w:pull-worker-message server) 'mmop-w:shutdown-worker-v0)))))

(finalize)
