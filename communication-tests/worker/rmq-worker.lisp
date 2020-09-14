(defpackage monomyth/communication-tests-worker/rmq-worker
  (:use :cl :rove :cl-rabbit :monomyth/rmq-worker :monomyth/worker :monomyth/mmop
        :monomyth/rmq-node-recipe :monomyth/rmq-node :monomyth/node
        :monomyth/tests/utils))
(in-package :monomyth/communication-tests-worker/rmq-worker)

(v:output-here *terminal-io*)
(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))
(defparameter *rmq-user* (uiop:getenv "TEST_RMQ_DEFAULT_USER"))
(defparameter *rmq-pass* (uiop:getenv "TEST_RMQ_DEFAULT_PASS"))

(defclass test-bad-recipe (rmq-node-recipe) ())

(defun build-bad-test-recipe ()
  (make-instance 'test-bad-recipe :source "doesnt-matter" :dest "at-all" :type :test))

(defun get-master-ip ()
  (let ((env-value (uiop:getenv "TEST_MASTER_IP")))
    (if env-value env-value
        (progn
          (format t "Please supply the master ip address:~%")
          (read-line)))))

(defparameter *master-uri* (format nil "tcp://~a:55555" (get-master-ip)))

(deftest worker-starts-shutdown
  (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
    (start-worker wrkr *master-uri*)
    (run-worker wrkr)
    (stop-worker wrkr)
    (pass "worker stopped")))

(deftest worker-can-start-node
  (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
    (start-worker wrkr *master-uri*)
    (run-worker wrkr)
    (stop-worker wrkr)
    (pass "worker stopped")))

(deftest worker-catches-bad-fn
  (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
    (start-worker wrkr *master-uri*)
    (run-worker wrkr)
    (stop-worker wrkr)
    (pass "worker stopped"))
  (skip "worker catches bad recipe type"))

(deftest worker-processes-data
  (testing "single node"
    (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
      (start-worker wrkr *master-uri*)
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped")))

  (testing "worker processes data - multiple nodes"
    (let ((wrkr (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
      (start-worker wrkr *master-uri*)
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped"))))
