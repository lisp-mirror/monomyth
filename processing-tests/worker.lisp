(defpackage monomyth/processing-tests/worker
  (:use :cl :rove :monomyth/processing-tests/utils :monomyth/rmq-worker
        :monomyth/worker))
(in-package :monomyth/processing-tests/worker)

(defparameter *master-host* (uiop:getenv "TEST_MASTER_IP"))
(v:output-here *terminal-io*)

(deftest worker-processing-test
  (let ((worker (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
    (start-worker worker (format nil "tcp://~a:~a" *master-host* *mmop-port*))
    (run-worker worker)
    (stop-worker worker)
    (pass "worker stopped")))
