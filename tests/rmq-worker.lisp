(defpackage monomyth/tests/rmq-worker
  (:use :cl :prove :monomyth/rmq-worker :monomyth/worker :monomyth/mmop))
(in-package :monomyth/tests/rmq-worker)

(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))

(plan nil)
(vom:config t :info)

(subtest "worker starts/shutdown"
  (bt:make-thread
   #'(lambda ()
       (pzmq:with-context nil
         (pzmq:with-socket master :router
           (pzmq:bind master "tcp://*:55555")
           (let ((msg (mmop-m:pull-master-message master)))
             (is-type msg 'mmop-m:worker-ready-v0)
             (sleep .1)
             (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0
                                         (mmop-m:worker-ready-v0-client-id msg))))))))
  (let ((wrkr (build-rmq-worker :host *rmq-host*)))
    (start-worker wrkr "tcp://localhost:55555")
    (run-worker wrkr)
    (stop-worker wrkr)
    (pass "worker stopped")))

(finalize)
