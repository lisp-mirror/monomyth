(defpackage monomyth/tests/rmq-worker
  (:use :cl :prove :monomyth/worker :monomyth/rmq-worker))
(in-package :monomyth/tests/rmq-worker)

(plan nil)

(defparameter *worker* (build-rmq-worker "127.0.0.1" 4 :host (uiop:getenv "TEST_RMQ")
                                         :port 5672 :username "guest" :password "guest"))
(start-worker *worker*)

(stop-worker *worker*)

(finalize)
