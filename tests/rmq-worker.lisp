(defpackage monomyth/tests/rmq-worker
  (:use :cl :prove :monomyth/worker :monomyth/rmq-worker :monomyth/rmq-node-recipe :alexandria
        :monomyth/rmq-node))
(in-package :monomyth/tests/rmq-worker)

(plan nil)

(defparameter *worker* (build-rmq-worker "127.0.0.1" 4 :host (uiop:getenv "TEST_RMQ")
                                         :port 5672 :username "guest" :password "guest"))
(defparameter *queue-1* (format nil "test-queue-1-~a" (get-universal-time)))
(defparameter *queue-2* (format nil "test-queue-2-~a" (get-universal-time)))
(defparameter *queue-3* (format nil "test-queue-3-~a" (get-universal-time)))
(defparameter *queue-4* (format nil "test-queue-4-~a" (get-universal-time)))
(start-worker *worker*)
(build-node *worker* (build-rmq-node-recipe :type1 #'(lambda (x) (format nil "test1 ~a" x))
                                            *queue-1* *queue-2* 1))
(build-node *worker* (build-rmq-node-recipe :type2 #'(lambda (x) (format nil "test2 ~a" x))
                                            *queue-2* *queue-3* 1))
(build-node *worker* (build-rmq-node-recipe :type3 #'(lambda (x) (format nil "test3 ~a" x))
                                            *queue-3* *queue-4* 1))

(defparameter *shutdown-node* (second (hash-table-plist (worker/nodes *worker*))))
(delete-queue *shutdown-node* *queue-1*)
(delete-queue *shutdown-node* *queue-2*)
(delete-queue *shutdown-node* *queue-3*)
(delete-queue *shutdown-node* *queue-4*)
(delete-queue *shutdown-node* "TYPE1-fail")
(delete-queue *shutdown-node* "TYPE2-fail")
(delete-queue *shutdown-node* "TYPE3-fail")
(stop-worker *worker*)

(finalize)
