(defpackage monomyth/tests/utils
  (:use :cl :monomyth/rmq-worker :monomyth/rmq-node-recipe :monomyth/rmq-node
        :monomyth/node :monomyth/worker :stmx :monomyth/node-recipe :rove
        :monomyth/dsl :monomyth/master)
  (:shadow :closer-mop)
  (:export
   *source-queue*
   *dest-queue*
   queue-1
   queue-2
   queue-3
   queue-4
   *rmq-host*
   *rmq-port*
   *rmq-user*
   *rmq-pass*
   test-node
   large-test-node
   work-node
   test-node-recipe
   build-test-node
   build-large-test-node
   build-work-node
   build-test-node-recipe
   test-request-success
   test-shutdown-success))
(in-package :monomyth/tests/utils)

(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))
(defparameter *rmq-port* 5672)
(defparameter *rmq-user* (uiop:getenv "TEST_RMQ_DEFAULT_USER"))
(defparameter *rmq-pass* (uiop:getenv "TEST_RMQ_DEFAULT_PASS"))
(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))
(defparameter queue-1 "START-to-TEST-NODE1")
(defparameter queue-2 "TEST-NODE1-to-TEST-NODE2")
(defparameter queue-3 "TEST-NODE2-to-TEST-NODE3")
(defparameter queue-4 "TEST-NODE3-to-END")

(defun fn (item)
  (format nil "test ~a" item))

(define-rmq-node test-node #'fn 1 :source-queue *source-queue* :dest-queue *dest-queue*)

(define-rmq-node large-test-node #'fn 10 :source-queue *source-queue* :dest-queue *dest-queue*)

(define-rmq-node work-node nil 10 :source-queue *dest-queue* :dest-queue *source-queue*)

(defun test-request-success (socket)
  (adt:match mmop-c:received-mmop (mmop-c:pull-control-message socket)
    ((mmop-c:start-node-request-success-v0) (pass "request succeeded message"))
    (_ (fail "unexpected message type"))))

(defun test-shutdown-success (socket)
  (adt:match mmop-c:received-mmop (mmop-c:pull-control-message socket)
    ((mmop-c:stop-worker-request-success-v0) (pass "stop worker succeeded message"))
    (_ (fail "unexpected message type"))))
