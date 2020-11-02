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
   test-node1
   test-node2
   test-node3
   work-node
   test-node-recipe
   test-node-recipe1
   test-node-recipe2
   test-node-recipe3
   build-test-node
   build-test-node1
   build-test-node2
   build-test-node3
   build-work-node
   build-test-node-recipe
   build-test-node1-recipe
   build-test-node2-recipe
   build-test-node3-recipe
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

(defun fn1 (item)
  (format nil "test1 ~a" item))

(defun fn2 (item)
  (format nil "test2 ~a" item))

(defun fn3 (item)
  (format nil "test3 ~a" item))

(define-system
    (:name test-node1 :fn #'fn1 :batch-size 5)
    (:name test-node2 :fn #'fn2 :batch-size 10)
    (:name test-node3 :fn #'fn3 :batch-size 4))

(defun fn (item)
  (format nil "test ~a" item))

(define-rmq-node test-node #'fn *source-queue* *dest-queue* 1)

(define-rmq-node work-node nil *dest-queue* *source-queue* 10)

(defun test-request-success (socket)
  (adt:match mmop-c:received-mmop (mmop-c:pull-control-message socket)
    ((mmop-c:start-node-request-success-v0) (pass "request succeeded message"))
    (_ (fail "unexpected message type"))))

(defun test-shutdown-success (socket)
  (adt:match mmop-c:received-mmop (mmop-c:pull-control-message socket)
    ((mmop-c:stop-worker-request-success-v0) (pass "stop worker succeeded message"))
    (_ (fail "unexpected message type"))))
