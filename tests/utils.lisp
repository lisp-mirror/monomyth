(defpackage monomyth/tests/utils
  (:use :cl :monomyth/rmq-worker :monomyth/rmq-node-recipe :monomyth/rmq-node
        :monomyth/node :monomyth/worker :stmx :monomyth/node-recipe :rove
        :monomyth/dsl)
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
(defparameter queue-1 (format nil "process-test-~a-1" (get-universal-time)))
(defparameter queue-2 (format nil "process-test-~a-2" (get-universal-time)))
(defparameter queue-3 (format nil "process-test-~a-3" (get-universal-time)))
(defparameter queue-4 (format nil "process-test-~a-4" (get-universal-time)))

(defun fn (item)
  (format nil "test ~a" item))

(define-rmq-node test-node #'fn *source-queue* *dest-queue* 1)

(defun fn1 (item)
  (format nil "test1 ~a" item))

(define-rmq-node test-node1 #'fn1 queue-1 queue-2 5)

(defun fn2 (item)
  (format nil "test2 ~a" item))

(define-rmq-node test-node2 #'fn2 queue-2 queue-3 10)

(defun fn3 (item)
  (format nil "test3 ~a" item))

(define-rmq-node test-node3 #'fn3 queue-3 queue-4 4)

(define-rmq-node work-node nil *dest-queue* *source-queue* 10)

(defun test-request-success (socket)
  (adt:match mmop-c:received-mmop (mmop-c:pull-control-message socket)
    ((mmop-c:start-node-request-success-v0) (pass "request succeeded message"))
    (_ (fail "unexpected message type"))))

(defun test-shutdown-success (socket)
  (adt:match mmop-c:received-mmop (mmop-c:pull-control-message socket)
    ((mmop-c:stop-worker-request-success-v0) (pass "stop worker succeeded message"))
    (_ (fail "unexpected message type"))))
