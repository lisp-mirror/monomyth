(defpackage monomyth/processing-tests/utils
  (:use :cl :monomyth/rmq-worker :monomyth/rmq-node-recipe :monomyth/rmq-node
        :monomyth/node :monomyth/worker :stmx :monomyth/node-recipe
        :monomyth/dsl)
  (:shadow :closer-mop)
  (:export *mmop-port*
           *rmq-host*
           *rmq-port*
           *rmq-user*
           *rmq-pass*
           *queue1*
           *queue2*
           *queue3*
           *queue4*
           *queue5*
           build-test-node
           test-node1
           test-node2
           test-node3
           test-node4
           test-node1-recipe
           test-node2-recipe
           test-node3-recipe
           test-node4-recipe
           build-test-node1-recipe
           build-test-node2-recipe
           build-test-node3-recipe
           build-test-node4-recipe))
(in-package :monomyth/processing-tests/utils)

(defparameter *rmq-host* (uiop:getenv "TEST_PROCESSING_RMQ"))
(defparameter *rmq-port* 5672)
(defparameter *rmq-user* (uiop:getenv "TEST_RMQ_DEFAULT_USER"))
(defparameter *rmq-pass* (uiop:getenv "TEST_RMQ_DEFAULT_PASS"))
(defparameter *mmop-port* 55555)

(defparameter *queue1* "START-to-TEST-NODE1")
(defparameter *queue2* "TEST-NODE1-to-TEST-NODE2")
(defparameter *queue3* "TEST-NODE2-to-TEST-NODE3")
(defparameter *queue4* "TEST-NODE3-to-TEST_NODE4")
(defparameter *queue5* "TEST-NODE4-to-END")

(define-rmq-node test-node nil 1 :source-queue *queue5* :dest-queue *queue1*)

(defun fn1 (node item)
  (declare (ignore node))
  (format nil "~a18" item))

(defun fn2 (node item)
  (declare (ignore node))
  (coerce (remove-if #'alpha-char-p (coerce item 'list)) 'string))

(defun fn3 (node item)
  (declare (ignore node))
  (format nil "~a" (* (parse-integer item) 7)))

(defun fn4 (node item)
  (declare (ignore node))
  (format nil "test ~a" item))

(define-system ()
    (:name test-node1 :fn #'fn1 :batch-size 10)
    (:name test-node2 :fn #'fn2 :batch-size 10)
  (:name test-node3 :fn #'fn3 :batch-size 10)
  (:name test-node4 :fn #'fn4 :batch-size 10))
