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

(defparameter *queue1* (format nil "processing-queue-1-~a" (get-universal-time)))
(defparameter *queue2* (format nil "processing-queue-2-~a" (get-universal-time)))
(defparameter *queue3* (format nil "processing-queue-3-~a" (get-universal-time)))
(defparameter *queue4* (format nil "processing-queue-4-~a" (get-universal-time)))
(defparameter *queue5* (format nil "processing-queue-5-~a" (get-universal-time)))

(define-rmq-node test-node nil *queue1* *queue2* 10)

(defmethod fn1 (item)
  (format nil "~a18" item))

(define-rmq-node test-node1 #'fn1 *queue1* *queue2* 10)

(defmethod fn2 (item)
  (coerce (remove-if #'alpha-char-p (coerce item 'list)) 'string))

(define-rmq-node test-node2 #'fn2 *queue2* *queue3* 10)

(defmethod fn3 (item)
  (format nil "~a" (* (parse-integer item) 7)))

(define-rmq-node test-node3 #'fn3 *queue3* *queue4* 10)

(defmethod fn4 (item)
  (format nil "test ~a" item))

(define-rmq-node test-node4 #'fn4 *queue4* *queue5* 10)
