(defpackage monomyth/tests/dsl
  (:use :cl :rove :monomyth/dsl :monomyth/node :monomyth/rmq-node :monomyth/tests/utils)
  (:shadow :closer-mop))
(in-package :monomyth/tests/dsl)

(v:output-here *terminal-io*)
(defvar *test-context*)

(setup
  (setf *test-context* (pzmq:ctx-new)))

(teardown
  (pzmq:ctx-destroy *test-context*))

(define-system (:pull-first nil)
  (:name dsl-pull-test :fn nil :batch-size 1))

(deftest pull-first-key
  (let ((tnode (build-dsl-pull-test nil nil nil *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
    (ng (node/pull-source tnode))
    (ng (rmq-node/source-queue tnode))))

(define-system (:place-last nil)
  (:name dsl-place-test :fn nil :batch-size 1))

(deftest place-past-key
  (let ((tnode (build-dsl-place-test nil nil nil *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
    (ng (node/place-destination tnode))
    (ng (rmq-node/dest-queue tnode))))

(defvar rmq-test-var)
(define-rmq-node fn-node-test nil 1
  :start-fn #'(lambda () (setf rmq-test-var t))
  :stop-fn #'(lambda () (setf rmq-test-var nil)))

(deftest fn-rmq-node
  (let ((tnode (build-fn-node-test (format nil "testnode-~a" (get-universal-time))
                                   nil :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
    (startup tnode *test-context* nil nil)
    (ok rmq-test-var)
    (shutdown tnode)
    (ng rmq-test-var)))

(defvar system-test-var)
(define-system ()
  (:name fn-system-test :batch-size 1
        :start-fn #'(lambda () (setf system-test-var t))
        :stop-fn #'(lambda () (setf system-test-var nil))))

(deftest fn-system
  (let ((tnode (build-fn-system-test (format nil "testnode-~a" (get-universal-time))
                                     nil :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
    (startup tnode *test-context* nil nil)
    (ok system-test-var)
    (shutdown tnode)
    (ng system-test-var)))
