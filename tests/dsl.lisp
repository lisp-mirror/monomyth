(defpackage monomyth/tests/dsl
  (:use :cl :rove :monomyth/dsl :monomyth/node :monomyth/rmq-node :monomyth/tests/utils
        :monomyth/node-recipe :monomyth/rmq-worker :monomyth/worker)
  (:shadow :closer-mop))
(in-package :monomyth/tests/dsl)

(v:output-here *terminal-io*)
(defvar *test-context*)

(setup
  (setf *test-context* (pzmq:ctx-new)))

(teardown
  (pzmq:ctx-destroy *test-context*))

(define-system tst1 (:pull-first nil)
  (:name dsl-pull-test :fn nil :batch-size 1))

(deftest pull-first-key
  (let* ((worker (build-rmq-worker :host *rmq-host* :port *rmq-port* :username *rmq-user*
                                   :password *rmq-pass*))
         (tnode (build-node worker (build-dsl-pull-test-recipe))))
    (ng (node/pull-source tnode))
    (ng (rmq-node/source-queue tnode))
    (ng (pull-items tnode))
    (ok (build-stub-items tnode))))

(define-system tst2 (:place-last nil)
  (:name dsl-place-test :fn nil :batch-size 1))

(deftest place-past-key
  (let* ((worker (build-rmq-worker :host *rmq-host* :port *rmq-port* :username *rmq-user*
                                   :password *rmq-pass*))
         (tnode (build-node worker (build-dsl-place-test-recipe))))
    (ng (node/place-destination tnode))
    (ng (rmq-node/dest-queue tnode))))

(defvar rmq-test-var)
(define-rmq-node fn-node-test nil 1
  :start-fn #'(lambda () (setf rmq-test-var t))
  :stop-fn #'(lambda () (setf rmq-test-var nil)))

(deftest fn-rmq-node
  (let ((tnode (build-fn-node-test (format nil "testnode-~a" (get-universal-time))
                                   nil :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
    (start-node tnode *test-context* nil nil)
    (ok rmq-test-var)
    (stop-node tnode)
    (ng rmq-test-var)))

(defvar system-test-var)
(define-system tst3 ()
  (:name fn-system-test :batch-size 1
        :start-fn #'(lambda () (setf system-test-var t))
        :stop-fn #'(lambda () (setf system-test-var nil))))

(deftest fn-system
  (let ((tnode (build-fn-system-test (format nil "testnode-~a" (get-universal-time))
                                     nil :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
    (start-node tnode *test-context* nil nil)
    (ok system-test-var)
    (stop-node tnode)
    (ng system-test-var)))

(define-system tst4 ()
  (:name dep-test1 :batch-size 1)
  (:name dep-test2 :batch-size 1))

(deftest dependents-passed-to-recipe
  (ok (equal '(:dep-test2) (node-recipe/dependent-nodes (build-dep-test1-recipe)))))
