(defpackage monomyth/tests/dsl
  (:use :cl :rove :monomyth/dsl :monomyth/node :monomyth/rmq-node :monomyth/tests/utils)
  (:shadow :closer-mop))
(in-package :monomyth/tests/dsl)

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
