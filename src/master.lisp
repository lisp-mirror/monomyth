(defpackage monomyth/master
  (:use :cl :monomyth/node :lfarm))
(in-package :monomyth/master)

(defstruct (master (:constructor build-master ()))
  "the master system only has two fields, a map of worker ips to workers
and a map of node type symbols to node recipes"
  (workers (make-hash-table :test #'string=) :read-only t)
  (recipes (make-hash-table) :read-only t))

(defun build-worker (node-class address threads &rest extra-args &key interval)
  "build-worker creates a worker object, the optional interval defaults to 10 seconds"
  (let ((args `(,node-class :address ,address :threads ,threads)))
    (if interval (setf args (append args `(:interval ,interval))))
    (apply #'make-instance (append args extra-args))))
