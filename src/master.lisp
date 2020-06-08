(defpackage monomyth/master
  (:use :cl :monomyth/node :lfarm))
(in-package :monomyth/master)

(defgeneric start-worker (worker)
  (:documentation "starts the worker processes, by the time this method is done
it should be okay start a node"))

(defgeneric start-node (worker node)
  (:documentation "runs the node every interval
the interval is based on the start time, if the node takes too long
the next interval is started immediately
uses universal time"))

(defclass worker ()
  ((address :reader worker/address
            :initarg :address
            :initform (error "worker address must be set")
            :documentation "ip address of the worker machine")
   (kernal :reader worker/kernal
           :initarg :kernal
           :initform (error "worker kernel must be set")
           :documentation "the lfarm kernal for that machine")
   (threads :reader worker/threads
            :initarg :threads
            :initform (error "worker thread count must be set")
            :documentation "the number of threads (clients) to start")
   (nodes :reader worker/nodes
          :initform (make-hash-table :test #'string=)
          :documentation "a hash table of node names to nodes")
   (interval :reader worker/interval
             :initarg :interval
             :initform 10
             :documentation "the minimum time between node cycles
defaults to 10"))
  (:documentation "defines a single machine with its own threads, nodes, and connections"))

(defstruct (master (:constructor build-master ()))
  "the master system only has two fields, a map of worker ips to workers
and a map of node type symbols to node recipes"
  (workers (make-hash-table :test #'string=) :read-only t)
  (recipes (make-hash-table) :read-only t))

(deftask run-node (worker node)
  (iter:iterate
    (let ((goal (+ (worker/interval worker) (get-universal-time))))
      (run-iteration node)
      (let ((end (get-universal-time)))
        (when (> goal end))
        (sleep (- goal end))))))

(defmethod start-worker ((worker worker))
  )

(defmethod start-node ((worker worker) (node node))
  (let ((chan (make-channel)))
    (submit-task chan #'run-node worker node)
    (receive-result chan)))
