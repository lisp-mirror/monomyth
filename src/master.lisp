(defpackage monomyth/master
  (:use :cl :monomyth/node :lfarm))
(in-package :monomyth/master)

(defparameter *starting-port* 60000)

(defgeneric start-worker (worker)
  (:documentation "starts the worker processes, by the time this method is done
it should be okay start a node"))

(defgeneric name-worker (worker)
  (:documentation "creates the name that lfarm uses internally for the worker's kernel"))

(defgeneric build-node (worker recipe)
  (:documentation "constructs a node from a recipe"))

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
   (kernal :accessor worker/kernal
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

(defun build-worker (address threads &optional interval)
  "build-worker creates a worker object, the optional interval defaults to 10 seconds"
  (let ((args `(worker :address ,address :threads ,threads)))
    (if interval (setf args (append args `(:interval ,interval))))
    (apply #'make-instance args)))

(defstruct (master (:constructor build-master ()))
  "the master system only has two fields, a map of worker ips to workers
and a map of node type symbols to node recipes"
  (workers (make-hash-table :test #'string=) :read-only t)
  (recipes (make-hash-table) :read-only t))

(deftask run-node (node) (run-iteration node))

(defmethod start-worker ((worker worker))
  (let* ((address (worker/address worker))
         (threads
           (iter:iterate
             (iter:for port from *starting-port* to
                       (+ *starting-port* (worker/threads worker) -1))
             (vom:info "starting worker server on ~a:~a" address port)
             (lfarm-server:start-server address port :background t)
             (iter:collect `(,address ,port)))))
    (vom:info "starting kernel for worker at ~a" address)
    (setf (worker/kernal worker) (make-kernel threads :name (name-worker worker)))))

(defmethod name-worker ((worker worker))
  (format nil "lfarm-client-~a" (worker/address worker)))

(defmethod start-node ((worker worker) (node node))
  (iter:iterate
    (let ((*kernel* (worker/kernal worker))
          (chan (make-channel))
          (goal (+ (worker/interval worker) (get-universal-time))))
      (submit-task chan #'run-node worker node)
      (receive-result chan)
      (let ((end (get-universal-time)))
        (when (> goal end) (sleep (- goal end)))))))
