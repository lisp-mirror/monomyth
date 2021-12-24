(defpackage monomyth/worker
  (:use :cl :uuid :monomyth/mmop :monomyth/mmop-worker :monomyth/node-recipe
        :monomyth/node :stmx)
  (:export worker
           worker/name
           worker/nodes
           worker/master-socket
           start-worker
           run-worker
           build-node
           stop-worker))
(in-package :monomyth/worker)

(defparameter *node-socket-address* "inproc://nodes")

(defgeneric start-worker (worker master-address)
  (:documentation "starts the worker processes, by the time this method is done
it should be okay start a node"))

(defgeneric build-node (worker recipe)
  (:documentation "uses a worker and recipe to build a new node"))

(defgeneric stop-worker (worker)
  (:documentation "stops the worker processes and frees all resources and connections"))

(defclass worker ()
  ((nodes-socket-address
    :reader worker/nodes-socket-address
    :initform (format nil "~a-~a" *node-socket-address* (make-v4-uuid))
    :documentation "stores a unique node socket address")
   (context :reader worker/context
            :initform (pzmq:ctx-new)
            :documentation "the zmq context used for the MMOP")
   (master-socket :accessor worker/master-socket
                  :documentation "the zmq dealer socket used for master related MMOP")
   (nodes-socket :accessor worker/nodes-socket
                 :documentation "the zmq pull socket used for node related MMOP")
   (name :reader worker/name
         :initform (name-worker)
         :documentation "a unique worker name set before startup")
   (mmop-version :reader worker/mmop-version
                 :initform *mmop-v0*
                 :documentation "the MMOP version the worker is using")
   (nodes :reader worker/nodes
          :initform (make-hash-table :test #'equal)
          :documentation "a hash table of node names to nodes"))
  (:documentation "defines a single machine with its own threads, nodes, and connections"))

(defun name-worker ()
  "creates the name that zmq uses for routing"
  (format nil "monomyth-worker-~a" (make-v4-uuid)))

(defmethod start-worker ((worker worker) master-address)
  (v:info :worker "starting worker ~a" (worker/name worker))
  (setf (worker/master-socket worker) (pzmq:socket (worker/context worker) :dealer)
        (worker/nodes-socket worker) (pzmq:socket (worker/context worker) :pull))
  (pzmq:setsockopt (worker/nodes-socket worker) :identity (worker/name worker))
  (pzmq:setsockopt (worker/master-socket worker) :identity (worker/name worker))
  (pzmq:bind (worker/nodes-socket worker) (worker/nodes-socket-address worker))
  (pzmq:connect (worker/master-socket worker) master-address)
  (send-msg (worker/master-socket worker) (worker/mmop-version worker) mmop-w:worker-ready-v0))

(defun run-worker (worker)
  "main event loop for the worker"
  (let ((master-socket (worker/master-socket worker))
        (nodes-socket (worker/nodes-socket worker)))
    (pzmq:with-poll-items items
        (master-socket nodes-socket)
      (iter:iterate
        (pzmq:poll items)
        (handler-case
            (progn
              (when (member :pollin (pzmq:revents items 0))
                (when (not (handle-message worker (pull-worker-message master-socket)))
                  (iter:finish)))
              (when (member :pollin (pzmq:revents items 1))
                (when (not (handle-message worker (pull-worker-message nodes-socket)))
                  (iter:finish))))
          (mmop-error (c)
            (v:error '(:worker :event-loop :mmop)
                     "could not pull MMOP message (version: ~a): ~a"
                     (mmop-error/version c) (mmop-error/message c))))))))

(defun handle-message (worker mmop-msg)
  "handles a specific message for the worker, return t if the worker should continue"
  (adt:match mmop-w:received-mmop mmop-msg
    ((shutdown-worker-v0) nil)

    ((start-node-v0 type recipe) (start-worker-node worker type recipe))

    ((node-task-completed-v0 node-type node-name)
     (complete-node-task worker node-type node-name))

    ((complete-task-v0 node-type) (complete-tasks worker node-type))))

(defun complete-node-task (worker node-type node-name)
  "when a node signals that it has completed its task it should be shutdown,
removed from the workers state, and then the master server should be notified."
  (let ((node (gethash node-name (worker/nodes worker))))
    (if node
        (progn
          (shutdown node)
          (remhash node-name (worker/nodes worker))
          (send-msg (worker/master-socket worker) *mmop-v0*
                    (worker-task-completed-v0 node-type)))
        (v:warn :worker.complete-node-task "no node ~a found to complete" node-name)))
  t)

(defun complete-tasks (worker node-type)
  "finds all nodes with the supplied node-type and sets them to complete when
there are no more items on the data stream"
  (v:debug :worker.complete-tasks "complete tasks request for ~a" node-type)
  (let ((nodes (iter:iterate
                 (iter:for (node-name node) in-hashtable (worker/nodes worker))
                 (when (string= node-type (string (node/type node)))
                   (iter:collect `(,node-name . ,node))))))
    (dolist (node nodes)
      (v:info :worker.complete-tasks "attempting to complete ~a" (car node))
      (atomic (setf (node/complete-when-ready (cdr node)) t))))
  t)

(defun start-worker-node (worker node-type recipe)
  "Attempts to start a node and communicate the result to the master server."
  (handler-case (build-node worker recipe)
    (sb-pcl::no-applicable-method-error (e)
      (declare (ignore e))
      (send-msg (worker/master-socket worker) (worker/mmop-version worker)
                (mmop-w:start-node-failure-v0
                 node-type "recipe build" "worker cannot handle recipe type")))

    (:no-error (res)
      (startup res (worker/context worker) (worker/nodes-socket-address worker))
      (let ((name (node/node-name res)))
        (setf (gethash name (worker/nodes worker)) res)
        (send-msg (worker/master-socket worker) (worker/mmop-version worker)
                  (mmop-w:start-node-success-v0 node-type)))))
  t)

(defmethod stop-worker ((worker worker))
  (v:info :worker "stopping worker ~a" (worker/name worker))
  (iter:iterate
    (iter:for (name node) in-hashtable (worker/nodes worker))
    (v:info '(:worker :node) "shutting down node ~a" name)
    (shutdown node))
  (pzmq:close (worker/master-socket worker))
  (pzmq:close (worker/nodes-socket worker))
  (pzmq:ctx-destroy (worker/context worker)))
