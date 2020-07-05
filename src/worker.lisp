(defpackage monomyth/worker
  (:use :cl :uuid :monomyth/mmop :monomyth/mmop-worker :monomyth/node-recipe
        :monomyth/node)
  (:export worker
           worker/name
           start-worker
           run-worker
           build-node
           stop-worker))
(in-package :monomyth/worker)

(defgeneric start-worker (worker master-address)
  (:documentation "starts the worker processes, by the time this method is done
it should be okay start a node"))

(defgeneric build-node (worker recipe)
  (:documentation "uses a worker and recipe to build a new node"))

(defgeneric stop-worker (worker)
  (:documentation "stops the worker processes and frees all resources and connections"))

(defclass worker ()
  ((context :reader worker/context
            :initform (pzmq:ctx-new)
            :documentation "the zmq context used for the MMOP")
   (socket :accessor worker/socket
           :documentation "the zmq dealer socket used for the MMOP")
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
  (vom:info "starting worker ~a" (worker/name worker))
  (setf (worker/socket worker) (pzmq:socket (worker/context worker) :dealer))
  (pzmq:setsockopt (worker/socket worker) :identity (worker/name worker))
  (pzmq:connect (worker/socket worker) master-address)
  (send-msg (worker/socket worker) (worker/mmop-version worker) (make-worker-ready-v0)))

(defun run-worker (worker)
  "main event loop for the worker"
  (iter:iterate
    (iter:for msg = (pull-worker-message (worker/socket worker)))
    (trivia:match msg
      ((shutdown-worker-v0) (iter:finish))

      ((start-node-v0 type recipe)
       (handler-case (build-node worker recipe)
         (end-of-file (e)
           (declare (ignore e))
           (send-msg (worker/socket worker) (worker/mmop-version worker)
                     (mmop-w:make-start-node-failure-v0
                      type "function read" "end of file (mismatched forms)")))
         (sb-pcl::no-applicable-method-error (e)
           (declare (ignore e))
           (send-msg (worker/socket worker) (worker/mmop-version worker)
                     (mmop-w:make-start-node-failure-v0
                      type "recipe build" "worker cannot handle recipe type")))
         (:no-error (res)
           (startup res)
           (let ((name (node/node-name res)))
             (setf (gethash name (worker/nodes worker)) res)
             (send-msg (worker/socket worker) (worker/mmop-version worker)
                       (mmop-w:make-start-node-success-v0 type)))))))))

(defmethod stop-worker ((worker worker))
  (vom:info "stopping worker ~a" (worker/name worker))
  (iter:iterate
    (iter:for (name node) in-hashtable (worker/nodes worker))
    (vom:info "shutting down node ~a" name)
    (shutdown node))
  (pzmq:close (worker/socket worker))
  (pzmq:ctx-destroy (worker/context worker)))