(defpackage monomyth/node
  (:use :cl :uuid :stmx :monomyth/mmop :monomyth/mmop-node)
  (:shadow :closer-mop)
  (:export *stub-message*
           startup
           build-stub-item
           pull-items
           transform-items
           transform-fn
           place-items
           handle-failure
           run-iteration
           shutdown
           complete-task
           node
           node/node-name
           node/batch-size
           node/type
           node/place-destination
           node/pull-source
           node-error
           node-error/step
           node-error/items
           task-complete
           task-complete/node-name))
(in-package :monomyth/node)

(defparameter *stub-message* "STUB-ITEM")

(defgeneric startup (node context worker-address &optional build-worker-thread)
  (:documentation "performs any initial start up to ensure the node is working as corrected.
The context is used to make socket connections.
The build worker thread option exists for testing purposes."))

(defgeneric pull-items (node)
  (:documentation "tells the node to pull count items from the message bus
should return a plist with one of the slots as :success"))

(defgeneric transform-items (node pulled)
  (:documentation "extracts the items from the pull step (provided it was successful)
and applies the transform function to each one"))

(defgeneric transform-fn (node item)
  (:documentation "the core transform function"))

(defgeneric place-items (node result)
  (:documentation "places the finished items on the message bus
takes the entire payload sent by transform
assumes that the items are passed under :items
should return a plist with one of the slots as :success"))

(defgeneric handle-failure (node step result)
  (:documentation "actions to take if the returned plist contains :success false
expects an explanation under :error in the result
the step can be :pull, :transform, or :place
the result is the full payload sent by the last step"))

(defgeneric shutdown (node)
  (:documentation "Graceful shutdown of the node.
Cannot be called within the node as it kills the thread, assumes that
the thread name is the node name."))

(defgeneric complete-task (node)
  (:documentation "The node signals that it has completed its task and then
stops the thread."))

(transactional
    (defclass node ()
      ((name :reader node/node-name
             :initarg :name
             :transactional nil
             :initform (format nil "node-~a" (make-v4-uuid))
             :documentation "name of the node")
       (type :reader node/type
             :transactional nil
             :initarg :type
             :initform (error "node type must be set")
             :documentation "the node type corresponds to the node recipe type")
       (batch-size :reader node/batch-size
                   :initarg :batch-size
                   :transactional nil
                   :initform 10
                   :documentation "number of items to pull in pull-items at a time")
       (socket :accessor node/socket
               :documentation "ZMQ socket to allow for communication to the worker thread.")
       (place-destination
        :reader node/place-destination
        :initarg :place-destination
        :transactional nil
        :initform t
        :documentation "whether or not to run the place-items method")
       (pull-source
        :reader node/pull-source
        :initarg :pull-source
        :transactional nil
        :initform t
        :documentation "whether or not to run the pull-items method")
       (running :accessor node/running
                :initform t
                :documentation "transactional condition that allows for safe shutdown"))
      (:documentation "base node class for the monomyth flow system")))

(define-condition node-error (error)
  ((step :reader node-error/step
         :initarg :step
         :initform (error "node error step must be set")
         :documentation "the step the node failed on
should be :place, :transform, or :pull if handle failure will take it")
   (message :reader node-error/message
            :initarg :message
            :initform (error "node error message must be set"))
   (items :reader node-error/items
          :initarg :items
          :initform nil
          :documentation "the items to be reprocessed"))
  (:documentation "an internal node error, handled by run-iteration")
  (:report (lambda (con stream)
             (format stream "internal node error: ~a" (node-error/message con)))))

(define-condition task-complete (condition)
  ((node-name :reader task-complete/node-name
              :initarg :name
              :initform (error "node name must be set")))
  (:documentation "signals that the bounded stream has finished")
  (:report (lambda (con stream)
             (format stream "~a completed task"
                     (task-complete/node-name con)))))

(defmethod transform-items ((node node) pulled)
  (handler-case
      (iter:iterate
        (iter:for item in pulled)
        (iter:collect (transform-fn node item)))
    (error (c)
      (error 'node-error :step :transform :items pulled
                         :message (format nil "~a" c)))
    (:no-error (res) res)))

(defgeneric build-stub-item (node)
  (:documentation "constructs a stub item message"))

(defmethod build-stub-item ((node node))
  *stub-message*)

(defun build-stub-items (node)
  "An alternative to pull-items when node/pull-source is nil.
Produces a list of length node/batch-size filled with :stub-item keywords."
  (iter:iterate
    (iter:repeat (node/batch-size node))
    (iter:collect (build-stub-item node))))

(defun run-iteration (node)
  "runs an entire operation start to finish"
  (handler-case
      (place-items node (transform-items node (if (node/pull-source node)
                                                  (pull-items node)
                                                  (build-stub-items node))))
    (node-error (c)
      (let ((step (node-error/step c))
            (msg (node-error/message c)))
        (v:error :node.event-loop "unexpected node error in ~a: ~a" step msg)
        (handle-failure node step (node-error/items c))))))

(defmethod startup :after ((node node) context worker-address &optional (build-worker-thread t))
  (setf (node/socket node) (pzmq:socket context :push))
  (pzmq:setsockopt (node/socket node) :identity (node/node-name node))
  (when build-worker-thread
    ;; NOTE: This connection is not necessary if not running the worker thread
    ;; because there should never be call by the worker thread to the worker.
    (pzmq:connect (node/socket node) worker-address)
    (v:info :node "starting thread for ~a" (node/node-name node))
    (bt:make-thread
     #'(lambda ()
         (iter:iterate
           (iter:while (node/running node))
           (run-iteration node)
           (sleep .1)))
     :name (format nil (node/node-name node)))))

(defmethod shutdown :before ((node node))
  (finish-output)
  (pzmq:close (node/socket node))
  (atomic (setf (node/running node) nil)))

(defmethod shutdown :after ((node node))
  (let ((node-thread
          (car
           (remove-if-not
            #'(lambda (th) (string= (bt:thread-name th) (node/node-name node)))
            (bt:all-threads)))))
    (when node-thread (bt:destroy-thread node-thread))))

(defmethod complete-task ((node node))
  (send-msg (node/socket node) *mmop-v0*
            (node-task-completed-v0
             (string (node/type node)) (node/node-name node))))
