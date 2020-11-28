(defpackage monomyth/node
  (:use :cl :uuid :stmx)
  (:shadow :closer-mop)
  (:export startup
           pull-items
           transform-items
           transform-fn
           place-items
           handle-failure
           run-iteration
           shutdown
           node
           node/node-name
           node/batch-size
           node/type
           node/place-destination
           node-error
           node-error/step
           node-error/items))
(in-package :monomyth/node)

(defgeneric startup (node &optional build-worker-thread)
  (:documentation "performs any initial start up to ensure the node is working as corrected.
The build worker thread option exists for testing purposes"))

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
takes the entire payload sent by transorm
assumes that the items are passed under :items
should return a plist with one of the slots as :success"))

(defgeneric handle-failure (node step result)
  (:documentation "actions to take if the returned plist contains :success false
expects an explanation under :error in the result
the step can be :pull, :transform, or :place
the result is the full payload sent by the last step"))

(defgeneric shutdown (node)
  (:documentation "graceful shutdown of the node"))

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
       (place-destination
        :reader node/place-destination
        :initarg :place-destination
        :transactional nil
        :initform t
        :documentation "whether or not to run the place-items method")
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

(defmethod transform-items ((node node) pulled)
  (handler-case
      (iter:iterate
        (iter:for item in pulled)
        (iter:collect (transform-fn item)))
    (error (c)
      (error 'node-error :step :transform :items pulled
             :message (format nil "~a" c)))
    (:no-error (res) res)))

(defun run-iteration (node)
  "runs an entire operation start to finish"
  (handler-case
      (place-items node (transform-items node (pull-items node)))
    (node-error (c)
      (let ((step (node-error/step c))
            (msg (node-error/message c)))
        (v:error :node.event-loop "unexpected node error in ~a: ~a" step msg)
        (handle-failure node step (node-error/items c))))))

(defmethod startup :after ((node node) &optional (build-worker-thread t))
  (when build-worker-thread
    (v:info :node "starting thread for ~a" (node/node-name node))
    (bt:make-thread
     #'(lambda ()
         (iter:iterate
           (iter:while (node/running node))
           (run-iteration node)
           (sleep .1)))
     :name (format nil "~a-thread" (node/node-name node)))))

(defmethod shutdown :before ((node node))
  (atomic (setf (node/running node) nil)))
