(defpackage monomyth/node
  (:use :cl :uuid)
  (:export startup
           pull-items
           transform-items
           place-items
           handle-failure
           run-iteration
           shutdown
           node
           node/node-name
           node/trans-fn
           node/batch-size
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

(defgeneric run-iteration (node)
  (:documentation "runs an entire operation start to finish"))

(defgeneric shutdown (node)
  (:documentation "graceful shutdown of the node"))

(defclass node ()
  ((name :reader node/node-name
         :initarg :name
         :initform (format nil "node-~a" (make-v4-uuid))
         :documentation "name of the node")
   (type :reader node/type
         :initarg :type
         :initform (error "node type must be set")
         :documentation "the node type corresponds to the node recipe type")
   (batch-size :reader node/batch-size
               :initarg :batch-size
               :initform 1
               :documentation "number of items to pull in pull-items at a time")
   (transform-fn :reader node/trans-fn
                 :initarg :transform-fn
                 :initform (error "transform function is required")
                 :documentation "transforms the pulled items
takes the entire payload returned by pull-items
should return a plist with one of the slots as :success and the new items under :items")
   (worker-thread :accessor node/worker-thread
                  :initform nil
                  :documentation "thread the iteration loop runs on"))
  (:documentation "base node class for the monomyth flow system"))

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
        (iter:collect (funcall (node/trans-fn node) item)))
    (error (c)
      (error 'node-error :step :transform :items pulled
             :message (format nil "~a" c)))
    (:no-error (res) res)))

(defmethod run-iteration ((node node))
  (handler-case
      (place-items node (transform-items node (pull-items node)))
    (node-error (c)
      (let ((step (node-error/step c))
            (msg (node-error/message c)))
        (vom:error "unexpected node error in ~a: ~a" step msg)
        (handle-failure node step (node-error/items c))))))

(defmethod startup :after ((node node) &optional (build-worker-thread t))
  (when build-worker-thread
    (setf (node/worker-thread node)
          (bt:make-thread
           #'(lambda ()
               (iter:iterate
                 (run-iteration node)
                 (sleep .1)))
           :name (format nil "~a-thread" (node/node-name node))))))

(defmethod shutdown :after ((node node))
  (when (node/worker-thread node) (bt:destroy-thread (node/worker-thread node))))