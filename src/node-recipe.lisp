(defpackage monomyth/node-recipe
  (:use :cl :uuid)
  (:export node-recipe
           node-recipe/batch-size
           node-recipe/type
           name-node
           serialize-recipe
           deserialize-recipe))
(in-package :monomyth/node-recipe)

(defgeneric name-node (recipe)
  (:documentation "takes a recipe and produces a unique node name"))

(defclass node-recipe ()
  ((type :reader node-recipe/type
         :initarg :type
         :initform (error "recipe type must be set")
         :documentation "node 'type', often used to name nodes")
   (batch-size :reader node-recipe/batch-size
               :initform 10
               :initarg :batch-size
               :documentation "the batch size that is passed directly to the node
if not set uses the default (10)"))
  (:documentation "everything the systems needs to make a new node"))

(defmethod name-node ((recipe node-recipe))
  (format nil "~a:~a" (node-recipe/type recipe) (make-v4-uuid)))

(defun serialize-recipe (recipe)
  "turns the recipe into a buffer acceptable to ZMQ"
  (flex:with-output-to-sequence (strm)
    (cl-store:store recipe strm)))

(defun deserialize-recipe (buffer)
  "turns a serialized buffer back into a recipe"
  (flex:with-input-from-sequence (in buffer)
    (cl-store:restore in)))
