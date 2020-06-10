(defpackage monomyth/node-recipe
  (:use :cl :uuid)
  (:export node-recipe
           node-recipe/type
           node-recipe/transform-fn
           node-recipe/batch-size
           name-node))
(in-package :monomyth/node-recipe)

(defgeneric name-node (recipe)
  (:documentation "takes a recipe and produces a unique node name"))

(defclass node-recipe ()
  ((type :reader node-recipe/type
         :initarg :type
         :initform (error "recipe type must be set")
         :documentation "node 'type', often used to name nodes")
   (transform-fn :reader node-recipe/transform-fn
                 :initarg :transform-fn
                 :initform (error "recipe transform function must be set")
                 :documentation "the function that is passed directly to the node")
   (batch-size :reader node-recipe/batch-size
               :initarg :batch-size
               :documentation "the batch size that is passed directly to the node
if not set uses the default"))
  (:documentation "everything the systems needs to make a new node"))

(defmethod name-node ((recipe node-recipe))
  (format nil "~a:~a" (node-recipe/type recipe) (make-v4-uuid)))
