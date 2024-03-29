(defpackage monomyth/node-recipe
  (:use :cl :uuid)
  (:export node-recipe
           node-recipe/type
           node-recipe/dependent-nodes
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
   (dependent-nodes
    :reader node-recipe/dependent-nodes
    :initarg :dependent-nodes
    :initform nil
    :documentation "all nodes that use the output of nodes created by this recipe"))
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
