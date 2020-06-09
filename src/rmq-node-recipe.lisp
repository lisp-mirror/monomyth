(defpackage monomyth/rmq-node-recipe
  (:use :cl :monomyth/node-recipe))
(in-package :monomyth/rmq-node-recipe)

(defgeneric name-fail-queue (recipe)
  (:documentation ""))

(defclass rmq-node-recipe (node-recipe)
  ((source-queue :initarg :source
                 :initform (error "source queue must be set")
                 :reader source-queue)
   (dest-queue :initarg :dest
               :initform (error "destination queue must be set")
               :reader dest-queue))
  (:documentation "recipe with the added fields needed to build an rmq-node"))

(defmethod name-fail-queue ((recipe rmq-node-recipe))
  (format nil "~a-fail" (node-recipe/type recipe)))
