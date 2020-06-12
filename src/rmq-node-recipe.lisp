(defpackage monomyth/rmq-node-recipe
  (:use :cl :monomyth/node-recipe)
  (:export rmq-node-recipe
           rmq-node-recipe/source-queue
           rmq-node-recipe/dest-queue
           build-rmq-node-recipe
           name-fail-queue))
(in-package :monomyth/rmq-node-recipe)

(defgeneric name-fail-queue (recipe)
  (:documentation "uses the node type to name the fail queue in a deterministic manor"))

(defclass rmq-node-recipe (node-recipe)
  ((source-queue :initarg :source
                 :initform (error "source queue must be set")
                 :reader rmq-node-recipe/source-queue)
   (dest-queue :initarg :dest
               :initform (error "destination queue must be set")
               :reader rmq-node-recipe/dest-queue))
  (:documentation "recipe with the added fields needed to build an rmq-node"))

(defun build-rmq-node-recipe (node-type transform-fn source-queue dest-queue
                              &optional batch-size)
  (make-instance 'rmq-node-recipe :type node-type :transform-fn transform-fn :batch-size batch-size
                 :source source-queue :dest dest-queue))

(defmethod name-fail-queue ((recipe rmq-node-recipe))
  (format nil "~a-fail" (node-recipe/type recipe)))
