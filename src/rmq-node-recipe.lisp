(defpackage monomyth/rmq-node-recipe
  (:use :cl :monomyth/node-recipe)
  (:export rmq-node-recipe
           name-fail-queue))
(in-package :monomyth/rmq-node-recipe)

(defgeneric name-fail-queue (recipe)
  (:documentation "uses the node type to name the fail queue in a deterministic manor"))

(defclass rmq-node-recipe (node-recipe)
  () (:documentation "base recipe for rmq-nodes"))

(defmethod name-fail-queue ((recipe rmq-node-recipe))
  (format nil "~a-fail" (node-recipe/type recipe)))
