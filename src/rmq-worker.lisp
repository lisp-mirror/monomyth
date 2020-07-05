(defpackage monomyth/rmq-worker
  (:use :cl :monomyth/worker :monomyth/rmq-node :monomyth/rmq-node-recipe
   :monomyth/node-recipe :monomyth/node :cl-rabbit)
  (:export build-rmq-worker))
(in-package :monomyth/rmq-worker)

(defclass rmq-worker (worker)
  ((host :reader rmq-worker/host
         :initarg :host
         :initform (error "rmq host must be set")
         :documentation "rabbit mq host")
   (port :reader rmq-worker/port
         :initarg :port
         :initform (error "rmq port must be set")
         :documentation "rabbit mq port")
   (username :reader rmq-worker/username
             :initarg :username
             :initform (error "rmq username must be set")
             :documentation "rabbit mq username")
   (password :reader rmq-worker/password
             :initarg :password
             :initform (error "rmq password")))
  (:documentation "a worker designed to work directly with rabbit mq nodes"))

(defun build-rmq-worker
    (&key (host "localhost") (port 5672) (username "guest") (password "guest"))
  (make-instance 'rmq-worker :host host :port port :username username :password password))

(defmethod build-node ((worker rmq-worker) (recipe rmq-node-recipe))
  (make-rmq-node (eval (read-from-string (node-recipe/transform-fn recipe)))
                 (node-recipe/type recipe)
                 (rmq-node-recipe/source-queue recipe)
                 (rmq-node-recipe/dest-queue recipe)
                 (name-fail-queue recipe)
                 :host (rmq-worker/host worker)
                 :port (rmq-worker/port worker)
                 :username (rmq-worker/username worker)
                 :password (rmq-worker/password worker)
                 :batch-size (node-recipe/batch-size recipe)))
