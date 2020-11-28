(defpackage monomyth/rmq-worker
  (:use :cl :monomyth/worker :monomyth/rmq-node :monomyth/rmq-node-recipe
   :monomyth/node-recipe :monomyth/node :cl-rabbit)
  (:export rmq-worker
           rmq-worker/host
           rmq-worker/port
           rmq-worker/username
           rmq-worker/password
           build-rmq-worker))
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
  (make-instance 'rmq-worker :host host :port port :username username
                             :password password))
