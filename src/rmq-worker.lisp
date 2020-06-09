(defpackage monomyth/rmq-worker
  (:use :cl :monomyth/master :monomyth/rmq-node :lfarm))
(in-package :monomyth/rmq-worker)

(defclass rmq-worker (worker)
  ((conn :initform (error "connection must be supplied")
         :accessor rmq-worker/conn
         :documentation "the rmq connection for the machine")
   (host :initform (error "the worker's rmq host must be set")
         :reader rmq-worker/host
         :initarg :host
         :documentation "queue host")
   (port :initform (error "the worker's rmq port must be set")
         :reader rmq-worker/port
         :initarg :port
         :documentation "queue port")
   (username :initform (error "the worker's rmq username must be set")
             :reader rmq-worker/username
             :initarg :username
             :documentation "queue username")
   (password :initform (error "the worker's rmq password must be set")
             :reader rmq-worker/password
             :initarg :password
             :documentation "queue password"))
  (:documentation "a worker designed to work directly with rabbit mq nodes"))

(defmethod start-worker :after ((worker rmq-worker))
  (let ((*kernel* (worker/kernal worker)))
    (set (rmq-worker/conn worker)
         (setup-connection (rmq-worker/host worker)
                           (rmq-worker/port worker)
                           (rmq-worker/username worker)
                           (rmq-worker/password worker)))))
