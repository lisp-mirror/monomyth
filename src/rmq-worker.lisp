(defpackage monomyth/rmq-worker
  (:use :cl :monomyth/worker :monomyth/rmq-node :monomyth/rmq-node-recipe :lfarm
        :monomyth/node-recipe :monomyth/node))
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
             :documentation "queue password")
   (chan-counter :initform 1
                 :accessor rmq-worker/chan-counter
                 :documentation "a counter to ensure that each worker has a unique channel"))
  (:documentation "a worker designed to work directly with rabbit mq nodes"))

(defmethod start-worker :after ((worker rmq-worker))
  (let ((*kernel* (worker/kernal worker)))
    (set (rmq-worker/conn worker)
         (setup-connection (rmq-worker/host worker)
                           (rmq-worker/port worker)
                           (rmq-worker/username worker)
                           (rmq-worker/password worker)))))

(defmethod build-node ((worker rmq-worker) (recipe rmq-node-recipe))
  (let* ((node-name (name-node recipe))
         (args `(,(node-recipe/transform-fn recipe)
                  ,node-name
                  ,(rmq-worker/conn worker)
                  ,(rmq-worker/chan-counter worker)
                  ,(rmq-node-recipe/source-queue recipe)
                  ,(rmq-node-recipe/dest-queue recipe)
                  ,(name-fail-queue recipe)))
         (batch-size (node-recipe/batch-size recipe)))
    (if batch-size (setf args (append args `(:batch-size ,batch-size))))
    (let ((node (apply #'make-rmq-node args)))
      (incf (rmq-worker/chan-counter worker))
      (startup node)
      (setf (gethash node-name (worker/nodes worker)) node))))

(defmethod remove-node ((worker rmq-worker) node-name)
  (shutdown (gethash node-name (worker/nodes worker)))
  (remhash node-name (worker/nodes worker)))
