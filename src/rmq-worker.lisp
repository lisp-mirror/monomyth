(defpackage monomyth/rmq-worker
  (:use :cl :monomyth/worker :monomyth/rmq-node :monomyth/rmq-node-recipe
   :monomyth/node-recipe :monomyth/node :cl-rabbit)
  (:export build-rmq-worker))
(in-package :monomyth/rmq-worker)

(defclass rmq-worker (worker)
  ((conn :reader rmq-worker/conn
         :initarg :conn
         :documentation "the rmq connection for the machine")
   (chan-counter :initform 0
                 :accessor rmq-worker/chan-counter
                 :documentation "a counter to ensure that each node has a unique channel"))
  (:documentation "a worker designed to work directly with rabbit mq nodes"))

(defun build-rmq-worker
    (&key (host "localhost") (port 5672) (username "guest") (password "guest"))
  (let ((conn (setup-connection :host host :port port :username username
                                :password password)))
    (if (getf conn :success)
        (make-instance 'rmq-worker :conn (getf conn :conn))
        (error (getf conn :error)))))

(defmethod build-node ((worker rmq-worker) (recipe rmq-node-recipe))
  (make-rmq-node (eval (read-from-string (node-recipe/transform-fn recipe)))
                 (node-recipe/type recipe)
                 (rmq-worker/conn worker)
                 (incf (rmq-worker/chan-counter worker))
                 (rmq-node-recipe/source-queue recipe)
                 (rmq-node-recipe/dest-queue recipe)
                 (name-fail-queue recipe)
                 :batch-size (node-recipe/batch-size recipe)))

(defmethod stop-worker :after ((worker rmq-worker))
  (sleep .1)
  (destroy-connection (rmq-worker/conn worker)))
