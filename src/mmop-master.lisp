(defpackage monomyth/mmop-master
  (:nicknames :mmop-m)
  (:use :cl :rutils.bind :monomyth/mmop :monomyth/node-recipe)
  (:export pull-master-message
           worker-ready-v0
           worker-ready-v0-client-id
           make-start-node-v0))
(in-package :monomyth/mmop-master)

(defstruct (worker-ready-v0 (:constructor make-worker-ready-v0 (client-id)))
  "MMOP/0 worker-ready"
  (client-id (error "client id must be set") :read-only t))

(defstruct (start-node-v0 (:constructor make-start-node-v0 (client-id recipe)))
  "MMOP/0 start-node"
  (client-id (error "client id must be set") :read-only t)
  (recipe (error "recipe must be set") :read-only t))
(defmethod create-frames ((message start-node-v0))
  (let ((recipe (start-node-v0-recipe message)))
    `(,(start-node-v0-client-id message) ,*mmop-v0* "START-NODE"
      ,(symbol-name (node-recipe/type recipe))
      ,(serialize-recipe recipe))))

(defun pull-master-message (socket)
  "pulls down a message designed for the master router socket and attempts to
translate it into an equivalent struct"
  (with (((id version &rest args) (pull-msg socket)))
    (unless (member version *mmop-verions* :test 'string=)
      (error 'mmop-error :message
             (format nil "unrecognized mmop version: ~a" version)))

    (rutil:switch (version :test #'string=)
      (*mmop-v0* (translate-v0 id args)))))

(defun translate-v0 (id args)
  "attempts to translate the arg frames into MMOP/0 structs"
  (let ((res (trivia:match args
               ((list "READY") (make-worker-ready-v0 id)))))

    (if res res
        (error 'mmop-error :version *mmop-v0* :message "unknown mmop command"))))
