(defpackage monomyth/mmop-worker
  (:nicknames :mmop-w)
  (:use :cl :rutils.bind :monomyth/mmop :monomyth/node-recipe)
  (:export pull-worker-message
           make-worker-ready-v0
           start-node-v0
           start-node-v0-type
           start-node-v0-recipe
           make-start-node-success-v0
           make-start-node-failure-v0
           shutdown-worker-v0))
(in-package :monomyth/mmop-worker)

(defstruct (worker-ready-v0 (:constructor make-worker-ready-v0 ()))
  "MMOP/0 worker-ready")
(defmethod create-frames ((message worker-ready-v0))
  `(,*mmop-v0* "READY"))

(defstruct (start-node-v0 (:constructor make-start-node-v0 (type recipe)))
  "MMOP/0 start-node (note, the recipe should be deserialized)"
  (type (error "type must be set") :read-only t)
  (recipe (error "recipe must be set") :read-only t))

(defstruct (start-node-success-v0
            (:constructor make-start-node-success-v0 (type)))
  "MMOP/0 start-node-success"
  (type (error "type must be set") :read-only t))
(defmethod create-frames ((message start-node-success-v0))
  `(,*mmop-v0* "START-NODE-SUCCESS" ,(start-node-success-v0-type message)))

(defstruct (start-node-failure-v0
            (:constructor make-start-node-failure-v0 (type reason-cat reason-msg)))
  "MMOP/0 start-node-failure"
  (type (error "type must be set") :read-only t)
  (reason-cat (error "reason-cat must be set") :read-only t)
  (reason-msg (error "reason-msg must be set") :read-only t))
(defmethod create-frames ((message start-node-failure-v0))
  `(,*mmop-v0* "START-NODE-FAILURE" ,(start-node-failure-v0-type message)
               ,(start-node-failure-v0-reason-cat message)
               ,(start-node-failure-v0-reason-msg message)))

(defstruct (shutdown-worker-v0 (:constructor make-shutdown-worker-v0 ()))
  "MMOP/0 stop-worker")

(defun pull-worker-message (socket)
  "pulls down a message designed for the worker dealer socket and attempts to
translate it into an equivalent struct"
  (with (((version &rest args) (pull-msg socket)))
    (unless (member version *mmop-verions* :test 'string=)
      (error 'mmop-error :message
             (format nil "unrecognized mmop version: ~a" version)))

    (rutil:switch (version :test #'string=)
      (*mmop-v0* (translate-v0 args)))))

(defun translate-v0 (args)
  "attempts to translate the arg frames into MMOP/0 structs"
  (let ((res (trivia:match args
               ((list "START-NODE" node-type recipe)
                (make-start-node-v0 node-type (deserialize-recipe
                                               (babel:string-to-octets recipe))))
               ((list "SHUTDOWN") (make-shutdown-worker-v0)))))

    (if res res
        (error 'mmop-error :version *mmop-v0* :message "unknown mmop command"))))
