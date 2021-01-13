(defpackage monomyth/mmop-worker
  (:nicknames :mmop-w)
  (:use :cl :rutils.bind :monomyth/mmop :monomyth/node-recipe)
  (:export pull-worker-message
           sent-mmop
           received-mmop
           node-task-completed-v0
           worker-task-completed-v0
           worker-ready-v0
           start-node-v0
           start-node-success-v0
           start-node-failure-v0
           shutdown-worker-v0))
(in-package :monomyth/mmop-worker)

(adt:defdata sent-mmop
  worker-ready-v0
  ;; type
  (start-node-success-v0 string)
  ;; type reason-category reason-message
  (start-node-failure-v0 string string string)
  ;; worker-id node-type
  (worker-task-completed-v0 string string))

(adt:defdata received-mmop
  ;; type recipe
  (start-node-v0 string node-recipe)
  ;; node-type node-name
  (node-task-completed-v0 string string)
  shutdown-worker-v0)

(defmethod create-frames ((message worker-ready-v0))
  `(,*mmop-v0* "READY"))

(defmethod create-frames ((message start-node-success-v0))
  `(,*mmop-v0* "START-NODE-SUCCESS" ,(start-node-success-v0%0 message)))

(defmethod create-frames ((message start-node-failure-v0))
  `(,*mmop-v0* "START-NODE-FAILURE" ,(start-node-failure-v0%0 message)
               ,(start-node-failure-v0%1 message)
               ,(start-node-failure-v0%2 message)))

(defmethod create-frames ((message worker-task-completed-v0))
  `(,*mmop-v0* "WORKER-TASK-COMPLETED" ,(worker-task-completed-v0%0 message)
               ,(worker-task-completed-v0%1 message)))

(defun pull-worker-message (socket)
  "pulls down a message designed for the worker dealer socket and attempts to
translate it into an equivalent struct"
  (with (((version &rest args) (pull-msg socket)))
    (unless (member version *mmop-versions* :test 'string=)
      (error 'mmop-error :message
             (format nil "unrecognized mmop version: ~a" version)))

    (rutil:switch (version :test #'string=)
      (*mmop-v0* (translate-v0 args)))))

(defun translate-v0 (args)
  "attempts to translate the arg frames into MMOP/0 structs"
  (let ((res (trivia:match args
               ((list "START-NODE" node-type recipe)
                (start-node-v0 node-type (deserialize-recipe
                                          (babel:string-to-octets recipe))))
               ((list "SHUTDOWN") shutdown-worker-v0)
               ((list "NODE-TASK-COMPLETED" node-type node-name)
                (node-task-completed-v0 node-type node-name)))))

    (if res res
        (error 'mmop-error :version *mmop-v0* :message "unknown mmop command"))))
