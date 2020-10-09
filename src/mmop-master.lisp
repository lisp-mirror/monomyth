(defpackage monomyth/mmop-master
  (:nicknames :mmop-m)
  (:use :cl :rutils.bind :monomyth/mmop :monomyth/node-recipe)
  (:export pull-master-message
           sent-mmop
           received-mmop
           ping-v0
           recipe-info-v0
           start-node-request-v0
           stop-worker-request-v0
           pong-v0
           recipe-info-response-v0
           start-node-request-success-v0
           start-node-request-failure-v0
           stop-worker-request-success-v0
           stop-worker-request-failure-v0
           worker-ready-v0
           start-node-v0
           start-node-success-v0
           start-node-failure-v0
           shutdown-worker-v0))
(in-package :monomyth/mmop-master)

(adt:defdata sent-mmop
  ;; client-id
  (pong-v0 string)
  ;; client-id
  (start-node-request-success-v0 string)
  ;; client-id error-reason
  (start-node-request-failure-v0 string string)
  ;; client-id json-response
  (recipe-info-response-v0 string string)
  ;; client-id recipe
  (start-node-v0 string node-recipe)
  ;; client-id
  (stop-worker-request-success-v0 string)
  ;; client-id error-message status-code
  (stop-worker-request-failure-v0 string string integer)
  ;; client-id
  (shutdown-worker-v0 string))

(adt:defdata received-mmop
  ;; client-id
  (ping-v0 string)
  ;; client-id
  (recipe-info-v0 string)
  ;; client-id recipe-type
  (start-node-request-v0 string string)
  ;; client-id worker-id
  (stop-worker-request-v0 string string)
  ;; client-id
  (worker-ready-v0 string)
  ;; client-id type
  (start-node-success-v0 string string)
  ;; client-id type reason-category reason-message
  (start-node-failure-v0 string string string string))

(defmethod create-frames ((message pong-v0))
  `(,(pong-v0%0 message) ,*mmop-v0* "PONG"))

(defmethod create-frames ((message recipe-info-response-v0))
  `(,(recipe-info-response-v0%0 message) ,*mmop-v0* "RECIPE-INF0-RESPONSE"
    ,(recipe-info-response-v0%1 message)))

(defmethod create-frames ((message start-node-request-success-v0))
  `(,(start-node-request-success-v0%0 message) ,*mmop-v0* "START-NODE-REQUEST-SUCCESS"))

(defmethod create-frames ((message start-node-request-failure-v0))
  `(,(start-node-request-failure-v0%0 message) ,*mmop-v0* "START-NODE-REQUEST-FAILURE"
    ,(start-node-request-failure-v0%1 message)))

(defmethod create-frames ((message stop-worker-request-success-v0))
  `(,(stop-worker-request-success-v0%0 message) ,*mmop-v0* "STOP-WORKER-REQUEST-SUCCESS"))

(defmethod create-frames ((message stop-worker-request-failure-v0))
  `(,(stop-worker-request-failure-v0%0 message) ,*mmop-v0* "STOP-WORKER-REQUEST-FAILURE"
    ,(stop-worker-request-failure-v0%1 message)
    ,(write-to-string (stop-worker-request-failure-v0%2 message))))

(defmethod create-frames ((message start-node-v0))
  (let ((recipe (start-node-v0%1 message)))
    `(,(start-node-v0%0 message) ,*mmop-v0* "START-NODE"
      ,(symbol-name (node-recipe/type recipe))
      ,(serialize-recipe recipe))))

(defmethod create-frames ((message shutdown-worker-v0))
  `(,(shutdown-worker-v0%0 message) ,*mmop-v0* "SHUTDOWN"))

(defun pull-master-message (socket)
  "pulls down a message designed for the master router socket and attempts to
translate it into an equivalent struct"
  (with (((id version &rest args) (pull-msg socket)))
    (v:debug '(:mmop :master) "pulled message (~{~a~^, ~}) from ~a with MMOP version ~a"
             args id version)
    (unless (member version *mmop-versions* :test 'string=)
      (error 'mmop-error :message
             (format nil "unrecognized mmop version: ~a" version)))

    (rutil:switch (version :test #'string=)
      (*mmop-v0* (translate-v0 id args)))))

(defun translate-v0 (id args)
  "attempts to translate the arg frames into MMOP/0 structs"
  (let ((res (trivia:match args
               ((list "PING") (ping-v0 id))
               ((list "RECIPE-INFO") (recipe-info-v0 id))
               ((list "START-NODE-REQUEST" recipe-type)
                (start-node-request-v0 id recipe-type))
               ((list "STOP-WORKER-REQUEST" worker-id)
                (stop-worker-request-v0 id worker-id))
               ((list "READY") (worker-ready-v0 id))
               ((list "START-NODE-SUCCESS" node-type)
                (start-node-success-v0 id node-type))
               ((list "START-NODE-FAILURE" node-type cat msg)
                (start-node-failure-v0 id node-type cat msg)))))

    (if res res
        (error 'mmop-error :version *mmop-v0* :message "unknown mmop command"))))
