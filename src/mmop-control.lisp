(defpackage monomyth/mmop-control
  (:nicknames :mmop-c)
  (:use :cl :rutils.bind :monomyth/mmop)
  (:export pull-control-message
           sent-mmop
           ping-v0
           recipe-info-v0
           worker-info-v0
           stop-worker-request-v0
           start-node-request-success-v0
           start-node-request-failure-v0
           received-mmop
           pong-v0
           stop-worker-request-success-v0
           stop-worker-request-failure-v0
           json-info-response-v0
           start-node-request-v0))
(in-package :monomyth/mmop-control)

(adt:defdata sent-mmop
  ping-v0
  recipe-info-v0
  worker-info-v0
  ;; recipe-type
  (start-node-request-v0 string)
  ;; worker-id
  (stop-worker-request-v0 string))

(adt:defdata received-mmop
  pong-v0
  ;; json-response
  (json-info-response-v0 string)
  start-node-request-success-v0
  ;; error-message status-code
  (start-node-request-failure-v0 string integer)
  stop-worker-request-success-v0
  ;; error-message status-code
  (stop-worker-request-failure-v0 string integer))

(defmethod create-frames ((message ping-v0))
  `(,*mmop-v0* "PING"))

(defmethod create-frames ((message recipe-info-v0))
  `(,*mmop-v0* "RECIPE-INFO"))

(defmethod create-frames ((message worker-info-v0))
  `(,*mmop-v0* "WORKER-INFO"))

(defmethod create-frames ((message start-node-request-v0))
  `(,*mmop-v0* "START-NODE-REQUEST" ,(start-node-request-v0%0 message)))

(defmethod create-frames ((message stop-worker-request-v0))
  `(,*mmop-v0* "STOP-WORKER-REQUEST" ,(stop-worker-request-v0%0 message)))

(defun pull-control-message (socket)
  "pulls down a message designed for the control server and attempts to translate it
into the correct adt"
  (with (((version &rest args) (pull-msg socket)))
    (unless (member version *mmop-versions* :test 'string=)
      (error 'mmop-error :message
             (format nil "unrecognized mmop version: ~a" version)))

    (rutil:switch (version :test #'string=)
      (*mmop-v0* (translate-v0 args)))))

(defun translate-v0 (args)
  "attempts to translate the arg frames into MMOP/0 adts"
  (let ((res (optima:match args
               ((list "PONG") pong-v0)
               ((list "JSON-INF0-RESPONSE" json) (json-info-response-v0 json))
               ((list "START-NODE-REQUEST-SUCCESS") start-node-request-success-v0)
               ((list "START-NODE-REQUEST-FAILURE" error-message status-code)
                (start-node-request-failure-v0 error-message (parse-integer status-code)))
               ((list "STOP-WORKER-REQUEST-SUCCESS") stop-worker-request-success-v0)
               ((list "STOP-WORKER-REQUEST-FAILURE" error-message status-code)
                (stop-worker-request-failure-v0 error-message (parse-integer status-code))))))

    (if res res
        (error 'mmop-error :version *mmop-v0* :message "unknown mmop command"))))
