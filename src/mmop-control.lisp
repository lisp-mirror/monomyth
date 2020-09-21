(defpackage monomyth/mmop-control
  (:nicknames :mmop-c)
  (:use :cl :rutils.bind :monomyth/mmop)
  (:export pull-control-message
           sent-mmop
           ping-v0
           received-mmop
           pong-v0))
(in-package :monomyth/mmop-control)

(adt:defdata sent-mmop
  ping-v0)

(adt:defdata received-mmop
  pong-v0)

(defmethod create-frames ((message ping-v0))
  `(,*mmop-v0* "PING"))

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
  (let ((res (trivia:match args
               ((list "PONG") pong-v0))))

    (if res res
        (error 'mmop-error :version *mmop-v0* :message "unknown mmop command"))))
