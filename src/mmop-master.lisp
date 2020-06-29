(defpackage monomyth/mmop-master
  (:nicknames :mmop-m)
  (:use :cl :rutils.bind :monomyth/mmop)
  (:export pull-master-message
           worker-ready-v0
           worker-ready-v0-client-id))
(in-package :monomyth/mmop-master)

(defstruct (worker-ready-v0 (:constructor make-worker-ready-v0 (client-id)))
  (client-id (error "client id must be set") :read-only t))

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
