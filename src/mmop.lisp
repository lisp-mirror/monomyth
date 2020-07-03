(defpackage monomyth/mmop
  (:use :cl :rutils.bind)
  (:export *mmop-v0*
           *mmop-verions*
           mmop-error
           mmop-error/version
           mmop-error/message
           send-msg-frames
           create-frames
           send-msg
           pull-msg))
(in-package :monomyth/mmop)

(defparameter *mmop-v0* "MMOP/0")
(defparameter *mmop-verions* `(,*mmop-v0*))

(define-condition mmop-error (error)
  ((message :initarg :message
            :initform (error "mmop-error message must be set")
            :reader mmop-error/message)
   (mmop-version :initarg :version
                 :initform "undefined"
                 :reader mmop-error/version))
  (:report (lambda (con stream)
             (format stream "MMOP error (version ~a): ~a~%"
                     (mmop-error/version con)
                     (mmop-error/message con))))
  (:documentation "an error that happens in the mmop protocol"))

(defmacro handle-libzmq-error (version &body body)
  "wraps a zmq call in an error handler"
  `(handler-case ,@body
     (pzmq:libzmq-error (e) (declare (ignore e))
       (error 'mmop-error
              :version ,version
              :message (format nil "zmq error: ~a" (pzmq:strerror))))
     (:no-error (res) res)))

(defun send-msg-frames (socket version frames)
  "Helper function that sends a set of frames as single message"
  (handle-libzmq-error version
    (let ((len (length frames)))
      (iter:iterate
        (iter:for frame in frames)
        (iter:for i upfrom 1)
        (typecase frame
          (string (pzmq:send socket frame :sndmore (/= len i)))
          ((vector (unsigned-byte 8))
           (cffi-sys:with-pointer-to-vector-data (ptr (subseq frame 0))
             (pzmq:send socket ptr :len (length frame) :sndmore (/= len i))))
          (t (error 'mmop-error :version version
                    :message "unrecognized frame type")))))))

(defgeneric create-frames (message)
  (:documentation "takes a message struct and builds a list of zmq frames to be sent"))

(defun send-msg (socket version msg)
  "translates a message struct into frames, and then sends them"
  (send-msg-frames socket version (create-frames msg)))

(defun pull-msg (socket)
  "Pulls down all message frames"
  (handler-case
      (iter:iterate
        (iter:for (frame nxt) = (multiple-value-list
                                 (pzmq:recv-string socket)))
        (iter:collect frame)
        (iter:while nxt))
    (pzmq:eagain (e) (declare (ignore e))
      (error 'mmop-error
             :message "no messages to pull"))
    (pzmq:libzmq-error (e) (declare (ignore e))
      (error 'mmop-error
             :message (format nil "could not pull messages: ~a" (pzmq:strerror))))
    (:no-error (res) res)))
