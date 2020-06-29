(defpackage monomyth/mmop-worker
  (:nicknames :mmop-w)
  (:use :cl :monomyth/mmop)
  (:export send-worker-message
           make-worker-ready-v0))
(in-package :monomyth/mmop-worker)

(defstruct (worker-ready-v0 (:constructor make-worker-ready-v0 ())))

(defun send-worker-message (socket message)
  "takes the dealer socket and a message struct and sends the equivalent zmq frames"
  (unless (trivia:match message
            ((worker-ready-v0)
             (progn (send-msg socket *mmop-v0* `(,*mmop-v0* "READY")) t)))

    (error 'mmop-error :message "unknown worker message type")))
