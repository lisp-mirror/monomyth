(defpackage monomyth
  (:use :cl))
(in-package :monomyth)

(defun start-flow (servers)
  "Accepts a list of server ip/port pairs and starts the lfarm kernal"
  (vom:info "starting flow kernal")
  (setf lfarm:*kernel* (lfarm:make-kernel servers)))

(defparameter *starting-port* 60000)

(defun start-local ()
  "sets up a local set servers on one less than the number of logical cores"
  (let ((server-count (- (cpus:get-number-of-processors) 2))
        (servers))
    (iter:iterate
      (iter:for port from *starting-port* to (+ *starting-port* server-count))
      (vom:info "starting server on port ~d" port)
      (lfarm-server:start-server "127.0.0.1" port :background t)
      (push (list "127.0.0.1" port) servers))
    (start-flow servers)))

(defun stop-local ()
  (let ((server-count (- (cpus:get-number-of-processors) 2)))
    (iter:iterate
      (iter:for port from *starting-port* to (+ *starting-port* server-count))
      (vom:info "stoping server on port ~d" port)
      (lfarm-admin:end-server "127.0.0.1" port))))
