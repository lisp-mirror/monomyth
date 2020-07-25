(defpackage monomyth/processing-tests/utils
  (:use :cl)
  (:export *mmop-port*
           *rmq-host*))
(in-package :monomyth/processing-tests/utils)

(defparameter *rmq-host* (uiop:getenv "TEST_PROCESSING_RMQ"))
(defparameter *mmop-port* 55555)
