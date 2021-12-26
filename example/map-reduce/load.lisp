(defpackage monomyth/map-reduce/load
  (:use :cl :monomyth/node)
  (:export build-start-file-fn
           build-load-file-fn
           stop-file-fn))
(in-package :monomyth/map-reduce/load)

(defvar *file-handle* nil
  "global file handle to store the test file in")

(defun build-start-file-fn (file-path)
  "builds a function that opens the file-path and stores it in the *file-handle*"
  #'(lambda ()
      (setf *file-handle* (open file-path))))

(defun build-load-file-fn (lines-per-batch)
  #'(lambda (node item)
      (declare (ignore item))
      (let ((res (iter:iterate
                   (iter:repeat lines-per-batch)
                   (iter:for line in-stream *file-handle* using #'read-line)
                   (when (not line)
                     (iter:finish))
                   (iter:reducing line by #'(lambda (acc val) (str:concat acc " " val))))))
        (if res res
            (progn
              (complete-task node)
              (wait-for-finish node))))))

(defun stop-file-fn ()
  (close *file-handle*))
