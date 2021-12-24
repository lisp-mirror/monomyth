(defpackage monomyth/map-reduce/store
  (:use :cl :monomyth/map-reduce/db :jonathan)
  (:import-from :alexandria :doplist)
  (:export start-db-fn
           stop-db-fn
           store-counts))
(in-package :monomyth/map-reduce/store)

(defvar *db-connection* nil)

(defun start-db-fn ()
  (setf *db-connection* (connect-to-db)))

(defun stop-db-fn ()
  (setf *db-connection* (dbi:disconnect *db-connection*)))

(defun store-counts (node item)
  "parses the json payload and stores all of the counts"
  (declare (ignore node))
  (let ((payload (parse item)))
    (doplist (word count payload)
      (add-word-counts *db-connection* (string word) count))))
