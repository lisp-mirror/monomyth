(defpackage monomyth/map-reduce/map
  (:use :cl :jonathan)
  (:import-from :alexandria :hash-table-plist)
  (:export text->word-count))
(in-package :monomyth/map-reduce/map)

(defun text->word-count (node item)
  "turns the text snipped into a json blob of word counts"
  (declare (ignore node))
  (iter:iterate
    (iter:with counts = (make-hash-table :test #'equal))
    (iter:for word in (str:words item))
    (iter:for key = (intern (str:downcase word) "KEYWORD"))
    (setf (gethash key counts) (1+ (gethash key counts 0)))
    (iter:finally (return (to-json (hash-table-plist counts))))))
