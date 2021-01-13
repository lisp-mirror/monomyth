(defpackage monomyth/mmop-node
  (:nicknames :mmop-n)
  (:use :cl :rutils.bind :monomyth/mmop)
  (:export sent-data
           node-task-completed-v0))
(in-package :monomyth/mmop-node)

(adt:defdata sent-data
  ;; node-type node-name
  (node-task-completed-v0 string string))

(defmethod create-frames ((message node-task-completed-v0))
  `(,*mmop-v0* "NODE-TASK-COMPLETED" ,(node-task-completed-v0%0 message)
               ,(node-task-completed-v0%1 message)))
