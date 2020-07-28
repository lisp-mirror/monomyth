(defpackage monomyth/processing-tests/utils
  (:use :cl :monomyth/rmq-worker :monomyth/rmq-node-recipe :monomyth/rmq-node
        :monomyth/node :monomyth/worker :stmx :monomyth/node-recipe)
  (:shadow :closer-mop)
  (:export *mmop-port*
           *rmq-host*
           build-test-node
           testing-node1
           testing-node2
           testing-node3
           testing-node4
           test-recipe1
           test-recipe2
           test-recipe3
           test-recipe4
           build-test-recipe1
           build-test-recipe2
           build-test-recipe3
           build-test-recipe4))
(in-package :monomyth/processing-tests/utils)

(defparameter *rmq-host* (uiop:getenv "TEST_PROCESSING_RMQ"))
(defparameter *mmop-port* 55555)

(transactional
    (defclass testing-node (rmq-node) ()))

(defun build-test-node (name source dest fail size)
  (make-instance 'testing-node :name name :source source :dest dest :fail fail
                               :batch-size size :type :test
                               :conn (setup-connection :host *rmq-host*)))

(transactional
    (defclass testing-node1 (rmq-node) ()))

(defclass test-recipe1 (rmq-node-recipe) ())

(defun build-test-node1 (name source dest fail size)
  (make-instance 'testing-node1 :name name :source source :dest dest :fail fail
                                :batch-size size :type :test1
                                :conn (setup-connection :host *rmq-host*)))

(defun build-test-recipe1 (source dest)
  (make-instance 'test-recipe1 :source source :dest dest :type :test1))

(defmethod transform-fn ((node testing-node1) item)
  (format nil "~a18" item))

(defmethod build-node ((worker rmq-worker) (recipe test-recipe1))
  (build-test-node1 (name-node recipe) (rmq-node-recipe/source-queue recipe)
                    (rmq-node-recipe/dest-queue recipe) (name-fail-queue recipe)
                    (node-recipe/batch-size recipe)))

(transactional
    (defclass testing-node2 (rmq-node) ()))

(defclass test-recipe2 (rmq-node-recipe) ())

(defun build-test-node2 (name source dest fail size)
  (make-instance 'testing-node2 :name name :source source :dest dest :fail fail
                                :batch-size size :type :test2
                                :conn (setup-connection :host *rmq-host*)))

(defun build-test-recipe2 (source dest)
  (make-instance 'test-recipe2 :source source :dest dest :type :test2))

(defmethod transform-fn ((node testing-node2) item)
  (coerce (remove-if #'alpha-char-p (coerce item 'list)) 'string))

(defmethod build-node ((worker rmq-worker) (recipe test-recipe2))
  (build-test-node2 (name-node recipe) (rmq-node-recipe/source-queue recipe)
                    (rmq-node-recipe/dest-queue recipe) (name-fail-queue recipe)
                    (node-recipe/batch-size recipe)))

(transactional
    (defclass testing-node3 (rmq-node) ()))

(defclass test-recipe3 (rmq-node-recipe) ())

(defun build-test-node3 (name source dest fail size)
  (make-instance 'testing-node3 :name name :source source :dest dest :fail fail
                                :batch-size size :type :test3
                                :conn (setup-connection :host *rmq-host*)))

(defun build-test-recipe3 (source dest)
  (make-instance 'test-recipe3 :source source :dest dest :type :test3))

(defmethod transform-fn ((node testing-node3) item)
  (format nil "~a" (* (parse-integer item) 7)))

(defmethod build-node ((worker rmq-worker) (recipe test-recipe3))
  (build-test-node3 (name-node recipe) (rmq-node-recipe/source-queue recipe)
                    (rmq-node-recipe/dest-queue recipe) (name-fail-queue recipe)
                    (node-recipe/batch-size recipe)))

(transactional
    (defclass testing-node4 (rmq-node) ()))

(defclass test-recipe4 (rmq-node-recipe) ())

(defun build-test-node4 (name source dest fail size)
  (make-instance 'testing-node4 :name name :source source :dest dest :fail fail
                                :batch-size size :type :test4
                                :conn (setup-connection :host *rmq-host*)))

(defun build-test-recipe4 (source dest)
  (make-instance 'test-recipe4 :source source :dest dest :type :test4))

(defmethod transform-fn ((node testing-node4) item)
  (format nil "test ~a" item))

(defmethod build-node ((worker rmq-worker) (recipe test-recipe4))
  (build-test-node4 (name-node recipe) (rmq-node-recipe/source-queue recipe)
                    (rmq-node-recipe/dest-queue recipe) (name-fail-queue recipe)
                    (node-recipe/batch-size recipe)))
