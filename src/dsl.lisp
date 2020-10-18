(defpackage monomyth/dsl
  (:use :cl :monomyth/rmq-node :stmx :monomyth/node :monomyth/rmq-node-recipe
   :monomyth/rmq-worker :monomyth/worker :monomyth/node-recipe)
  (:import-from :alexandria with-gensyms)
  (:export define-rmq-node mashup-symbol))
(in-package :monomyth/dsl)

(defun mashup-symbol (&rest objects)
  "takes a bunch of symbols and combines them"
  (intern (format nil "~{~a~}" objects)))

(defmacro define-rmq-node (name transform-func source-queue dest-queue size)
  "Defines all classes, methods, and functions for a new node type."
  (with-gensyms (keyword-sym)

    `(let ((,keyword-sym ,(intern (symbol-name name) "KEYWORD")))

       (transactional (defclass ,name (rmq-node) ()))

       (defclass ,(mashup-symbol name '-recipe) (rmq-node-recipe) ())

       (defun ,(mashup-symbol 'build- name)
           (name source dest fail type size host port user pass)
         (make-instance
          (quote ,name)
          :name name :source source :dest dest :fail fail :type type :size size
          :conn (setup-connection :host host :port port :username user
                                  :password pass)))

       (defun ,(mashup-symbol 'build- name '-recipe) ()
         (make-instance
          (quote ,(mashup-symbol name '-recipe))
          :source ,source-queue :dest ,dest-queue :type ,keyword-sym
          :batch-size ,size))

       (defmethod build-node
           ((worker rmq-worker) (recipe ,(mashup-symbol name '-recipe)))
         (,(mashup-symbol 'build- name)
          (name-node recipe)
          (rmq-node-recipe/source-queue recipe)
          (rmq-node-recipe/dest-queue recipe)
          (name-fail-queue recipe)
          (node-recipe/type recipe)
          (node-recipe/batch-size recipe)
          (rmq-worker/host worker)
          (rmq-worker/port worker)
          (rmq-worker/username worker)
          (rmq-worker/password worker)))

       (defmethod transform-fn ((node ,name) items)
         (funcall ,transform-func items)))))
