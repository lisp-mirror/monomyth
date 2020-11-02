(defpackage monomyth/dsl
  (:use :cl :monomyth/rmq-node :stmx :monomyth/node :monomyth/rmq-node-recipe
   :monomyth/rmq-worker :monomyth/worker :monomyth/node-recipe :monomyth/master)
  (:import-from :alexandria with-gensyms)
  (:export define-system define-rmq-node mashup-symbol build-queues))
(in-package :monomyth/dsl)

(defparameter *start-name* 'start)
(defparameter *end-name* 'end)

(defun mashup-symbol (&rest objects)
  "takes a bunch of symbols and combines them"
  (intern (format nil "~{~a~}" objects)))

(defun define-rmq-node-internal
    (name transform-func source-queue dest-queue size name-key)
  "Internal function that creates the rmq node top level forms."
  `(let ((,name-key ,(intern (symbol-name name) "KEYWORD")))
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
        :source ,source-queue :dest ,dest-queue :type ,name-key
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
       (funcall ,transform-func items))))

(defmacro define-rmq-node (name transform-func source-queue dest-queue size)
  "Defines all classes, methods, and functions for a new node type."
  (with-gensyms (keyword-sym)
    (define-rmq-node-internal name transform-func source-queue dest-queue
      size keyword-sym)))

(defun build-queues (nodes)
  "turns the linear list of edges into queue names"
  (mapcar #'(lambda (first-node second-node)
              (format nil "~a-to-~a" (getf first-node :name)
                      (getf second-node :name)))
          (cons `(:name ,*start-name*) nodes)
          (append nodes `((:name ,*end-name*)))))

(defmacro define-system (&rest nodes)
  "Takes a list of plist (:name :fn :batch-size) and turns them into rmq nodes
that work in sequential order.
All recipes are also set up to be loaded into master server via the add-recipes method."
  (let ((queues (build-queues nodes)))
    `(progn
       ,@(mapcar
          #'(lambda (node queue1 queue2)
              (with-gensyms (name-key)
                (define-rmq-node-internal (getf node :name) (getf node :fn) queue1
                  queue2 (getf node :batch-size) name-key)))
          nodes queues (cdr queues))

       (defmethod add-recipes ((mstr master))
         ,@(mapcar
            #'(lambda (node)
                `(add-recipe mstr (,(mashup-symbol 'build- (getf node :name) '-recipe))))
            nodes)))))
