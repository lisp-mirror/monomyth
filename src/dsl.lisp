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
         (name fail type host port user pass)
       (make-instance
        (quote ,name)
        :name name :source ,source-queue :fail fail :type type
        :batch-size ,size
        ,@(if dest-queue `(:dest ,dest-queue) '(:place-destination nil))
        :conn (setup-connection :host host :port port :username user
                                :password pass)))

     (defun ,(mashup-symbol 'build- name '-recipe) ()
       (make-instance (quote ,(mashup-symbol name '-recipe))
        :type ,name-key))

     (defmethod build-node
         ((worker rmq-worker) (recipe ,(mashup-symbol name '-recipe)))
       (,(mashup-symbol 'build- name)
        (name-node recipe)
        (name-fail-queue recipe)
        (node-recipe/type recipe)
        (rmq-worker/host worker)
        (rmq-worker/port worker)
        (rmq-worker/username worker)
        (rmq-worker/password worker)))

     (defmethod transform-fn ((node ,name) item)
       (funcall ,transform-func item))))

(defmacro define-rmq-node
    (name transform-func source-queue size &key dest-queue)
  "Defines all classes, methods, and functions for a new node type."
  (with-gensyms (keyword-sym)
    (define-rmq-node-internal name transform-func source-queue dest-queue
      size keyword-sym)))

(defun build-queues (place-last nodes)
  "turns the linear list of edges into queue names"
  (mapcar #'(lambda (first-node second-node)
              (if (getf second-node :name)
                  (format nil "~a-to-~a" (getf first-node :name)
                          (getf second-node :name))
                  nil))
          (cons `(:name ,*start-name*) nodes)
          (append nodes `((:name ,(if place-last *end-name* nil))))))

(defmacro define-system ((&key (place-last t)) &body nodes)
  "Takes a list of plist (:name :fn :batch-size) and turns them into rmq nodes
that work in sequential order.
All recipes are also set up to be loaded into master server via the add-recipes
method."
  (let ((queues (build-queues place-last nodes)))
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
