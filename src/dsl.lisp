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

(defun build-node-name-key (name)
  "constructs the name key of the node"
  (intern (symbol-name name) "KEYWORD"))

(defun define-rmq-node-internal
    (name transform-func dependents source-queue dest-queue size name-key
     start-fn stop-fn)
  "Internal function that creates the rmq node top level forms."
  `(let ((,name-key ,(build-node-name-key name)))
     (transactional (defclass ,name (rmq-node) ()))

     (defclass ,(mashup-symbol name '-recipe) (rmq-node-recipe) ())

     (defun ,(mashup-symbol 'build- name)
         (name fail node-type host port user pass)
       (make-instance
        (quote ,name)
        :name name :fail fail :type node-type :batch-size ,size
        ,@(if dest-queue `(:dest ,dest-queue) '(:place-destination nil))
        ,@(if source-queue `(:source ,source-queue) '(:pull-source nil))
        :conn (setup-connection :host host :port port :username user
                                :password pass)))

     (defun ,(mashup-symbol 'build- name '-recipe) ()
       (make-instance (quote ,(mashup-symbol name '-recipe))
        :type ,name-key
        ,@(when dependents
           `(:dependent-nodes (quote ,dependents)))))

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
       (funcall ,transform-func node item))

     ,(if start-fn
          ;; NOTE: This method is run ~:before~ because this ensures that the
          ;; worker thread has not been started when the user code executes.
          `(defmethod startup :before
               ((node ,name) context worker-address &optional build-worker-thread)
             (declare (ignorable node context worker-address build-worker-thread))
             (funcall ,start-fn)))

     ,(if stop-fn
          ;; NOTE: This method is run ~:after~ because this ensures that the
          ;; worker thread has been terminated when the user code executes.
           `(defmethod shutdown :after ((node ,name))
              (declare (ignorable node))
              (funcall ,stop-fn)))))

(defmacro define-rmq-node
    (name transform-func size &key source-queue dest-queue start-fn stop-fn)
  "Defines all classes, methods, and functions for a new node type.
The ~:start-fn~ and ~:stop-fn~ should functions that take no arguments and are used
to extend the node's startup and shutdown methods."
  (with-gensyms (keyword-sym)
    (define-rmq-node-internal name transform-func nil source-queue dest-queue
      size keyword-sym start-fn stop-fn)))

(defun build-queues (pull-first place-last nodes)
  "turns the linear list of edges into queue names"
  (mapcar #'(lambda (first-node second-node)
              (if (and (getf first-node :name) (getf second-node :name))
                  (format nil "~a-to-~a" (getf first-node :name)
                          (getf second-node :name))
                  nil))
          (cons `(:name ,(if pull-first *start-name* nil)) nodes)
          (append nodes `((:name ,(if place-last *end-name* nil))))))

(defun build-dependents-lists (nodes)
  "Takes the nodes and constructs the dependency lists needed by the node recipes.
Note that, right now, all systems are a simple linear line, so this function is currently
quite simple."
  (append (cdr (mapcar #'(lambda (val) `(,(build-node-name-key (getf val :name)))) nodes))
          '(nil)))

(defmacro define-system (name (&key (pull-first t) (place-last t)) &body nodes)
  "Takes a system name and a list of plist (:name :fn :batch-size &optional
:start-fn :stop-fn) and turns them into rmq nodes that work in sequential order.
A function called add-<system-name>-recipes is built to make it easy to add the recipes
at start up.
The ~:start-fn~ and ~:stop-fn~ should functions that take no arguments and are used
to extend the node's startup and shutdown methods.
In the system wide keys, pull-first and place-last indicate if the first node
should pull from a source queue and if the last node should place on a destination queue."
  (let ((queues (build-queues pull-first place-last nodes)))
    `(progn
       ,@(mapcar
          #'(lambda (node queue1 queue2 deps)
              (with-gensyms (name-key)
                (define-rmq-node-internal (getf node :name) (getf node :fn) deps
                  queue1 queue2 (getf node :batch-size) name-key (getf node :start-fn)
                  (getf node :stop-fn))))
          nodes queues (cdr queues) (build-dependents-lists nodes))

       (defun ,(mashup-symbol 'add- name '-recipes) (master)
         ,@(mapcar
            #'(lambda (node)
                `(add-recipe master (,(mashup-symbol 'build- (getf node :name) '-recipe))))
            nodes)))))
