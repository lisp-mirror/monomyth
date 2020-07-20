(defpackage monomyth/master
  (:use :cl :stmx :stmx.util :monomyth/mmop :monomyth/node-recipe :trivia
        :monomyth/mmop-master)
  (:export start-master
           stop-master
           master-workers
           master-recipes
           master-context
           worker-info-type-counts
           worker-info-outstanding-request-counts
           add-recipe
           ask-to-start-node))
(in-package :monomyth/master)

(setf *arity-check-by-test-call* nil)
(defparameter *internal-conn-name* "inproc://mmop-master-routing")
(defparameter *ready-message* "READY")
(defparameter *end-message* "END")
(defparameter *router-thread-name* "monomyth-master-router")
(defparameter *worker-thread-prefix* "monomyth-worker-thread")
(defparameter *shutdown-pause* 1)

(defstruct (worker-info (:constructor build-worker-info ()))
  "the only info tracked by a worker is the number of worker nodes of each type
and the number of outstanding requests of each type"
  (type-counts (make-instance 'thash-table :test 'equal) :read-only t)
  (outstanding-request-counts (make-instance 'thash-table :test 'equal) :read-only t))

(transactional
    (defstruct (master (:constructor build-master))
      "the master system only has two fields, a table of worker identifiers to worker infos,
and a table of node type symbols to node recipes"
      (workers (make-instance 'thash-table :test 'equal)
       :read-only t
       :transactional nil)
      (recipes (make-instance 'thash-table :test 'equal)
       :read-only t
       :transactional nil)
      (context (pzmq:ctx-new)
       :read-only t
       :transactional nil)
      (outbound-socket nil
       :transactional nil)
      (running t)))

(defun start-master (thread-count client-port)
  "starts up all worker threads and the router loop for load balancing"
  (v:info :master "starting master server with ~a threads listening for workers at port ~a"
          thread-count client-port)
  (let ((master (build-master)))
    (atomic
     (let ((socket (pzmq:socket (master-context master) :router)))
       (setf (master-outbound-socket master) socket)
       (pzmq:bind socket (format nil "tcp://*:~a" client-port))))

    (iter:iterate
      (iter:repeat thread-count)
      (start-handler-thread master))
    (start-router-loop master thread-count)
    master))

(defun stop-master (master)
  (v:info :master "shutting down master server")
  (let ((context (master-context master)))
    (atomic (setf (master-running master) nil))
    (sleep *shutdown-pause*)
    (dolist (th (remove-if-not
                 #'(lambda (th) (let ((th-name (bt:thread-name th)))
                                  (or (string= th-name *router-thread-name*)
                                      (rtl:starts-with *worker-thread-prefix* th-name))))
                 (bt:all-threads)))
      (bt:destroy-thread th))
    (pzmq:close (master-outbound-socket master))
    (pzmq:ctx-destroy context)))

(defun start-router-loop (master thread-count)
  "runs the thread router's event loop in a new thread"
  (bt:make-thread
   #'(lambda ()
       (pzmq:with-socket (threads (master-context master)) :router
         (pzmq:bind threads *internal-conn-name*)

         (iter:iterate
           (iter:while (master-running master))
           (iter:for worker-id = (first (handle-pull-msg threads "get-thread")))
           (unless worker-id (iter:next-iteration))
           (iter:for msg-frames = (handle-pull-msg (master-outbound-socket master) "get-msg"))
           (unless msg-frames (iter:next-iteration))
           (forward-frames-to-worker threads worker-id msg-frames)

           (iter:finally (end-threads threads thread-count)))))
   :name *router-thread-name*))

(defun end-threads (socket thread-count)
  "sends a message to all threads, allowing them to cycle and so to quit"
  (iter:iterate
    (iter:repeat thread-count)
    (iter:for worker-id = (first (handle-pull-msg socket "get-thread")))
    (forward-frames-to-worker socket worker-id `(,*end-message* ,*end-message*))))

(defun forward-frames-to-worker (socket thread-id frames)
  "constructs the message frames and sends them on to the thread"
  (handler-case (send-msg-frames socket nil (append `(,thread-id "") frames))
    (mmop-error (c)
      (v:error '(:master.router :mmop) "could not forward message: ~a"
               (mmop-error/message c)))))

(defun handle-pull-msg (socket step)
  "wraps the pull msg call in an appropriate handler"
  (handler-case (pull-msg socket)
    (mmop-error (c)
      (v:error '(:master.router :mmop)
               "could not pull MMOP message (version: ~a) for ~a: ~a"
               (mmop-error/version c) step (mmop-error/message c))
      nil)))

(defun start-handler-thread (master)
  "starts up a handler thread listening for a router to send it messages"
  (let ((idenifier (format nil "~a-~a" *worker-thread-prefix* (uuid:make-v4-uuid))))
    (bt:make-thread
     #'(lambda ()
         (pzmq:with-socket (router (master-context master)) :req
           (pzmq:setsockopt router :identity idenifier)
           (pzmq:connect router *internal-conn-name*)

           (iter:iterate
             (iter:while (master-running master))
             (pzmq:send router *ready-message*)
             (handler-case (handle-message master (mmop-m:pull-master-message router))
               (mmop-error (c)
                 (v:error '(:master.handler :mmop)
                          "could not pull MMOP message (version: ~a): ~a"
                          (mmop-error/version c) (mmop-error/message c)))))))
     :name idenifier)))

(defun handle-message (master mmop-msg)
  "handles a specific message for the master, return t if the master should continue"
  (let ((res (match mmop-msg
               ((mmop-m:worker-ready-v0 :client-id client-id)
                (add-worker master client-id))

               ((mmop-m:start-node-success-v0
                 :client-id id :type type-id)
                (start-successful master id type-id))

               ((mmop-m:start-node-failure-v0
                 :client-id id :type type-id :reason-cat cat :reason-msg msg)
                (start-unsuccessful master id type-id cat msg)))))

    (unless res
      (v:error '(:master.handler.event-loop :mmop)
               "did not recognize [~a] in worker event loop" mmop-msg))
    t))

(defun start-unsuccessful (master client-id type-id cat msg)
  "removes the record of the outstanding request"
  (v:error :master.handler "~a node failed to start on ~a (~a): ~a"
           type-id client-id cat msg)
  (atomic
   (decf (get-ghash
          (worker-info-outstanding-request-counts
           (get-ghash (master-workers master) client-id))
          type-id))))

(defun start-successful (master client-id type-id)
  "removes the record of the outstanding request and increments the type count for that client"
  (v:info :master.handler "~a node started on ~a" type-id client-id)
  (atomic
   (decf (get-ghash
          (worker-info-outstanding-request-counts
           (get-ghash (master-workers master) client-id))
          type-id))
   (let ((val (get-ghash
               (worker-info-type-counts
                (get-ghash (master-workers master) client-id))
               type-id 0)))
     (setf (get-ghash
            (worker-info-type-counts
             (get-ghash (master-workers master) client-id))
            type-id)
           (1+ val)))))

(defun add-worker (master client-id)
  "adds a worker info and id to the master"
  (v:info :master.handler "worker ~a has signaled that it is ready" client-id)
  (atomic
   (setf (get-ghash (master-workers master) client-id)
         (build-worker-info))))

(transaction
    (defun total-posible-nodes (worker type-id)
      "calculates the total possible worker threads of that type"
      (+ (get-ghash (worker-info-type-counts worker) type-id 0)
          (get-ghash (worker-info-outstanding-request-counts worker) type-id 0))))

(transaction
    (defun find-worker-lowest-node-type-count (master type-id)
      "finds the worker id with the smallest number of those nodes running"
      (first
       (reduce
        #'(lambda (pair1 pair2)
            (if (< (cdr pair1) (cdr pair2))
                pair1 pair2))
        (mapcar
         #'(lambda (worker-pair)
             `(,(car worker-pair) . ,(total-posible-nodes (cdr worker-pair) type-id)))
         (ghash-pairs (master-workers master)))))))

(transaction
    (defun determine-worker-for-node (master type-id)
      "determines the best worker id for the recipe type"
      (find-worker-lowest-node-type-count master type-id)))

(transaction
    (defun add-recipe (master recipe)
      "adds a recipe to the master records"
      (setf (get-ghash (master-recipes master)
                       (symbol-name (node-recipe/type recipe)))
            recipe)))

(defun ask-to-start-node (master type-id)
  "attempts to start a node of type-id on one of the masters workers.
returns t if it works, nil otherwise"
  (let ((recipe (get-ghash (master-recipes master) type-id)))
    (if recipe
        (handler-case
            (let ((worker-id (determine-worker-for-node master type-id)))
              (send-msg (master-outbound-socket master) *mmop-v0*
                        (mmop-m:make-start-node-v0 worker-id recipe))
              (atomic
               (let ((val (get-ghash
                           (worker-info-outstanding-request-counts
                            (get-ghash (master-workers master) worker-id))
                           type-id 0)))
                 (setf (get-ghash
                        (worker-info-outstanding-request-counts
                         (get-ghash (master-workers master) worker-id))
                        type-id)
                       (1+ val))))
              t)
          (mmop-error (c)
            (progn
              (v:error :master.handler
                       "could not send start node message (mmop version: ~a): ~a"
                       (mmop-error/version c) (mmop-error/message c))
              nil)))
        (progn (v:error :master.handler "could not find recipe type ~a" type-id)
               nil))))
