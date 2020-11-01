(defpackage monomyth/master
  (:use :cl :stmx :stmx.util :monomyth/mmop :monomyth/node-recipe :trivia
        :monomyth/mmop-master :jonathan)
  (:export start-master
           stop-master
           master-workers
           master-recipes
           master-context
           worker-info-type-counts
           worker-info-outstanding-request-counts
           add-recipe
           ask-to-start-node
           ask-to-shutdown-worker))
(in-package :monomyth/master)

(setf *arity-check-by-test-call* nil)
(defparameter *internal-conn-name* "inproc://mmop-master-routing")
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
      (running t)))

(defun start-master (thread-count client-port)
  "starts up all worker threads and the router loop for load balancing"
  (v:info :master "starting master server with ~a threads listening for workers at port ~a"
          thread-count client-port)
  (let* ((master (build-master))
         (thread-names
           (iter:iterate
             (iter:repeat thread-count)
             (iter:collect (start-handler-thread master)))))
    (start-router-loop master client-port thread-count thread-names)
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
    (pzmq:ctx-destroy context)))

(defun start-router-loop (master client-port thread-count thread-names)
  "runs the thread router's event loop in a new thread"
  (bt:make-thread
   #'(lambda ()
       (pzmq:with-sockets (((threads (master-context master)) :router)
                           ((clients (master-context master)) :router))
         (pzmq:bind threads *internal-conn-name*)
         (pzmq:bind clients (format nil "tcp://*:~a" client-port))

         (pzmq:with-poll-items items (threads clients)
           (iter:iterate
             (iter:with wrker-count = 0)
             (iter:while (master-running master))
             (pzmq:poll items)

             (when (member :pollin (pzmq:revents items 0))
              (route-outgoing-message clients threads))

             (when (member :pollin (pzmq:revents items 1))
               (route-incoming-message
                clients threads (nth (mod wrker-count thread-count) thread-names))
               (incf wrker-count))))))
   :name *router-thread-name*))

(defun route-outgoing-message (clients threads)
  (let ((frames (handle-pull-msg threads "get-inbound-msg")))
    (v:debug '(:master.router.outgoing)
             "received message: (~{~a~^, ~})" frames)
    (forward-frames-to-client clients frames)
    (v:debug '(:master.router.outgoing) "forwarded message to client")))

(defun route-incoming-message (clients threads worker-id)
  (let ((msg-frames (handle-pull-msg clients "get-inbound-msg")))
    (v:debug '(:master.router.incoming)
             "received message: (~{~a~^, ~})" msg-frames)
    (forward-frames-to-worker threads worker-id msg-frames)
    (v:debug '(:master.router.incoming) "forwarded message to worker")))

(defun forward-frames-to-client (socket frames)
  "drops unneeded frames and sends them to the client"
  (handler-case (send-msg-frames socket *mmop-v0* (cdr frames))
    (mmop-error (c)
      (v:error '(:master.router :mmop) "could not forward outbound message: ~a"
               (mmop-error/message c)))))

(defun forward-frames-to-worker (socket thread-id frames)
  "constructs the message frames and sends them on to the thread"
  (handler-case (send-msg-frames socket *mmop-v0* (cons thread-id frames))
    (mmop-error (c)
      (v:error '(:master.router :mmop) "could not forward inbound message: ~a"
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
         (pzmq:with-socket (router (master-context master)) :dealer
           (pzmq:setsockopt router :identity idenifier)
           (pzmq:connect router *internal-conn-name*)

           (iter:iterate
             (iter:while (master-running master))
             (handler-case (handle-message master router (mmop-m:pull-master-message router))
               (mmop-error (c)
                 (v:error '(:master.handler :mmop)
                          "could not pull MMOP message (version: ~a): ~a"
                          (mmop-error/version c) (mmop-error/message c)))))))
     :name idenifier)
    idenifier))

(defun handle-message (master socket mmop-msg)
  "handles a specific message for the master"
  (adt:match received-mmop mmop-msg
    ((ping-v0 client-id)
     (send-pong-v0 socket client-id))

    ((recipe-info-v0 client-id)
     (send-recipe-info master socket client-id))

    ((worker-info-v0 client-id)
     (send-worker-info master socket client-id))

    ((start-node-request-v0 client-id recipe-type)
     (ask-to-start-node master socket client-id recipe-type))

    ((stop-worker-request-v0 client-id worker-id)
     (ask-to-shutdown-worker master socket client-id worker-id))

    ((worker-ready-v0 client-id)
     (add-worker master client-id))

    ((start-node-success-v0 id type-id)
     (start-successful master id type-id))

    ((start-node-failure-v0 id type-id cat msg)
     (start-unsuccessful master id type-id cat msg))))

(defun send-pong-v0 (socket client-id)
  (v:debug '(:master.handler.ping) "got message (~a)" client-id)
  (send-msg socket *mmop-v0* (pong-v0 client-id))
  (v:debug '(:master.handler.ping) "sent pong"))

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
    (defun pull-worker-type-running-info (worker)
      "takes a worker-info and produces an fset map that links each recipe type
to a plist with :running"
      (reduce #'(lambda (acc val) (fset:with acc (car val) `(:|running| ,(cdr val))))
              (ghash-pairs (worker-info-type-counts worker))
              :initial-value (fset:empty-map))))

(transaction
    (defun pull-worker-type-queued-info (worker)
      "takes a worker-info and produces an fset map that links each recipe type
to a plist with :queued"
      (reduce #'(lambda (acc val) (fset:with acc (car val) `(:|queued| ,(cdr val))))
              (ghash-pairs (worker-info-outstanding-request-counts worker))
              :initial-value (fset:empty-map))))

(transaction
    (defun pull-worker-type-info (worker)
      "takes a worker-info and produces an fset map that links each recipe type
to a plist with :running and :queued"
      (fset:map-union (pull-worker-type-queued-info worker)
                      (pull-worker-type-running-info worker)
                      #'append)))

(defun combine-type-plist (l1 l2)
  "takes two type count plists and combines the counts"
  (flet ((add-property (prop) (+ (getf l1 prop 0) (getf l2 prop 0))))
    `(:|running| ,(add-property :|running|) :|queued| ,(add-property :|queued|))))

(transaction
    (defun pull-master-type-info (master)
      "takes a master object and produces an fset map that links each recipe type
to a plist with :running and :queued"
      (reduce
       #'(lambda (acc val) (fset:map-union acc val #'combine-type-plist))
       (mapcar #'pull-worker-type-info (ghash-values (master-workers master)))
       :initial-value (fset:empty-map))))

(defun send-recipe-info (master socket client-id)
  (let ((info-map (atomic (pull-master-type-info master))))
    (send-msg
     socket *mmop-v0*
     (json-info-response-v0
      client-id
      (to-json
       (fset:reduce
        #'(lambda (acc key val)
            (append `((:|type| ,key :|counts| ,val)) acc))
        info-map
        :initial-value '()))))))

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

(defun start-node (master socket client-id type-id recipe)
  "Sends the start node request to a client with the supplied recipe"
  (handler-case
      (let ((worker-id (atomic (determine-worker-for-node master type-id))))
        (send-msg socket *mmop-v0* (start-node-v0 worker-id recipe))
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
        (send-msg socket *mmop-v0* (start-node-request-success-v0 client-id)))

    (mmop-error (c)
      (progn
        (v:error :master.handler
                 "could not send start node message (mmop version: ~a): ~a"
                 (mmop-error/version c) (mmop-error/message c))))))

(defun confirm-start-node-failure (socket client-id message code)
  "Sends a request failure message to the client with the supplied message."
  (handler-case
      (send-msg socket *mmop-v0* (start-node-request-failure-v0 client-id message code))

    (mmop-error (c)
      (v:error :master.handler
               "could not send start node failed message (mmop version: ~a): ~a"
               (mmop-error/version c) (mmop-error/message c)))))

(defun ask-to-start-node (master socket client-id type-id)
  "attempts to start a node of type-id on one of the masters workers.
returns t if it works, nil otherwise"
  (v:info :master "requesting to start node ~a" type-id)
  (let ((recipe (get-ghash (master-recipes master) type-id)))
    (cond
      ((ghash-table-empty? (master-workers master))
       (let ((msg "no active worker servers"))
         (v:error :master.handler.start-node msg)
         (confirm-start-node-failure socket client-id msg 503)))

      (recipe (start-node master socket client-id type-id recipe))

      (t (let ((msg (format nil "could not find recipe type ~a" type-id)))
           (v:error :master.handler.start-node msg)
           (confirm-start-node-failure socket client-id msg 400))))))

(defun ask-to-shutdown-worker (master socket client-id worker-id)
  "uses a master to tell a worker to shutdown via MMOP.
No state in the master is currently changes, returns t if the call seems to have
been sent, nil otherwise"
  (v:info :master "requesting to stop worker ~a" worker-id)
  (if (get-ghash (master-workers master) worker-id)
      (handler-case
          (progn
            (send-msg socket *mmop-v0* (shutdown-worker-v0 worker-id))
            (send-msg socket *mmop-v0* (stop-worker-request-success-v0 client-id)))

        (mmop-error (c)
          (progn
            (v:error :master.handler.shutdown-worker
                     "could not send stop worker message (mmop version: ~a): ~a"
                     (mmop-error/version c) (mmop-error/message c)))))
      (progn
        (let ((msg (format nil "could not shutdown unrecognized worker ~a" worker-id)))
          (v:warn :master.handler.shutdown-worker msg)
          (handler-case
              (send-msg socket *mmop-v0* (stop-worker-request-failure-v0 client-id msg 400))

            (mmop-error (c)
              (v:error :master.handler.shutdown-worker
                       "could not send stop worker message (mmop version: ~a): ~a"
                       (mmop-error/version c) (mmop-error/message c))))))))

(transaction
    (defun get-worker-type-info (worker)
      "translates a worker type count map object into an equivalent list of plists"
      (mapcar
       #'(lambda (type-pair)
           `(:|recipe_name| ,(car type-pair) :|node_count| ,(cdr type-pair)))
       (ghash-pairs
        (worker-info-type-counts worker)))))

(transaction
    (defun get-all-worker-type-info (master)
      "translates all workers into plists with node information"
      (mapcar
       #'(lambda (worker-pair)
           `(:|worker_id| ,(car worker-pair)
             :|nodes| ,(get-worker-type-info (cdr worker-pair))))
       (ghash-pairs (master-workers master)))))

(defun send-worker-info (master socket client-id)
  "responds to a worker-info message by pulling the info and turning it into json"
  (send-msg socket *mmop-v0*
            (json-info-response-v0
             client-id (to-json (atomic (get-all-worker-type-info master))))))
