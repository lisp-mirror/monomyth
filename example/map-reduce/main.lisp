(defpackage monomyth/map-reduce/main
  (:use :cl :monomyth/map-reduce/load :monomyth/map-reduce/map
        :monomyth/map-reduce/store :monomyth/dsl :monomyth/worker
        :monomyth/rmq-worker :cl-rabbit :monomyth/rmq-node :monomyth/mmop
        :monomyth/node-recipe :monomyth/master :monomyth/control-api/main
        :rutils.bind :jonathan)
  (:export run))
(in-package :monomyth/map-reduce/main)

(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))
(defparameter *rmq-port* 5672)
(defparameter *rmq-user* (uiop:getenv "TEST_RMQ_DEFAULT_USER"))
(defparameter *rmq-pass* (uiop:getenv "TEST_RMQ_DEFAULT_PASS"))
(defparameter *master-host* "localhost")
(defparameter *mmop-port* 55555)
(defparameter *master-uri* (format nil "tcp://~a:~a" *master-host* *mmop-port*))

(defparameter *queue1* "LOAD-FILE-to-MAP-TEXT")
(defparameter *queue2* "MAX-TEXT-to-STORE-COUNTS")
(defparameter *fail-queue1* "LOAD-FILE-fail")
(defparameter *fail-queue2* "MAP-TEXT-fail")
(defparameter *fail-queue3* "STORE-COUNTS-fail")

(defparameter *file-path* (uiop:getenv "TEST_FILE_PATH"))
(defparameter *load-items-lines-per-item* 10)
(defparameter *load-item-batch-size* 1)
(defparameter *map-text-batch-size* 10)
(defparameter *store-batch-size* 10)

(defparameter *worker-counts* 2)
(defparameter *load-node-counts* 1)
(defparameter *map-node-counts* 1)
(defparameter *store-node-counts* 1)

(defparameter *control-port* 44444)
(defparameter *control-uri* (quri:uri (format nil "http://127.0.0.1:~a" *control-port*)))

(defparameter *master-threads* 2)

(defparameter *wait-between-checks* 5)

(define-system map-reduce (:pull-first nil :place-last nil)
  (:name load-file
   :fn (build-load-file-fn *load-items-lines-per-item*)
   :batch-size *load-item-batch-size*
   :start-fn (build-start-file-fn *file-path*)
   :stop-fn #'stop-file-fn)
  (:name map-text
   :fn #'text->word-count
   :batch-size *map-text-batch-size*)
  (:name store-counts
   :fn #'store-counts
   :batch-size *store-batch-size*
   :start-fn #'start-db-fn
   :stop-fn #'stop-db-fn))

(defun run-worker-thread (i)
  (bt:make-thread
   #'(lambda ()
       (let ((worker (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                       :password *rmq-pass*)))
         (start-worker worker *master-uri*)
         (run-worker worker)
         (stop-worker worker)))
   :name (format nil "worker-thread~a" i)))

(defun run-workers ()
  (iter:iterate
    (iter:for i from 0 below *worker-counts*)
    (run-worker-thread i)))

(defun clean-rmq ()
  (let ((conn (setup-connection :host *rmq-host* :username *rmq-user*
                                :password *rmq-pass*)))
    (with-channel (conn 1)
      (queue-delete conn 1 *queue1*)
      (queue-delete conn 1 *queue2*)
      (queue-delete conn 1 *fail-queue1*)
      (queue-delete conn 1 *fail-queue2*)
      (queue-delete conn 1 *fail-queue3*))
    (destroy-connection conn)))

(defun start-nodes ()
  (let ((uri (quri:copy-uri *control-uri*)))
    (setf (quri:uri-path uri) "/start-node/STORE-COUNTS")
    (iter:iterate
      (iter:repeat *store-node-counts*)
      (v:info :main "start node (store counts) returned [~a]"
              (dex:post uri)))

    (setf (quri:uri-path uri) "/start-node/MAP-TEXT")
    (iter:iterate
      (iter:repeat *map-node-counts*)
      (v:info :main "start node (map text) returned [~a]"
              (dex:post uri)))

    (setf (quri:uri-path uri) "/start-node/LOAD-FILE")
    (iter:iterate
      (iter:repeat *load-node-counts*)
      (v:info :main "start node (load file) returned [~a]"
              (dex:post uri)))))

(defun stop-workers ()
  (let ((uri (quri:copy-uri *control-uri*)))
    (setf (quri:uri-path uri) "/worker-info")

    (with ((body status (dex:get uri))
           (payload (parse body)))
      (v:info :main "worker info request returned [status ~a]" status)
      (iter:iterate
        (iter:for item in payload)
        (iter:for path = (format nil "/stop-worker/~a" (getf item :|worker_id|)))
        (setf (quri:uri-path uri) path)
        (v:info :main "worker stop request (~a) returned [result ~a]"
                path (dex:post uri))))))

(defun check-tasks-complete (uri)
  (with ((body status (dex:get uri))
         (payload (parse body)))
    (v:info :main.check "check call returned [status ~a] [body ~a]"
            status body)

    (iter:iterate
      (iter:for item in payload)
      (iter:for counts = (getf item :|counts|))
      (iter:always (and (zerop (getf counts :|queued|)) (zerop (getf counts :|running|)))))))

(defun wait-for-tasks ()
  (let ((uri (quri:copy-uri *control-uri*)))
    (setf (quri:uri-path uri) "/recipe-info")

    (iter:iterate
      (sleep *wait-between-checks*)
      (iter:until (check-tasks-complete uri)))))

(defun run ()
  (let ((master (start-master *master-threads* *mmop-port*))
        (handler (start-control-server *master-uri* *control-port*)))
    (add-map-reduce-recipes master)
    (v:info :main "servers started")
    (run-workers)
    (v:info :main "workers started")
    (v:info :main "starting nodes")
    (start-nodes)
    (wait-for-tasks)
    (v:info :main "stopping workers")
    (stop-workers)
    (stop-control-server handler)
    (stop-master master)))
