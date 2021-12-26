(defpackage monomyth/tests/control-api
  (:use :cl :rove :monomyth/control-api/main :bordeaux-threads :monomyth/mmop
   :monomyth/master :jonathan :cl-rabbit :monomyth/tests/utils :rutils.misc
        :monomyth/rmq-node :monomyth/rmq-worker :monomyth/worker
        :monomyth/dsl))
(in-package :monomyth/tests/control-api)

(v:output-here *terminal-io*)
(setf (v:repl-level) :debug)
(defparameter *api-port* 8888)
(defparameter *api-uri* (format nil "http://127.0.0.1:~a" *api-port*))
(defparameter *master-port* 55555)
(defparameter *master-uri* (format nil "tcp://127.0.0.1:~a" *master-port*))
(defparameter *startup-wait* 0.5)

(defun fn1 (node item)
  (declare (ignore node))
  (format nil "test1 ~a" item))

(defun fn2 (node item)
  (declare (ignore node))
  (format nil "test2 ~a" item))

(defun fn3 (node item)
  (declare (ignore node))
  (format nil "test3 ~a" item))

(define-system api ()
    (:name test-node1 :fn #'fn1 :batch-size 5)
    (:name test-node2 :fn #'fn2 :batch-size 10)
    (:name test-node3 :fn #'fn3 :batch-size 4))

(teardown
  (let ((conn (setup-connection :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
    (with-channel (conn 1)
      (queue-delete conn 1 "TEST-NODE1-fail")
      (queue-delete conn 1 "TEST-NODE2-fail")
      (queue-delete conn 1 "TEST-NODE3-fail")
      (queue-delete conn 1 queue-1)
      (queue-delete conn 1 queue-2)
      (queue-delete conn 1 queue-3)
      (queue-delete conn 1 queue-4))
    (destroy-connection conn)))

(deftest startup
  (let ((master (start-master 2 *master-port*))
        (handler (start-control-server *master-uri* *api-port*)))
    (sleep *startup-wait*)

    (stop-control-server handler)
    (stop-master master)
    (pass "server shutdown")))


(deftest ping-endpoint
  (let ((master (start-master 2 *master-port*))
        (handler (start-control-server *master-uri* *api-port*))
        (uri (quri:uri *api-uri*)))
    (sleep *startup-wait*)
    (add-api-recipes master)

    (setf (quri:uri-path uri) "/ping")
    (let* ((resp (multiple-value-list (dex:get uri)))
           (body (car resp)))
      (ok (= (nth 1 resp) 200))
      (ok (string= body "pong")))

    (stop-control-server handler)
    (stop-master master)
    (pass "server shutdown")))

(deftest start-node-endpoint
  (let ((master (start-master 2 *master-port*))
        (worker (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                  :password *rmq-pass*))
        (handler (start-control-server *master-uri* *api-port*))
        (uri (quri:uri *api-uri*)))
    (sleep *startup-wait*)
    (add-api-recipes master)

    (testing "no workers"
      (setf (quri:uri-path uri) "/start-node/TEST-NODE1")
      (handler-case (dex:post uri)
        (dex:http-request-failed (e)
          (ok (= (dex:response-status e) 503))
          (let ((body (parse (dex:response-body e))))
            (ng (getf body :|request_sent_to_worker|))
            (ok (string= (getf body :|error_message|)
                         "no active worker servers"))))))

    (bt:make-thread #'(lambda ()
                        (start-worker worker (format nil "tcp://localhost:~a" *master-port*))
                        (run-worker worker)
                        (stop-worker worker)
                        (pass "worker-stopped")))

    (sleep .1)

    (testing "good recipe"
      (let* ((resp (multiple-value-list (dex:post uri)))
             (body (parse (car resp))))
        (ok (= (nth 1 resp) 201))
        (ok (getf body :|request_sent_to_worker|))))

    (testing "bad recipe"
      (setf (quri:uri-path uri) "/start-node/BAD-TEST")
      (handler-case (dex:post uri)
        (dex:http-request-failed (e)
          (ok (= (dex:response-status e) 400))
          (let ((body (parse (dex:response-body e))))
            (ng (getf body :|request_sent_to_worker|))
            (ok (string= (getf body :|error_message|)
                         "could not find recipe type BAD-TEST"))))))

    (stop-control-server handler)
    (stop-master master)
    (pass "server shutdown")))

(deftest stop-worker-endpoint
  (let ((master (start-master 2 *master-port*))
        (worker (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                  :password *rmq-pass*))
        (handler (start-control-server *master-uri* *api-port*))
        (uri (quri:uri *api-uri*)))
    (sleep *startup-wait*)
    (add-api-recipes master)

    (testing "no workers"
      (setf (quri:uri-path uri) "/stop-worker/test")
      (handler-case (dex:post uri)
        (dex:http-request-failed (e)
          (ok (= (dex:response-status e) 400))
          (let ((body (parse (dex:response-body e))))
            (ng (getf body :|request_sent_to_worker|))
            (ok (string= (getf body :|error_message|)
                         "could not shutdown unrecognized worker test"))))))

    (bt:make-thread #'(lambda ()
                        (start-worker worker (format nil "tcp://localhost:~a" *master-port*))
                        (run-worker worker)
                        (stop-worker worker)
                        (pass "worker-stopped")))

    (sleep .1)

    (testing "good worker"
      (setf (quri:uri-path uri) (format nil "/stop-worker/~a" (worker/name worker)))
      (let* ((resp (multiple-value-list (dex:post uri)))
             (body (parse (car resp))))
        (ok (= (nth 1 resp) 201))
        (ok (getf body :|request_sent_to_worker|))))

    (testing "bad worker"
      (setf (quri:uri-path uri) "/stop-worker/test")
      (handler-case (dex:post uri)
        (dex:http-request-failed (e)
          (ok (= (dex:response-status e) 400))
          (let ((body (parse (dex:response-body e))))
            (ng (getf body :|request_sent_to_worker|))
            (ok (string= (getf body :|error_message|)
                         "could not shutdown unrecognized worker test"))))))

    (stop-control-server handler)
    (stop-master master)
    (pass "server shutdown")))

(defun confirm-recipe-counts (json)
  (flet ((get-total (counts)
           (+ (getf counts :|queued| 0) (getf counts :|running| 0))))
    (iter:iterate
      (iter:for obj in json)
      (switch ((getf obj :|type|) :test #'string=)
        ("TEST1" (ok (= 1 (get-total (getf obj :|counts|)))))
        ("TEST2" (ok (= 2 (get-total (getf obj :|counts|)))))
        ("TEST3" (ok (= 3 (get-total (getf obj :|counts|)))))))))

(deftest recipe-info-endpoint
  (let ((master (start-master 2 *master-port*))
        (worker (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                  :password *rmq-pass*))
        (handler (start-control-server *master-uri* *api-port*))
        (client-name (format nil "test-client-~a" (uuid:make-v4-uuid)))
        (uri (quri:uri *api-uri*)))
    (sleep *startup-wait*)

    (bt:make-thread
     #'(lambda ()
         (start-worker worker (format nil "tcp://localhost:~a" *master-port*))
         (run-worker worker)
         (stop-worker worker)
         (pass "worker-stopped")))
    (setf (quri:uri-path uri) "/recipe-info")

    (testing "no running nodes/no recipes"
      (ok (string= (dex:get uri) (to-json '()))))

    (testing "no running nodes/recipes"
      (add-api-recipes master)


      (let* ((resp (multiple-value-list (dex:get uri)))
             (payload (parse (car resp))))
        (ok (= (nth 1 resp) 200))
        (ok (= 3 (length payload)))
        (iter:iterate
          (iter:for item in payload)
          (ok (equal '(:|completed| 0 :|queued| 0 :|running| 0) (getf item :|counts|))))))

    (testing "nodes running"
      (pzmq:with-context nil
        (pzmq:with-socket client :dealer
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client (format nil "tcp://localhost:~a" *master-port*))

          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE2"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE1"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE2"))
          (test-request-success client)))

      (let ((resp (multiple-value-list (dex:get uri))))
        (ok (= (nth 1 resp) 200))
        (confirm-recipe-counts (parse (car resp)))))

    (sleep .1)

    (testing "node completed"
      (send-msg (worker/master-socket worker) *mmop-v0*
                (mmop-w:worker-task-completed-v0 "TEST-NODE1"))

      (sleep 3)

      (iter:iterate
        (sleep 1)
        (let* ((resp (multiple-value-list (dex:get uri)))
               (body (parse (car resp))))
          (ok (= (nth 1 resp) 200))
          (when (iter:iterate
                  (iter:for item in body)
                  (iter:for name = (getf item :|type|))
                  (iter:for count = (getf (getf item :|counts|) :|completed| 0))
                  (if (string= "TEST-NODE1" name)
                      (iter:always (= 1 count)))
                  (if (string= "TEST-NODE2" name)
                      (iter:always (= 2 count)))
                  (if (string= "TEST-NODE3" name)
                      (iter:always (= 3 count))))
            (iter:leave (ok "all jobs complete"))))))

    (pzmq:with-context nil
      (pzmq:with-socket client :dealer
        (pzmq:setsockopt client :identity client-name)
        (pzmq:connect client (format nil "tcp://localhost:~a" *master-port*))

        (send-msg client *mmop-v0* (mmop-c:stop-worker-request-v0 (worker/name worker)))
        (test-shutdown-success client)))

    (stop-control-server handler)
    (stop-master master)
    (pass "server shutdown")))

(deftest worker-info-endpoint
  (let* ((master (start-master 2 *master-port*))
         (handler (start-control-server *master-uri* *api-port*))
         (worker1 (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                    :password *rmq-pass*))
         (worker2 (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                    :password *rmq-pass*))
         (worker3 (build-rmq-worker :host *rmq-host* :username *rmq-user*
                                    :password *rmq-pass*))
         (worker-ids `(,(worker/name worker1) ,(worker/name worker2) ,(worker/name worker3)))
         (client-name (format nil "test-client-~a" (uuid:make-v4-uuid)))
         (uri (quri:uri *api-uri*)))
    (sleep *startup-wait*)
    (add-api-recipes master)
    (setf (quri:uri-path uri) "/worker-info")

    (testing "no workers running"
      (let ((resp (multiple-value-list (dex:get uri))))
        (ok (= (nth 1 resp) 200))
        (ok (string= (car resp) (to-json '())))))

    (bt:make-thread #'(lambda ()
                        (start-worker worker1 (format nil "tcp://localhost:~a" *master-port*))
                        (run-worker worker1)
                        (stop-worker worker1)
                        (pass "worker-stopped")))
    (bt:make-thread #'(lambda ()
                        (start-worker worker2 (format nil "tcp://localhost:~a" *master-port*))
                        (run-worker worker2)
                        (stop-worker worker2)
                        (pass "worker-stopped")))
    (bt:make-thread #'(lambda ()
                        (start-worker worker3 (format nil "tcp://localhost:~a" *master-port*))
                        (run-worker worker3)
                        (stop-worker worker3)
                        (pass "worker-stopped")))

    (sleep .1)

    (testing "workers running, no recipes"
      (let* ((resp (multiple-value-list (dex:get uri)))
             (body (parse (car resp))))
        (ok (= (nth 1 resp) 200))
        (iter:iterate
          (iter:for worker-info in body)
          (ok (member (getf worker-info :|worker_id|) worker-ids :test #'string=))
          (ok (equal (getf worker-info :|nodes|) '())))))

    (testing "started nodes"
      (pzmq:with-context nil
        (pzmq:with-socket client :dealer
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client (format nil "tcp://localhost:~a" *master-port*))

          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE2"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE1"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST-NODE2"))
          (test-request-success client)))

      (sleep .1)

      (let* ((resp (multiple-value-list (dex:get uri)))
             (body (parse (car resp)))
             (counts (reduce
                      #'(lambda (acc val)
                          (fset:map-union
                           acc (node-count-plist-to-map (getf val :|nodes|)) #'add-node-count))
                      body :initial-value (fset:empty-map))))
        (ok (= (nth 1 resp) 200))
        (ok (= (fset:lookup counts "TEST-NODE1") 1))
        (ok (= (fset:lookup counts "TEST-NODE2") 2))
        (ok (= (fset:lookup counts "TEST-NODE3") 4))))

    (stop-control-server handler)
    (stop-master master)
    (pass "server shutdown")))

(defun add-node-count (x y)
  (cond
    ((not x) y)
    ((not y) x)
    (t (+ x y))))

(defun node-count-plist-to-map (plist)
  "translates the node plists into an fset map"
  (reduce
   #'(lambda (acc val)
       (fset:with acc (getf val :|recipe_name|) (getf val :|node_count|)))
   plist
   :initial-value (fset:empty-map)))
