(defpackage monomyth/tests/control-api
  (:use :cl :rove :monomyth/control-api/main :bordeaux-threads :monomyth/mmop
   :monomyth/master :jonathan :cl-rabbit :monomyth/tests/utils :rutils.misc
   :monomyth/rmq-node :monomyth/rmq-worker :monomyth/worker))
(in-package :monomyth/tests/control-api)

(v:output-here *terminal-io*)
(setf (v:repl-level) :debug)
(defparameter *api-port* 8888)
(defparameter *api-uri* (format nil "http://127.0.0.1:~a" *api-port*))
(defparameter *master-port* 55555)
(defparameter *master-uri* (format nil "tcp://127.0.0.1:~a" *master-port*))
(defparameter queue-1 (format nil "process-test-~a-1" (get-universal-time)))
(defparameter queue-2 (format nil "process-test-~a-2" (get-universal-time)))
(defparameter queue-3 (format nil "process-test-~a-3" (get-universal-time)))
(defparameter queue-4 (format nil "process-test-~a-4" (get-universal-time)))

(teardown
 (let ((conn (setup-connection :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
   (with-channel (conn 1)
     (queue-delete conn 1 "TEST1-fail")
     (queue-delete conn 1 "TEST2-fail")
     (queue-delete conn 1 "TEST3-fail")
     (queue-delete conn 1 queue-1)
     (queue-delete conn 1 queue-2)
     (queue-delete conn 1 queue-3)
     (queue-delete conn 1 queue-4))
   (destroy-connection conn)))

(deftest startup
  (let ((master (start-master 2 *master-port*)))
    (start-server *master-uri* *api-port*)
    (stop-server)
    (stop-master master)
    (pass "server shutdown")))

(deftest ping-endpoint
  (let ((master (start-master 2 *master-port*))
        (uri (quri:uri *api-uri*)))
    (start-server *master-uri* *api-port*)

    (setf (quri:uri-path uri) "/ping")
    (let* ((resp (multiple-value-list (dex:get uri))))
      (ok (= (nth 1 resp) 200))
      (ok (string= (car resp) "pong")))

    (stop-server)
    (stop-master master)
    (pass "server shutdown")))

(deftest start-node-endpoint
  (let ((master (start-master 2 *master-port*))
        (worker (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*))
        (uri (quri:uri *api-uri*))
        (recipe (build-test-recipe queue-1 queue-2)))
    (start-server *master-uri* *api-port*)
    (add-recipe master recipe)

    (testing "no workers"
      (setf (quri:uri-path uri) "/start-node/TEST")
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

    (stop-server)
    (stop-master master)
    (pass "server shutdown")))

(deftest stop-worker-endpoint
  (let ((master (start-master 2 *master-port*))
        (worker (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*))
        (uri (quri:uri *api-uri*))
        (recipe (build-test-recipe queue-1 queue-2)))
    (start-server *master-uri* *api-port*)
    (add-recipe master recipe)

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

    (stop-server)
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
        (recipe1 (build-test-recipe1 queue-1 queue-2 5))
        (recipe2 (build-test-recipe2 queue-2 queue-3 10))
        (recipe3 (build-test-recipe3 queue-3 queue-4 4))
        (worker (build-rmq-worker :host *rmq-host* :username *rmq-user* :password *rmq-pass*))
        (client-name (format nil "test-client-~a" (uuid:make-v4-uuid)))
        (uri (quri:uri *api-uri*)))
    (start-server *master-uri* *api-port*)
    (bt:make-thread #'(lambda ()
                        (start-worker worker (format nil "tcp://localhost:~a" *master-port*))
                        (run-worker worker)
                        (stop-worker worker)
                        (pass "worker-stopped")))
    (add-recipe master recipe1)
    (add-recipe master recipe2)
    (add-recipe master recipe3)
    (setf (quri:uri-path uri) "/recipe-info")

    (testing "no running nodes/no recipes"
      (ok (string= (dex:get uri) (to-json '()))))

    (testing "no running nodes/recipes"
      (add-recipe master recipe1)
      (add-recipe master recipe2)
      (add-recipe master recipe3)

      (let ((resp (multiple-value-list (dex:get uri))))
        (ok (= (nth 1 resp) 200))
        (ok (string= (car resp) (to-json '())))))

    (testing "nodes running"
      (pzmq:with-context nil
        (pzmq:with-socket client :dealer
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client (format nil "tcp://localhost:~a" *master-port*))

          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST2"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST1"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST3"))
          (test-request-success client)
          (send-msg client *mmop-v0* (mmop-c:start-node-request-v0 "TEST2"))
          (test-request-success client)))

      (let ((resp (multiple-value-list (dex:get uri))))
        (ok (= (nth 1 resp) 200))
        (confirm-recipe-counts (parse (car resp)))))

      (pzmq:with-context nil
        (pzmq:with-socket client :dealer
          (pzmq:setsockopt client :identity client-name)
          (pzmq:connect client (format nil "tcp://localhost:~a" *master-port*))

          (send-msg client *mmop-v0* (mmop-c:stop-worker-request-v0 (worker/name worker)))
          (test-shutdown-success client)))

    (stop-server)
    (stop-master master)
    (pass "server shutdown")))
