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
(defparameter *master-uri* (format nil "127.0.0.1:~a" *master-port*))
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
    (ok (string= (dex:get uri) "pong"))

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
        (uri (quri:uri *api-uri*)))
    (bt:make-thread #'(lambda ()
                        (start-worker worker (format nil "tcp://localhost:~a" *master-port*))
                        (run-worker worker)
                        (stop-worker worker)
                        (pass "worker-stopped")))
    (add-recipe master recipe1)
    (add-recipe master recipe2)
    (add-recipe master recipe3)
    (start-server *master-uri* *api-port*)
    (setf (quri:uri-path uri) "/recipe-info")

    (testing "no running nodes/no recipes"
      (ok (string= (dex:get uri) (to-json '()))))

    (testing "no running nodes/recipes"
      (add-recipe master recipe1)
      (add-recipe master recipe2)
      (add-recipe master recipe3)

      (ok (string= (dex:get uri) (to-json '()))))

    (testing "nodes running"
      (ok (ask-to-start-node master "TEST3"))
      (ok (ask-to-start-node master "TEST3"))
      (ok (ask-to-start-node master "TEST2"))
      (ok (ask-to-start-node master "TEST1"))
      (ok (ask-to-start-node master "TEST3"))
      (ok (ask-to-start-node master "TEST2"))

      (confirm-recipe-counts (parse (dex:get uri))))

    (ask-to-shutdown-worker master (worker/name worker))
    (stop-server)
    (stop-master master)
    (pass "server shutdown")))
