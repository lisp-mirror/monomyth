(defpackage monomyth/tests/control-api
  (:use :cl :rove :monomyth/control-api/main :bordeaux-threads :monomyth/mmop
        :monomyth/master))
(in-package :monomyth/tests/control-api)

(v:output-here *terminal-io*)
(setf (v:repl-level) :debug)
(defparameter *api-port* 8888)
(defparameter *api-uri* (format nil "http://127.0.0.1:~a" *api-port*))
(defparameter *master-port* 5000)
(defparameter *master-uri* (format nil "127.0.0.1:~a" *master-port*))

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
