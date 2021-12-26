(defpackage monomyth/processing-tests/master
  (:use :cl :rove :monomyth/processing-tests/utils :monomyth/rmq-node :monomyth/node
   :cl-rabbit :monomyth/rmq-node-recipe :monomyth/master :monomyth/node-recipe
   :monomyth/mmop :stmx.util))
(in-package :monomyth/processing-tests/master)

(v:output-here *terminal-io*)

(defparameter *process-time* 10)
(defparameter *number-of-test-msgs* 75)
(defparameter *length-of-test-msgs* 20)
(defparameter *batch-range* 3)
(defparameter *batch-min* 1)
(defparameter *threads-per-recipe-range* 5)
(defparameter *master-threads* 3)

(defun calculate-batch-size ()
  (+ *batch-min* (random *batch-range*)))

(defparameter *control-name* "CONTROL-API")

(defparameter *recipe1* (build-test-node1-recipe))
(defparameter *recipe2* (build-test-node2-recipe))
(defparameter *recipe3* (build-test-node3-recipe))
(defparameter *recipe4* (build-test-node4-recipe))
(defparameter *recipes* `(,*recipe1* ,*recipe2* ,*recipe3* ,*recipe4*))

(defvar *test-context*)

(setup
  (setf *test-context* (pzmq:ctx-new)))

(teardown
  (pzmq:ctx-destroy *test-context*)
  (let ((conn (setup-connection :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
    (with-channel (conn 1)
      (queue-delete conn 1 "TEST-NODE1-fail")
      (queue-delete conn 1 "TEST-NODE2-fail")
      (queue-delete conn 1 "TEST-NODE3-fail")
      (queue-delete conn 1 "TEST-NODE4-fail")
      (queue-delete conn 1 *queue1*)
      (queue-delete conn 1 *queue2*)
      (queue-delete conn 1 *queue3*)
      (queue-delete conn 1 *queue4*)
      (queue-delete conn 1 *queue5*))
    (destroy-connection conn)))

(defun generate-test-msg (length)
  (with-output-to-string (stream)
    (let ((*print-base* 36))
      (loop repeat length do (princ (random 36) stream)))))

(defun send-test-messages ()
  (let ((work-node (build-test-node (format nil "worknode-~d" (get-universal-time))
                                    *queue1* :work *rmq-host*
                                    *rmq-port* *rmq-user* *rmq-pass*)))
    (start-node work-node *test-context* nil nil)
    (let ((msgs (iter:iterate
                  (iter:repeat *number-of-test-msgs*)
                  (iter:for msg = (generate-test-msg *length-of-test-msgs*))
                  (iter:collect msg)
                  (send-message work-node *queue1* msg))))
      (stop-node work-node)
      msgs)))

(defun calculate-result-messge (test-msg)
  (format nil "test ~a"
          (format nil "~a"
                  (* (parse-integer
                      (coerce
                       (remove-if
                        #'alpha-char-p
                        (coerce
                         (format nil "~a~a" test-msg 18)
                         'list))
                       'string))
                     7))))

(deftest master-processing-test
  (let* ((master (start-master *master-threads* *mmop-port*))
         (test-msgs (send-test-messages))
         (expected-result (mapcar #'calculate-result-messge test-msgs))
         (work-node (build-test-node (format nil "worknode-~d" (get-universal-time))
                                     *queue1* :work *rmq-host*
                                     *rmq-port* *rmq-user* *rmq-pass*)))
    (add-basic-example-recipes master)
    (format t "Ready to start tests?~%")
    (read-line)

    (pzmq:with-context nil
      (pzmq:with-socket control :dealer
        (pzmq:setsockopt control :identity *control-name*)
        (pzmq:connect control (format nil "tcp://localhost:~a" *mmop-port*))

        (iter:iterate
          (iter:for recipe in *recipes*)
          (iter:iterate
            (iter:repeat (1+ (random *threads-per-recipe-range*)))
            (send-msg control *mmop-v0* (mmop-c:start-node-request-v0
                                         (symbol-name (node-recipe/type recipe))))))))

    (sleep *process-time*)

    (pzmq:with-context nil
      (pzmq:with-socket control :dealer
        (pzmq:setsockopt control :identity *control-name*)
        (pzmq:connect control (format nil "tcp://localhost:~a" *mmop-port*))

        (iter:iterate
          (iter:for worker-id in (ghash-keys (master-workers master)))
          (send-msg control *mmop-v0* (mmop-c:stop-worker-request-v0 worker-id)))))

    (start-node work-node *test-context* nil nil)
    (labels ((get-msg-w-restart ()
               (handler-case (car (pull-items work-node))
                 (rabbitmq-error (c)
                   (declare (ignore c))
                   (sleep .1)
                   (get-msg-w-restart)))))
      (iter:iterate
        (iter:repeat *number-of-test-msgs*)
        (iter:for got = (get-msg-w-restart))
        (ok (member (rmq-message-body got) expected-result :test #'string=))
        (ack-message work-node got))
      (stop-node work-node))))
