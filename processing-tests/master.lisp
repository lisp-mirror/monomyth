(defpackage monomyth/processing-tests/master
  (:use :cl :rove :monomyth/processing-tests/utils :monomyth/rmq-node :monomyth/node
   :cl-rabbit :monomyth/rmq-node-recipe :monomyth/master :monomyth/node-recipe
   :stmx.util))
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

(defparameter *fn1* "#'(lambda (x) (format nil \"~a~a\" x 18))")
(defparameter *fn2* "#'(lambda (x) (coerce (remove-if #'alpha-char-p (coerce x 'list)) 'string))")
(defparameter *fn3* "#'(lambda (x) (format nil \"~a\" (* (parse-integer x) 7)))")
(defparameter *fn4* "#'(lambda (x) (format nil \"test ~a\" x))")

(defparameter *queue1* (format nil "processing-queue-1-~a" (get-universal-time)))
(defparameter *queue2* (format nil "processing-queue-2-~a" (get-universal-time)))
(defparameter *queue3* (format nil "processing-queue-3-~a" (get-universal-time)))
(defparameter *queue4* (format nil "processing-queue-4-~a" (get-universal-time)))
(defparameter *queue5* (format nil "processing-queue-5-~a" (get-universal-time)))

(defparameter *recipe1* (build-rmq-node-recipe :test1 *fn1* *queue1* *queue2*
                                               (calculate-batch-size)))
(defparameter *recipe2* (build-rmq-node-recipe :test2 *fn2* *queue2* *queue3*
                                               (calculate-batch-size)))
(defparameter *recipe3* (build-rmq-node-recipe :test3 *fn3* *queue3* *queue4*
                                               (calculate-batch-size)))
(defparameter *recipe4* (build-rmq-node-recipe :test4 *fn4* *queue4* *queue5*
                                               (calculate-batch-size)))
(defparameter *recipes* `(,*recipe1* ,*recipe2* ,*recipe3* ,*recipe4*))

(teardown
  (let ((conn (setup-connection :host *rmq-host*)))
    (with-channel (conn 1)
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
  (let ((work-node (make-rmq-node nil (format nil "worknode-~d" (get-universal-time))
                                  *queue1* *queue1* *queue1* :host *rmq-host*)))
    (startup work-node nil)
    (let ((msgs (iter:iterate
                  (iter:repeat *number-of-test-msgs*)
                  (iter:for msg = (generate-test-msg *length-of-test-msgs*))
                  (iter:collect msg)
                  (send-message work-node *queue1* msg))))
      (shutdown work-node)
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
         (work-node (make-rmq-node nil (format nil "worknode-~d" (get-universal-time))
                                   *queue5* *queue1* *queue1* :host *rmq-host*)))
    (format t "Ready to start tests?~%")
    (read-line)

    (add-recipe master *recipe1*)
    (add-recipe master *recipe2*)
    (add-recipe master *recipe3*)
    (add-recipe master *recipe4*)

    (iter:iterate
      (iter:for recipe in *recipes*)
      (iter:iterate
        (iter:repeat (1+ (random *threads-per-recipe-range*)))
        (ask-to-start-node master (symbol-name (node-recipe/type recipe)))))

    (sleep *process-time*)

    (iter:iterate
      (iter:for worker-id in (ghash-keys (master-workers master)))
      (ask-to-shutdown-worker master worker-id))
    (stop-master master)

    (startup work-node nil)
    (labels ((get-msg-w-restart ()
               (handler-case (get-message work-node)
                 (rabbitmq-error (c)
                   (declare (ignore c))
                   (sleep .1)
                   (get-msg-w-restart)))))
      (iter:iterate
        (iter:repeat *number-of-test-msgs*)
        (iter:for got = (get-msg-w-restart))
        (ok (member (rmq-message-body got) expected-result :test #'string=))
        (ack-message work-node got))
      (shutdown work-node))))
