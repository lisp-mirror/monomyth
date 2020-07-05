(defpackage monomyth/tests/rmq-node
  (:use :cl :prove :monomyth/node :monomyth/rmq-node :cl-mock :cl-rabbit))
(in-package :monomyth/tests/rmq-node)

(plan nil)

(vom:config t :info)
(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))
(defparameter *fail-queue* (format nil "test-fail-~d" (get-universal-time)))
(defvar *node* (make-rmq-node nil (format nil "test-rmq-node-~d" (get-universal-time))
                              *source-queue* *dest-queue* *fail-queue*
                              :host (uiop:getenv "TEST_RMQ")))
(defvar *checking-node*
  (make-rmq-node nil (format nil "test-rmq-node-1-~d" (get-universal-time))
                 *dest-queue* *source-queue* *fail-queue*
                 :batch-size 10 :host (uiop:getenv "TEST_RMQ")))
(startup *node* nil)
(startup *checking-node* nil)

(subtest "test-full-message-path"
  (let ((test-msg (format nil "test-~d" (get-universal-time))))
    (is (send-message *checking-node* *source-queue* test-msg) :amqp-status-ok)
    (sleep .1)
    (let ((got-msg (get-message *node*)))
      (is (rmq-message-body got-msg) test-msg)
      (ack-message *node* got-msg))))

(subtest "get-message-timeout"
  (is-error (get-message *node*) 'rabbitmq-library-error))

(subtest "nack works as expected (requeue)"
  (let ((test-msg (format nil "test-~d" (get-universal-time))))
    (is (send-message *checking-node* *source-queue* test-msg) :amqp-status-ok)
    (sleep .1)
    (let ((got-msg (get-message *node*)))
      (is (rmq-message-body got-msg) test-msg)
      (is (nack-message *node* got-msg t) :amqp-status-ok))
    (sleep .1)

    (let ((got-msg (get-message *node*)))
      (is (rmq-message-body got-msg) test-msg)
      (is (nack-message *node* got-msg nil) :amqp-status-ok))
    (is-error (get-message *node*) 'rabbitmq-library-error)))

(subtest "nack works as expected (no requeue)"
  (let ((test-msg (format nil "test-~d" (get-universal-time))))
    (is (send-message *checking-node* *source-queue* test-msg) :amqp-status-ok)
    (sleep .1)

    (let ((got-msg (get-message *node*)))
      (is (rmq-message-body got-msg) test-msg)
      (is (nack-message *node* got-msg nil) :amqp-status-ok))
    (is-error (get-message *node*) 'rabbitmq-library-error)))

(shutdown *node*)
(setf *node*
      (make-rmq-node #'(lambda (x) (format nil "test ~a" x))
                     (format nil "test-rmq-node-~d" (get-universal-time))
                     *source-queue* *dest-queue* *fail-queue*
                     :batch-size 10 :host (uiop:getenv "TEST_RMQ")))
(startup *node* nil)

(subtest "pull-messages-gets-full-batch"
  (iter:iterate
    (iter:repeat 10)
    (send-message *checking-node* *source-queue* "testing"))
  (sleep .1)

  (let ((items (pull-items *node*)))
    (is (length items) 10)
    (iter:iterate
      (iter:for item in items)
      (ack-message *node* item))))


(subtest "pull-messages-gets-partial"
  (iter:iterate
    (iter:repeat 5)
    (send-message *checking-node* *source-queue* "testing"))
  (sleep .1)

  (let ((items (pull-items *node*)))
    (is (length items) 5)
    (iter:iterate
      (iter:for item in items)
      (ack-message *node* item))))

(subtest "transform-items success"
  (let ((items `("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))
    (iter:iterate
      (iter:for item in items)
      (send-message *checking-node* *source-queue* item))
    (sleep .1)

    (let ((new-items (transform-items *node* (pull-items *node*))))
      (is (length new-items) 10)
      (iter:iterate
        (iter:for expect in items)
        (iter:for got in new-items)
        (is (rmq-message-body got) (format nil "test ~a" expect))
        (ack-message *node* got)))))

(shutdown *node*)
(setf *node*
      (make-rmq-node #'(lambda (x) (declare (ignore x)) (error "test"))
                     (format nil "test-rmq-node-~d" (get-universal-time))
                     *source-queue* *dest-queue* *fail-queue*
                     :batch-size 10 :host (uiop:getenv "TEST_RMQ")))
(startup *node* nil)

(subtest "transform-items failure"
  (let ((items `("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))
    (iter:iterate
      (iter:for item in items)
      (send-message *checking-node* *source-queue* item))
    (sleep .1)

    (let ((got-items (pull-items *node*)))
      (handler-case (transform-items *node* got-items)
        (node-error (c)
          (is (node-error/step c) :transform)
          (is (length (node-error/items c)) (length items))
          (iter:iterate
            (iter:for expected in items)
            (iter:for got in (node-error/items c))
            (is (rmq-message-body got) expected)
            (ack-message *node* got)))
        (:no-error (res) (declare (ignore res))
          (fail "transform should not have succeeded"))))))

(shutdown *node*)
(shutdown *checking-node*)
(setf *node*
      (make-rmq-node #'(lambda (x) (format nil "test ~a" x))
                     (format nil "test-rmq-node-~d" (get-universal-time))
                     *source-queue* *dest-queue* *fail-queue*
                     :batch-size 10 :host (uiop:getenv "TEST_RMQ")))

(setf *checking-node*
      (make-rmq-node nil (format nil "test-rmq-node-1-~d" (get-universal-time))
                     *dest-queue* *source-queue* *fail-queue*
                     :batch-size 10 :host (uiop:getenv "TEST_RMQ")))
(startup *node* nil)
(startup *checking-node* nil)

(subtest "place items works"
  (let ((items `("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))
    (iter:iterate
      (iter:for item in items)
      (send-message *checking-node* *source-queue* item))
    (sleep .1)

    (let ((first-got-items (pull-items *node*)))
      (place-items *node* first-got-items)
      (sleep .1)

      (let ((second-got-items (pull-items *checking-node*)))
        (is (length second-got-items) (length first-got-items))
        (iter:iterate
          (iter:for expected in items)
          (iter:for got-first in first-got-items)
          (iter:for got-second in second-got-items)
          (is expected (rmq-message-body got-first))
          (is expected (rmq-message-body got-second))
          (ack-message *checking-node* got-second))))))

(skip 1 "place items handles put failure")

(skip 1 "place items handles ack failure")

(subtest "handle-failure-unexpected-step"
  (is-error (handle-failure *node* :bad nil) 'simple-error))

(shutdown *checking-node*)
(setf *checking-node*
      (make-rmq-node nil (format nil "test-rmq-node-1-~d" (get-universal-time))
                     *fail-queue* *dest-queue* *source-queue*
                     :batch-size 10 :host (uiop:getenv "TEST_RMQ")))
(startup *checking-node* nil)

(subtest "handle-failure-pull-step send successful"
  (iter:iterate
    (iter:for item in '("1" "2" "3" "4" "5"))
    (send-message *node* *source-queue* item))
  (sleep .1)

  (let ((got-items (pull-items *node*)))
    (is (length got-items) 5)
    (handle-failure *node* :pull got-items)
    (sleep .1)

    (let ((final-items (pull-items *checking-node*)))
      (is (length final-items) (length got-items))
      (iter:iterate
        (iter:for got-item in got-items)
        (iter:for final-item in final-items)
        (is (rmq-message-body final-item) (rmq-message-body got-item))
        (ack-message *checking-node* final-item)))))

(skip 1 "handle-failure-pull-step-send-unsuccessful")

(subtest "handle-failure-transform-step-send-successful"
  (iter:iterate
    (iter:for item in '("1" "2" "3" "4" "5"))
    (send-message *node* *source-queue* item))
  (sleep .1)

  (let ((got-items (pull-items *node*)))
    (is (length got-items) 5)
    (handle-failure *node* :transform got-items)
    (sleep .1)

    (let ((final-items (pull-items *checking-node*)))
      (is (length final-items) (length got-items))
      (iter:iterate
        (iter:for got-item in got-items)
        (iter:for final-item in final-items)
        (is (rmq-message-body final-item) (rmq-message-body got-item))
        (ack-message *checking-node* final-item)))))

(skip 1 "handle-failure-transform-step-send-unsuccessful")

(subtest "handle-failure-place-step-send-successful"
  (iter:iterate
    (iter:for item in '("1" "2" "3" "4" "5"))
    (send-message *node* *source-queue* item))
  (sleep .1)

  (let ((got-items (pull-items *node*)))
    (is (length got-items) 5)
    (handle-failure *node* :place got-items)
    (sleep .1)

    (let ((final-items (pull-items *checking-node*)))
      (is (length final-items) (length got-items))
      (iter:iterate
        (iter:for got-item in got-items)
        (iter:for final-item in final-items)
        (is (rmq-message-body final-item) (rmq-message-body got-item))
        (ack-message *checking-node* final-item)))))

(skip 1 "handle-failure-place-step-send-unsuccessful")

(shutdown *node*)
(shutdown *checking-node*)
(setf *node*
      (make-rmq-node #'(lambda (x) (format nil "test ~a" x))
                     (format nil "test-rmq-node-~d" (get-universal-time))
                     *source-queue* *dest-queue* *fail-queue*
                     :batch-size 1 :host (uiop:getenv "TEST_RMQ")))
(setf *checking-node*
      (make-rmq-node nil (format nil "test-rmq-node-1-~d" (get-universal-time))
                     *dest-queue* *source-queue* *fail-queue*
                     :batch-size 5 :host (uiop:getenv "TEST_RMQ")))
(startup *node* nil)
(startup *checking-node* nil)

(subtest "full node path - success"
  (let ((test-items '("1" "2" "3" "4" "5")))
    (iter:iterate
      (iter:for item in test-items)
      (send-message *checking-node* *source-queue* item))
    (sleep .1)

    (iter:iterate
      (iter:repeat 5)
      (run-iteration *node*))
    (sleep .1)

    (let* ((got-items (pull-items *checking-node*)))
      (is (length test-items) (length got-items))
      (iter:iterate
        (iter:for test-item in test-items)
        (iter:for got-item in got-items)
        (is (format nil "test ~a" test-item) (rmq-message-body got-item))
        (ack-message *checking-node* got-item)))))

(shutdown *checking-node*)
(setf *checking-node*
      (make-rmq-node nil (format nil "test-rmq-node-1-~d" (get-universal-time))
                     *fail-queue* *source-queue* *dest-queue*
                     :batch-size 5 :host (uiop:getenv "TEST_RMQ")))
(startup *checking-node* nil)

(subtest "full node path - pull fail"
  (iter:iterate
    (iter:repeat 5)
    (with-mocks ()
      (answer (pull-items _)
        (error 'node-error :message "test" :step :pull))
      (run-iteration *node*)))

  (is (pull-items *checking-node*) nil))

(subtest "full node path - transform fail"
  (let ((test-items '("1" "2" "3" "4" "5")))
    (iter:iterate
      (iter:for item in test-items)
      (send-message *checking-node* *source-queue* item))
    (sleep .1)

    (iter:iterate
      (iter:repeat 5)
      (with-mocks ()
        (answer (transform-items _ items)
          (error 'node-error :message "test" :items items :step :transform))
        (run-iteration *node*)))
    (sleep .1)

    (let ((got-items (pull-items *checking-node*)))
      (is (length got-items) (length test-items))
      (iter:iterate
        (iter:for test-item in test-items)
        (iter:for got-item in got-items)
        (is test-item (rmq-message-body got-item))
        (ack-message *checking-node* got-item)))))

(subtest "full node path - place fail"
  (let ((test-items '("1" "2" "3" "4" "5")))
    (iter:iterate
      (iter:for item in test-items)
      (send-message *checking-node* *source-queue* item))
    (sleep .1)

    (iter:iterate
      (iter:repeat 5)
      (with-mocks ()
        (answer (place-items _ items)
          (error 'node-error :message "test" :items items :step :place))
        (run-iteration *node*)))
    (sleep .1)

    (let ((got-items (pull-items *checking-node*)))
      (is (length got-items) (length test-items))
      (iter:iterate
        (iter:for test-item in test-items)
        (iter:for got-item in got-items)
        (is (format nil "test ~a" test-item) (rmq-message-body got-item))
        (ack-message *checking-node* got-item)))))

(shutdown *checking-node*)
(defparameter *final-queue* (format nil "test-final-~d" (get-universal-time)))
(defvar *second-node*
  (make-rmq-node #'(lambda (x) (format nil "test1 ~a" x))
                 (format nil "test-rmq-node-2-~d" (get-universal-time))
                 *dest-queue* *final-queue* *fail-queue*
                 :batch-size 1 :host (uiop:getenv "TEST_RMQ")))
(setf *checking-node*
      (make-rmq-node nil (format nil "test-rmq-node-1-~d" (get-universal-time))
                     *final-queue* *source-queue* *dest-queue*
                     :batch-size 5 :host (uiop:getenv "TEST_RMQ")))
(startup *second-node* nil)
(startup *checking-node* nil)

(subtest "full node path - success - two nodes"
  (let ((test-items '("1" "2" "3" "4" "5")))
    (iter:iterate
      (iter:for item in test-items)
      (send-message *checking-node* *source-queue* item))
    (sleep .1)

    (iter:iterate
      (iter:repeat 5)
      (iter:for i upfrom 0)
      (run-iteration *node*)
      (diag (format nil "first iter: ~d" i))
      (sleep .1)
      (run-iteration *second-node*)
      (diag (format nil "second iter: ~d" i))
      (sleep .1))

    (let ((got-items (pull-items *checking-node*)))
      (is (length got-items) (length test-items))
      (iter:iterate
        (iter:for test-item in test-items)
        (iter:for got-item in got-items)
        (is (format nil "test1 test ~a" test-item) (rmq-message-body got-item))
        (ack-message *checking-node* got-item)))))

(defvar delete-conn (setup-connection :host (uiop:getenv "TEST_RMQ")))
(with-channel (delete-conn 1)
  (queue-delete delete-conn 1 *source-queue*)
  (queue-delete delete-conn 1 *dest-queue*)
  (queue-delete delete-conn 1 *final-queue*)
  (queue-delete delete-conn 1 *fail-queue*))
(destroy-connection delete-conn)
(shutdown *node*)
(shutdown *second-node*)
(shutdown *checking-node*)

(finalize)
