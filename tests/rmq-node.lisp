(defpackage monomyth/tests/rmq-node
  (:use :cl :rove :monomyth/node :monomyth/rmq-node :cl-mock :cl-rabbit :stmx
        :monomyth/tests/utils)
  (:shadow :closer-mop))
(in-package :monomyth/tests/rmq-node)

(v:output-here *terminal-io*)
(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))
(defparameter *fail-queue* (format nil "test-fail-~d" (get-universal-time)))
(defparameter *final-queue* (format nil "test-final-~d" (get-universal-time)))
(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))

(teardown
  (let ((conn (setup-connection :host *rmq-host*)))
    (with-channel (conn 1)
      (queue-delete conn 1 *source-queue*)
      (queue-delete conn 1 *dest-queue*)
      (queue-delete conn 1 *final-queue*)
      (queue-delete conn 1 *fail-queue*))
    (destroy-connection conn)))

(deftest test-full-message
  (testing "happy path"
    (let ((pulling-node
            (build-test-node (format nil "test-rmq-node-~d" (get-universal-time))
                             *source-queue* *dest-queue* *fail-queue* 1 *rmq-host*))
          (sending-node
            (build-test-node (format nil "test-rmq-node-1-~d" (get-universal-time))
                             *dest-queue* *source-queue* *fail-queue* 10 *rmq-host*))
          (test-msg (format nil "test-~d" (get-universal-time))))
      (startup pulling-node nil)
      (startup sending-node nil)

      (ok (eql (send-message sending-node *source-queue* test-msg) :amqp-status-ok))
      (sleep .1)
      (let ((got-msg (get-message pulling-node)))
        (ok (equal (rmq-message-body got-msg) test-msg))
        (ack-message pulling-node got-msg))

      (shutdown pulling-node)
      (shutdown sending-node)))

  (testing "timeout"
    (let ((pulling-node
            (build-test-node (format nil "test-rmq-node-~d" (get-universal-time))
                             *source-queue* *dest-queue* *fail-queue* 1 *rmq-host*)))
      (startup pulling-node nil)
      (ok (signals (get-message pulling-node) 'rabbitmq-library-error))
      (shutdown pulling-node))))

(deftest nack
  (let ((pulling-node
          (build-test-node (format nil "test-rmq-node-~d" (get-universal-time))
                           *source-queue* *dest-queue* *fail-queue* 1 *rmq-host*))
        (sending-node
          (build-test-node (format nil "test-rmq-node-1-~d" (get-universal-time))
                           *dest-queue* *source-queue* *fail-queue* 10 *rmq-host*)))
    (startup pulling-node nil)
    (startup sending-node nil)

    (testing "requeue"
      (let ((test-msg (format nil "test-~d" (get-universal-time))))
        (ok (eql (send-message sending-node *source-queue* test-msg) :amqp-status-ok))
        (sleep .1)
        (let ((got-msg (get-message pulling-node)))
          (ok (equal (rmq-message-body got-msg) test-msg))
          (ok (eql (nack-message pulling-node got-msg t) :amqp-status-ok)))
        (sleep .1)

        (let ((got-msg (get-message pulling-node)))
          (ok (equal (rmq-message-body got-msg) test-msg))
          (ok (eql (nack-message pulling-node got-msg nil) :amqp-status-ok)))
        (ok (signals (get-message pulling-node) 'rabbitmq-library-error))))

    (testing "no requeue"
      (let ((test-msg (format nil "test-~d" (get-universal-time))))
        (ok (eql (send-message sending-node *source-queue* test-msg) :amqp-status-ok))
        (sleep .1)

        (let ((got-msg (get-message pulling-node)))
          (ok (equal (rmq-message-body got-msg) test-msg))
          (ok (eql (nack-message pulling-node got-msg nil) :amqp-status-ok)))
        (ok (signals (get-message pulling-node) 'rabbitmq-library-error))))

    (shutdown pulling-node)
    (shutdown sending-node)))

(deftest pull-messages
  (let ((pulling-node
          (build-test-node (format nil "test-rmq-node-~d" (get-universal-time))
                           *source-queue* *dest-queue* *fail-queue* 10 *rmq-host*))
        (sending-node
          (build-test-node (format nil "test-rmq-node-1-~d" (get-universal-time))
                           *dest-queue* *source-queue* *fail-queue* 10 *rmq-host*)))
    (startup pulling-node nil)
    (startup sending-node nil)

    (testing "full batch"
      (iter:iterate
        (iter:repeat 10)
        (send-message sending-node *source-queue* "testing"))
      (sleep .1)

      (let ((items (pull-items pulling-node)))
        (ok (= (length items) 10))
        (iter:iterate
          (iter:for item in items)
          (ack-message pulling-node item))))

    (testing "partial"
      (iter:iterate
        (iter:repeat 5)
        (send-message sending-node *source-queue* "testing"))
      (sleep .1)

      (let ((items (pull-items pulling-node)))
        (ok (= (length items) 5))
        (iter:iterate
          (iter:for item in items)
          (ack-message pulling-node item))))

    (testing "timeout"
      (ok (eql (pull-items pulling-node) nil)))

    (shutdown pulling-node)
    (shutdown sending-node)))

(deftest transform-items-success
  (let ((pulling-node
          (build-test-node (format nil "test-rmq-node-~d" (get-universal-time))
                           *source-queue* *dest-queue* *fail-queue* 10 *rmq-host*))
        (sending-node
          (build-test-node (format nil "test-rmq-node-1-~d" (get-universal-time))
                           *dest-queue* *source-queue* *fail-queue* 10 *rmq-host*))
        (items '("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))
    (startup pulling-node nil)
    (startup sending-node nil)

    (iter:iterate
      (iter:for item in items)
      (send-message sending-node *source-queue* item))
    (sleep .1)

    (let ((new-items (transform-items pulling-node (pull-items pulling-node))))
      (ok (= (length new-items) 10))
      (iter:iterate
        (iter:for expect in items)
        (iter:for got in new-items)
        (ok (string= (rmq-message-body got) (format nil "test ~a" expect)))
        (ack-message pulling-node got)))

    (shutdown pulling-node)
    (shutdown sending-node)))

(transactional
    (defclass failing-node (rmq-node) ()))

(defun build-fail-node (name source dest fail size)
  (make-instance 'failing-node :name name :source source :dest dest :fail fail
                               :host *rmq-host* :batch-size size :type :test
                               :conn (setup-connection :host *rmq-host*)))

(defmethod transform-fn ((node failing-node) item)
  (error "test"))

(deftest transform-items-failure
  (let ((pulling-node
          (build-fail-node (format nil "test-rmq-node-~d" (get-universal-time))
                           *source-queue* *dest-queue* *fail-queue* 10))
        (sending-node
          (build-test-node (format nil "test-rmq-node-1-~d" (get-universal-time))
                           *fail-queue* *source-queue* *fail-queue* 10 *rmq-host*))
        (items `("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))
    (startup pulling-node nil)
    (startup sending-node nil)

    (iter:iterate
      (iter:for item in items)
      (send-message sending-node *source-queue* item))
    (sleep .1)

    (let ((got-items (pull-items pulling-node)))
      (handler-case (transform-items pulling-node got-items)
        (node-error (c)
          (ok (eql (node-error/step c) :transform))
          (ok (= (length (node-error/items c)) (length items)))
          (iter:iterate
            (iter:for expected in items)
            (iter:for got in (node-error/items c))
            (ok (string= (rmq-message-body got) expected))
            (ack-message pulling-node got)))
        (:no-error (res) (declare (ignore res))
          (fail "transform should not have succeeded"))))

    (shutdown pulling-node)
    (shutdown sending-node)))

(deftest place-items
  (testing "happy path"
    (let ((pulling-node
            (build-test-node (format nil "test-rmq-node-~d" (get-universal-time))
                             *source-queue* *dest-queue* *fail-queue* 10 *rmq-host*))
          (sending-node
            (build-test-node (format nil "test-rmq-node-1-~d" (get-universal-time))
                             *dest-queue* *source-queue* *fail-queue* 10 *rmq-host*))
          (items `("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))
      (startup pulling-node nil)
      (startup sending-node nil)

      (iter:iterate
        (iter:for item in items)
        (send-message sending-node *source-queue* item))
      (sleep .1)

      (let ((first-got-items (pull-items pulling-node)))
        (place-items pulling-node first-got-items)
        (sleep .1)

        (let ((second-got-items (pull-items sending-node)))
          (ok (= (length second-got-items) (length first-got-items)))
          (iter:iterate
            (iter:for expected in items)
            (iter:for got-first in first-got-items)
            (iter:for got-second in second-got-items)
            (ok (string= expected (rmq-message-body got-first)))
            (ok (string= expected (rmq-message-body got-second)))
            (ack-message sending-node got-second))))

      (shutdown pulling-node)
      (shutdown sending-node))

    (skip "put failure")

    (skip "ack failure")))

(defmacro test-handle-failure (step-name step)
  `(testing ,(format nil "~a send successful" step-name)
     (iter:iterate
       (iter:for item in '("1" "2" "3" "4" "5"))
       (send-message sending-node *source-queue* item))
     (sleep .1)

     (let ((got-items (pull-items pulling-node)))
       (ok (= (length got-items) 5))
       (handle-failure pulling-node ,step got-items)
       (sleep .1)

       (let ((final-items (pull-items sending-node)))
         (ok (= (length final-items) (length got-items)))
         (iter:iterate
           (iter:for got-item in got-items)
           (iter:for final-item in final-items)
           (ok (string= (rmq-message-body final-item) (rmq-message-body got-item)))
           (ack-message sending-node final-item))))))

(deftest handle-failure
  (let ((pulling-node
          (build-test-node (format nil "test-rmq-node-~d" (get-universal-time))
                           *source-queue* *dest-queue* *fail-queue* 5 *rmq-host*))
        (sending-node
          (build-test-node (format nil "test-rmq-node-1-~d" (get-universal-time))
                           *fail-queue* *source-queue* *fail-queue* 10 *rmq-host*)))
    (startup pulling-node nil)
    (startup sending-node nil)

    (testing "unexpected step"
      (ok (signals (handle-failure pulling-node :bad nil) 'simple-error)))

    (test-handle-failure "pull-step" :pull)

    (skip "pull-step send unsuccessful")

    (test-handle-failure "transform-step" :transform)

    (skip "transform-step send unsuccessful")

    (test-handle-failure "place-step" :place)

    (skip "place-step send unsuccessful")

    (shutdown pulling-node)
    (shutdown sending-node)))

(deftest full-node-path-success
  (let ((pulling-node
          (build-test-node (format nil "test-rmq-node-~d" (get-universal-time))
                           *source-queue* *dest-queue* *fail-queue* 5 *rmq-host*))
        (sending-node
          (build-test-node (format nil "test-rmq-node-1-~d" (get-universal-time))
                           *dest-queue* *source-queue* *fail-queue* 10 *rmq-host*))
        (test-items '("1" "2" "3" "4" "5")))
    (startup pulling-node nil)
    (startup sending-node nil)

    (iter:iterate
      (iter:for item in test-items)
      (send-message sending-node *source-queue* item))
    (sleep .1)

    (iter:iterate
      (iter:repeat 5)
      (run-iteration pulling-node))
    (sleep .1)

    (let* ((got-items (pull-items sending-node)))
      (ok (= (length test-items) (length got-items)))
      (iter:iterate
        (iter:for test-item in test-items)
        (iter:for got-item in got-items)
        (ok (string= (format nil "test ~a" test-item) (rmq-message-body got-item)))
        (ack-message sending-node got-item)))

    (shutdown pulling-node)
    (shutdown sending-node)))

(deftest full-node-path-failures
  (let ((pulling-node
          (build-test-node (format nil "test-rmq-node-~d" (get-universal-time))
                           *source-queue* *dest-queue* *fail-queue* 5 *rmq-host*))
        (sending-node
          (build-test-node (format nil "test-rmq-node-1-~d" (get-universal-time))
                           *fail-queue* *source-queue* *fail-queue* 10 *rmq-host*)))
    (startup pulling-node nil)
    (startup sending-node nil)

    (testing "pull fail"
      (iter:iterate
        (iter:repeat 5)
        (with-mocks ()
          (answer (pull-items _)
            (error 'node-error :message "test" :step :pull))
          (run-iteration pulling-node)))

      (ok (eql (pull-items pulling-node) nil)))

    (testing "transform fail"
      (let ((test-items '("1" "2" "3" "4" "5")))
        (iter:iterate
          (iter:for item in test-items)
          (send-message sending-node *source-queue* item))
        (sleep .1)

        (iter:iterate
          (iter:repeat 5)
          (with-mocks ()
            (answer (transform-items _ items)
              (error 'node-error :message "test" :items items :step :transform))
            (run-iteration pulling-node)))
        (sleep .1)

        (let ((got-items (pull-items sending-node)))
          (ok (= (length got-items) (length test-items)))
          (iter:iterate
            (iter:for test-item in test-items)
            (iter:for got-item in got-items)
            (ok (string= test-item (rmq-message-body got-item)))
            (ack-message sending-node got-item)))))

    (testing "place fail"
      (let ((test-items '("1" "2" "3" "4" "5")))
        (iter:iterate
          (iter:for item in test-items)
          (send-message sending-node *source-queue* item))
        (sleep .1)

        (iter:iterate
          (iter:repeat 5)
          (with-mocks ()
            (answer (place-items _ items)
              (error 'node-error :message "test" :items items :step :place))
            (run-iteration pulling-node)))
        (sleep .1)

        (let ((got-items (pull-items sending-node)))
          (ok (= (length got-items) (length test-items)))
          (iter:iterate
            (iter:for test-item in test-items)
            (iter:for got-item in got-items)
            (ok (string= (format nil "test ~a" test-item) (rmq-message-body got-item)))
            (ack-message sending-node got-item)))))

    (shutdown pulling-node)
    (shutdown sending-node)))

(deftest full-node-path-success-two-nodes
  (let ((pulling-node
          (build-test-node (format nil "test-rmq-node-~d" (get-universal-time))
                           *source-queue* *dest-queue* *fail-queue* 1 *rmq-host*))
        (second-node
          (build-test-node1 (format nil "test-rmq-node-2-~d" (get-universal-time))
                            *dest-queue* *final-queue* *fail-queue* 1 *rmq-host*))
        (sending-node
          (build-test-node (format nil "test-rmq-node-1-~d" (get-universal-time))
                           *final-queue* *source-queue* *fail-queue* 10 *rmq-host*))
        (test-items '("1" "2" "3" "4" "5")))
    (startup pulling-node nil)
    (startup sending-node nil)
    (startup second-node nil)

    (iter:iterate
      (iter:for item in test-items)
      (send-message sending-node *source-queue* item))
    (sleep .1)

    (iter:iterate
      (iter:repeat 5)
      (iter:for i upfrom 0)
      (run-iteration pulling-node)
      (diag (format nil "first iter: ~d~%" i))
      (sleep .1)
      (run-iteration second-node)
      (diag (format nil "second iter: ~d~%" i))
      (sleep .1))

    (let ((got-items (pull-items sending-node)))
      (ok (= (length got-items) (length test-items)))
      (iter:iterate
        (iter:for test-item in test-items)
        (iter:for got-item in got-items)
        (ok (string= (format nil "test1 test ~a" test-item) (rmq-message-body got-item)))
        (ack-message sending-node got-item)))

    (shutdown second-node)
    (shutdown pulling-node)
    (shutdown sending-node)))
