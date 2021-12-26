(defpackage monomyth/tests/rmq-node
  (:use :cl :rove :monomyth/node :monomyth/rmq-node :cl-mock :cl-rabbit :stmx
        :monomyth/tests/utils :monomyth/dsl)
  (:shadow :closer-mop))
(in-package :monomyth/tests/rmq-node)

(v:output-here *terminal-io*)
(defparameter *fail-queue* (format nil "test-fail-~d" (get-universal-time)))
(defvar *test-context*)

(setup
  (setf *test-context* (pzmq:ctx-new)))

(teardown
  (pzmq:ctx-destroy *test-context*)
  (let ((conn (setup-connection :host *rmq-host* :username *rmq-user* :password *rmq-pass*)))
    (with-channel (conn 1)
      (queue-delete conn 1 *source-queue*)
      (queue-delete conn 1 *dest-queue*)
      (queue-delete conn 1 *full-queue1*)
      (queue-delete conn 1 *full-queue2*)
      (queue-delete conn 1 *full-queue3*)
      (queue-delete conn 1 *fail-queue*))
    (destroy-connection conn)))

(deftest test-full-message
  (testing "happy path"
    (let ((pulling-node
            (build-test-node
             (format nil "test-rmq-node-~d" (get-universal-time))
             *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
          (sending-node
            (build-work-node
             (format nil "test-rmq-node-1-~d" (get-universal-time))
             *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
          (test-msg (format nil "test-~d" (get-universal-time))))
      (start-node pulling-node *test-context* "inproc://test" nil)
      (start-node sending-node *test-context* "inproc://test" nil)

      (ok (eql (send-message sending-node *source-queue* test-msg) :amqp-status-ok))
      (sleep .1)
      (let ((got-msg (get-message pulling-node)))
        (ok (equal (rmq-message-body got-msg) test-msg))
        (ack-message pulling-node got-msg))

      (stop-node pulling-node)
      (stop-node sending-node)))

  (testing "timeout"
    (let ((pulling-node
            (build-test-node
             (format nil "test-rmq-node-~d" (get-universal-time))
             *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
      (start-node pulling-node *test-context* "inproc://test" nil)
      (ok (signals (get-message pulling-node) 'rabbitmq-library-error))
      (stop-node pulling-node))))

(deftest nack
  (let ((pulling-node
          (build-test-node
           (format nil "test-rmq-node-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (sending-node
          (build-work-node
           (format nil "test-rmq-node-1-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
    (start-node pulling-node *test-context* "inproc://test" nil)
    (start-node sending-node *test-context* "inproc://test" nil)

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

    (stop-node pulling-node)
    (stop-node sending-node)))

(deftest pull-messages
  (let ((pulling-node
          (build-large-test-node
           (format nil "test-rmq-node-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (sending-node
          (build-work-node
           (format nil "test-rmq-node-1-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
    (start-node pulling-node *test-context* "inproc://test" nil)
    (start-node sending-node *test-context* "inproc://test" nil)

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

    (stop-node pulling-node)
    (stop-node sending-node)))

(define-rmq-node dont-pull-test #'identity-fn 8 :dest-queue *dest-queue*)

(deftest dont-pull-messages
  (let ((sending-node
          (build-dont-pull-test
           (format nil "test-rmq-node-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (pulling-node
          (build-work-node
           (format nil "test-rmq-node-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))

    (start-node sending-node *test-context* "inproc://test" nil)
    (start-node pulling-node *test-context* "inproc://test" nil)

    (run-iteration sending-node)

    (sleep .1)

    (let ((items (pull-items pulling-node)))
      (ok (= (length items) (node/batch-size sending-node)))
      (iter:iterate
        (iter:for item in items)
        (ok (string= "STUB-ITEM" (rmq-message-body item)))
        (ack-message pulling-node item)))

    (stop-node pulling-node)
    (stop-node sending-node)))

(deftest transform-items-success
  (let ((pulling-node
          (build-large-test-node
           (format nil "test-rmq-node-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (sending-node
          (build-work-node
           (format nil "test-rmq-node-1-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (items '("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))
    (start-node pulling-node *test-context* "inproc://test" nil)
    (start-node sending-node *test-context* "inproc://test" nil)

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

    (stop-node pulling-node)
    (stop-node sending-node)))

(transactional
    (defclass failing-node (rmq-node) ()))

(defun build-fail-node (name source dest fail size)
  (make-instance 'failing-node :name name :source source :dest dest :fail fail
                               :host *rmq-host* :batch-size size :type :test
                               :conn (setup-connection :host *rmq-host*
                                                       :username *rmq-user*
                                                       :password *rmq-pass*)))

(defmethod transform-fn ((node failing-node) item)
  (error "test"))

(deftest transform-items-failure
  (let ((pulling-node
          (build-fail-node
           (format nil "test-rmq-node-~d" (get-universal-time))
           *source-queue* *dest-queue* *fail-queue* 10))
        (sending-node
          (build-work-node
           (format nil "test-rmq-node-1-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (items `("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))
    (start-node pulling-node *test-context* "inproc://test" nil)
    (start-node sending-node *test-context* "inproc://test" nil)

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

    (stop-node pulling-node)
    (stop-node sending-node)))

(deftest place-items
  (testing "happy path"
    (let ((pulling-node
            (build-large-test-node
             (format nil "test-rmq-node-~d" (get-universal-time))
             *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
          (sending-node
            (build-work-node
             (format nil "test-rmq-node-1-~d" (get-universal-time))
             *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
          (items `("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))
      (start-node pulling-node *test-context* "inproc://test" nil)
      (start-node sending-node *test-context* "inproc://test" nil)

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

      (stop-node pulling-node)
      (stop-node sending-node))

    (skip "put failure")

    (skip "ack failure")))

(deftest dont-place-items
  (let* ((items `("1" "2" "3" "4" "5" "6" "7" "8" "9" "10"))
         (i 0))
    (flet ((check-fn (node item)
             (declare (ignore node))
             (ok (string= item (nth i items)))
             (incf i)))
      (define-rmq-node no-place #'check-fn 10 :source-queue *source-queue*)
      (let ((sending-node
              (build-work-node
               (format nil "test-rmq-node-1-~d" (get-universal-time))
               *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
            (pulling-node
              (build-no-place "no place node" *fail-queue* :no-place
                              *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
        (start-node pulling-node *test-context* "inproc://test" nil)
        (start-node sending-node *test-context* "inproc://test" nil)

        (iter:iterate
          (iter:for item in items)
          (send-message sending-node *source-queue* item))
        (sleep .1)

        (run-iteration pulling-node)

        (ng (pull-items pulling-node))
        (ng (pull-items sending-node))

        (stop-node pulling-node)
        (stop-node sending-node)))))

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

(define-rmq-node fail-work-node nil 10 :source-queue *fail-queue* :dest-queue *source-queue*)

(deftest handle-failure
  (let ((pulling-node
          (build-large-test-node
           (format nil "test-rmq-node-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (sending-node
          (build-fail-work-node
           (format nil "test-rmq-node-1-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
    (start-node pulling-node *test-context* "inproc://test" nil)
    (start-node sending-node *test-context* "inproc://test" nil)

    (testing "unexpected step"
      (ok (signals (handle-failure pulling-node :bad nil) 'simple-error)))

    (test-handle-failure "pull-step" :pull)

    (skip "pull-step send unsuccessful")

    (test-handle-failure "transform-step" :transform)

    (skip "transform-step send unsuccessful")

    (test-handle-failure "place-step" :place)

    (skip "place-step send unsuccessful")

    (stop-node pulling-node)
    (stop-node sending-node)))

(deftest full-node-path-success
  (let ((test-items '("1" "2" "3" "4" "5"))
        (pulling-node
          (build-test-node
           (format nil "test-rmq-node-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (sending-node
          (build-work-node
           (format nil "test-rmq-node-1-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
    (start-node pulling-node *test-context* "inproc://test" nil)
    (start-node sending-node *test-context* "inproc://test" nil)

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

    (stop-node pulling-node)
    (stop-node sending-node)))

(deftest full-node-path-failures
  (let ((pulling-node
          (build-test-node
           (format nil "test-rmq-node-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (sending-node
          (build-fail-work-node
           (format nil "test-rmq-node-1-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*)))
    (start-node pulling-node *test-context* "inproc://test" nil)
    (start-node sending-node *test-context* "inproc://test" nil)

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

    (stop-node pulling-node)
    (stop-node sending-node)))

(defun fn (node item)
  (declare (ignore node))
  (format nil "test ~a" item))

(defun fn1 (node item)
  (declare (ignore node))
  (format nil "test1 ~a" item))

(define-system node ()
    (:name full-test-node1 :fn #'fn :batch-size 1)
    (:name full-test-node2 :fn #'fn1 :batch-size 1))

(defparameter *full-queue1* "START-to-FULL-TEST-NODE1")
(defparameter *full-queue2* "FULL-TEST-NODE1-to-FULL-TEST-NODE2")
(defparameter *full-queue3* "FULL-TEST-NODE2-to-END")

(define-rmq-node final-work-node nil 5 :source-queue *full-queue3* :dest-queue *full-queue1*)

(deftest full-node-path-success-two-nodes
  (let ((pulling-node
          (build-full-test-node1
           (format nil "test-rmq-node-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (second-node
          (build-full-test-node2
           (format nil "test-rmq-node-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (sending-node
          (build-final-work-node
           (format nil "test-rmq-node-1-~d" (get-universal-time))
           *fail-queue* :test *rmq-host* *rmq-port* *rmq-user* *rmq-pass*))
        (test-items '("1" "2" "3" "4" "5")))
    (start-node pulling-node *test-context* "inproc://test" nil)
    (start-node sending-node *test-context* "inproc://test" nil)
    (start-node second-node *test-context* "inproc://test" nil)

    (iter:iterate
      (iter:for item in test-items)
      (send-message sending-node *full-queue1* item))
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

    (stop-node second-node)
    (stop-node pulling-node)
    (stop-node sending-node)))
