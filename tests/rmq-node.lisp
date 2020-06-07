(defpackage monomyth/tests/rmq-node
  (:use :cl :prove :monomyth/node :monomyth/rmq-node :cl-mock))
(in-package :monomyth/tests/rmq-node)

(plan nil)

(vom:config t :info)
(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))
(defparameter *fail-queue* (format nil "test-fail-~d" (get-universal-time)))
(defparameter *conn* (getf (setup-connection) :conn))
(defvar *node* (make-rmq-node nil (format nil "test-rmq-node-~d" (get-universal-time))
                              *conn* 1 *source-queue* *dest-queue* *fail-queue*))
(startup *node*)

(subtest "test-full-message-path"
  (let ((test-msg (format nil "test-~d" (get-universal-time))))
    (ok (getf (send-message *node* *source-queue* test-msg) :success))
    (sleep 1)
    (let* ((got-msg (get-message *node*))
           (inner-msg (getf got-msg :result)))
      (ok (getf got-msg :success))
      (ok (not (getf got-msg :timeout)))
      (is (rmq-message-body inner-msg) test-msg)
      (ok (getf (ack-message *node* inner-msg) :success)))))

(subtest "get-message-handles-timeout"
  (let ((got-msg (get-message *node*)))
    (ok (getf got-msg :success))
    (ok (getf got-msg :timeout))))

(subtest "nack works as expected (requeue)"
  (let ((test-msg (format nil "test-~d" (get-universal-time))))
    (ok (getf (send-message *node* *source-queue* test-msg) :success))
    (sleep 1)
    (let* ((got-msg (get-message *node*))
           (inner-msg (getf got-msg :result)))
      (ok (getf got-msg :success))
      (ok (not (getf got-msg :timeout)))
      (is (rmq-message-body inner-msg) test-msg)
      (ok (getf (nack-message *node* inner-msg t) :success)))
    (sleep 1)
    (let* ((got-msg (get-message *node*))
           (inner-msg (getf got-msg :result)))
      (ok (getf got-msg :success))
      (ok (not (getf got-msg :timeout)))
      (is (rmq-message-body inner-msg) test-msg)
      (ok (getf (ack-message *node* inner-msg) :success)))))

(subtest "nack works as expected (no requeue)"
  (let ((test-msg (format nil "test-~d" (get-universal-time))))
    (ok (getf (send-message *node* *source-queue* test-msg) :success))
    (sleep 1)
    (let* ((got-msg (get-message *node*))
           (inner-msg (getf got-msg :result)))
      (ok (getf got-msg :success))
      (ok (not (getf got-msg :timeout)))
      (is (rmq-message-body inner-msg) test-msg)
      (ok (getf (nack-message *node* inner-msg nil) :success)))
    (sleep 1)
    (let* ((got-msg (get-message *node*)))
      (ok (getf got-msg :success))
      (ok (getf got-msg :timeout))
      (is (getf got-msg :items) nil))))

(shutdown *node*)
(setf *node* (make-rmq-node nil (format nil "test-rmq-node-~d" (get-universal-time))
                            *conn* 1 *source-queue* *dest-queue* *fail-queue* :batch-size 10))
(startup *node*)

(subtest "pull-messages-gets-full-batch"
  (iter:iterate
     (iter:repeat 10)
     (send-message *node* *source-queue* "testing"))

   (sleep 1)

   (let* ((result (pull-items *node*))
          (items (getf result :items)))
     (ok (getf result :success))
     (is (length items) 10)
     (iter:iterate
       (iter:for item in items)
       (ack-message *node* item))))

(subtest "pull-messages-gets-partial"
  (iter:iterate
    (iter:repeat 5)
    (send-message *node* *source-queue* "testing"))

  (sleep 1)

  (let* ((result (pull-items *node*))
         (items (getf result :items)))
    (ok (getf result :success))
    (is (length items) 5)
    (iter:iterate
      (iter:for item in items)
      (ack-message *node* item))))

(subtest "pull-messages-handles-error-list"
  (with-mocks ()
    (answer get-message '(:error "test"))
    (let ((result (pull-items *node*)))
      (ok (not (getf result :success)))
      (is (getf result :error) "test"))))

(defvar *checking-node* (make-rmq-node nil (format nil "test-rmq-node-1-~d" (get-universal-time))
                                       *conn* 2 *dest-queue* *source-queue* *fail-queue* :batch-size 10))
(startup *checking-node*)

(subtest "place items works"
  (let ((items `("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))

    (iter:iterate
      (iter:for item in items)
      (send-message *node* *source-queue* item))

    (sleep 1)

    (let ((first-got-items (pull-items *node*)))
      (ok (getf first-got-items :success))
      (ok (getf (place-items *node* first-got-items) :success))

      (sleep 1)

      (let ((second-got-items (pull-items *checking-node*)))
        (ok (getf second-got-items :success))
        (is (length (getf second-got-items :items)) (length (getf first-got-items :items)))
        (iter:iterate
          (iter:for expected in items)
          (iter:for got-first in (getf second-got-items :items))
          (iter:for got-second in (getf first-got-items :items))
          (string= expected (rmq-message-body got-first))
          (string= expected (rmq-message-body got-second)))

        (iter:iterate
          (iter:for item in (getf second-got-items :items))
          (ack-message *checking-node* item))))))

(subtest "place items handles put failure"
  (let ((items `("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))

    (iter:iterate
      (iter:for item in items)
      (send-message *node* *source-queue* item))

    (sleep 1)

    (let ((got-items (pull-items *node*)))
      (ok (getf got-items :success))
      (with-mocks ()
        (answer send-message '(:success nil))
        (let ((res (place-items *node* got-items)))
          (ok (not (getf res :success)))
          (is (getf res :items) (getf got-items :items))))

      (iter:iterate
        (iter:for item in (getf got-items :items))
        (ack-message *node* item)))))

(subtest "place items handles ack failure"
  (let ((items `("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))

    (iter:iterate
      (iter:for item in items)
      (send-message *node* *source-queue* item))

    (sleep 1)

    (let ((got-items (pull-items *node*)))
      (ok (getf got-items :success))
      (with-mocks ()
        (answer send-message '(:success t))
        (answer ack-message '(:success nil))
        (let ((res (place-items *node* got-items)))
          (ok (not (getf res :success)))
          (is (getf res :items) (getf got-items :items))))

      (iter:iterate
        (iter:for item in (getf got-items :items))
        (ack-message *node* item)))))

(subtest "handle-failure-unexpected-step"
  (is-error (handle-failure *node* :bad nil) 'simple-error))

(subtest "handle-failure-pull-step"
  (is (handle-failure *node* :pull '(:error "test")) '(:error "test")))

(shutdown *checking-node*)
(setf *checking-node* (make-rmq-node nil (format nil "test-rmq-node-1-~d" (get-universal-time))
                                     *conn* 2 *fail-queue* *dest-queue* *source-queue* :batch-size 10))
(startup *checking-node*)

(subtest "handle-failure-transform-step-send-successful"
  (iter:iterate
    (iter:for item in '("1" "2" "3" "4" "5"))
    (send-message *node* *source-queue* item))

  (sleep 1)

  (let ((got-items (pull-items *node*)))
    (ok (getf got-items :success))
    (is (length (getf got-items :items)) 5)
    (setf got-items (append '(:error "test") got-items))
    (is (handle-failure *node* :transform got-items)
        got-items)

    (sleep 1)

    (let ((final-items (pull-items *checking-node*)))
      (ok (getf final-items :success))
      (ok (not (getf final-items :timeout)))
      (is (length (getf final-items :items)) (length (getf got-items :items)))

      (iter:iterate
        (iter:for got-item in (getf got-items :items))
        (iter:for final-item in (getf final-items :items))
        (is (rmq-message-body final-item) (rmq-message-body got-item))
        (ack-message *checking-node* final-item)))))

(subtest "handle-failure-transform-step-send-unsuccessful"
  (iter:iterate
    (iter:for item in '("1" "2" "3" "4" "5"))
    (send-message *node* *source-queue* item))

  (sleep 1)

  (let ((got-items (pull-items *node*)))
    (ok (getf got-items :success))
    (is (length (getf got-items :items)) 5)
    (setf got-items (append '(:error "test") got-items))
    (with-mocks ()
      (answer send-message '(:error "test"))
      (is (handle-failure *node* :transform got-items)
          got-items))

    (sleep 1)

    (let ((final-items (pull-items *node*)))
      (ok (getf final-items :success))
      (ok (not (getf final-items :timeout)))
      (is (length (getf final-items :items)) (length (getf got-items :items)))

      (iter:iterate
        (iter:for got-item in (getf got-items :items))
        (iter:for final-item in (getf final-items :items))
        (is (rmq-message-body final-item) (rmq-message-body got-item))
        (ack-message *node* final-item)))))

(subtest "handle-failure-place-step-send-successful"
  (iter:iterate
    (iter:for item in '("1" "2" "3" "4" "5"))
    (send-message *node* *source-queue* item))

  (sleep 1)

  (let ((got-items (pull-items *node*)))
    (ok (getf got-items :success))
    (is (length (getf got-items :items)) 5)
    (setf got-items (append '(:error "test") got-items))
    (is (handle-failure *node* :place got-items)
        got-items)

    (sleep 1)

    (let ((final-items (pull-items *checking-node*)))
      (ok (getf final-items :success))
      (ok (not (getf final-items :timeout)))
      (is (length (getf final-items :items)) (length (getf got-items :items)))

      (iter:iterate
        (iter:for got-item in (getf got-items :items))
        (iter:for final-item in (getf final-items :items))
        (is (rmq-message-body final-item) (rmq-message-body got-item))
        (ack-message *checking-node* final-item)))))

(subtest "handle-failure-place-step-send-unsuccessful"
  (iter:iterate
    (iter:for item in '("1" "2" "3" "4" "5"))
    (send-message *node* *source-queue* item))

  (sleep 1)

  (let ((got-items (pull-items *node*)))
    (ok (getf got-items :success))
    (is (length (getf got-items :items)) 5)
    (setf got-items (append '(:error "test") got-items))
    (with-mocks ()
      (answer send-message '(:error "test"))
      (is (handle-failure *node* :place got-items)
          got-items))

    (sleep 1)

    (let ((final-items (pull-items *node*)))
      (ok (getf final-items :success))
      (ok (not (getf final-items :timeout)))
      (is (length (getf final-items :items)) (length (getf got-items :items)))

      (iter:iterate
        (iter:for got-item in (getf got-items :items))
        (iter:for final-item in (getf final-items :items))
        (is (rmq-message-body final-item) (rmq-message-body got-item))
        (ack-message *node* final-item)))))

(shutdown *node*)
(shutdown *checking-node*)
(setf *node* (make-rmq-node #'(lambda (x) (build-rmq-message
                                           :body (format nil "test ~a" (rmq-message-body x))
                                           :delivery-tag (rmq-message-delivery-tag x)))
                            (format nil "test-rmq-node-~d" (get-universal-time))
                            *conn* 1 *source-queue* *dest-queue* *fail-queue* :batch-size 1))
(setf *checking-node* (make-rmq-node nil (format nil "test-rmq-node-1-~d" (get-universal-time))
                                     *conn* 2 *dest-queue* *source-queue* *fail-queue* :batch-size 5))
(startup *node*)
(startup *checking-node*)

(subtest "full node path - success"
  (let ((test-items '("1" "2" "3" "4" "5")))
    (iter:iterate
      (iter:for item in test-items)
      (send-message *node* *source-queue* item))

    (sleep 1)

    (iter:iterate
      (iter:repeat 5)
      (ok (getf (run-iteration *node*) :success)))

    (sleep 1)

    (let* ((got-items (pull-items *checking-node*))
           (inner-got-items (getf got-items :items)))
      (ok (getf got-items :success))
      (iter:iterate
        (iter:for test-item in test-items)
        (iter:for got-item in inner-got-items)
        (is (format nil "test ~a" test-item) (rmq-message-body got-item))
        (ack-message *checking-node* got-item)))))

(subtest "full node path - pull fail"
  (let ((res '(:error "test")))
    (with-mocks ()
      (answer pull-items res)
      (is (run-iteration *node*) res))))

(shutdown *checking-node*)
(setf *checking-node* (make-rmq-node nil (format nil "test-rmq-node-1-~d" (get-universal-time))
                                     *conn* 2 *fail-queue* *source-queue* *dest-queue* :batch-size 5))
(startup *checking-node*)

(subtest "full node path - transform fail"
  (let ((test-items '("1" "2" "3" "4" "5")))
    (iter:iterate
      (iter:for item in test-items)
      (send-message *node* *source-queue* item))

    (sleep 1)

    (iter:iterate
      (iter:repeat 5)
      (with-mocks ()
        (answer (transform-items _ items)
          `(:error "test" :items ,(getf items :items)))
        (ok (not (getf (run-iteration *node*) :success)))))

    (sleep 1)

    (let* ((got-items (pull-items *checking-node*))
           (inner-got-items (getf got-items :items)))
      (ok (getf got-items :success))
      (is (length inner-got-items) (length test-items))
      (iter:iterate
        (iter:for test-item in test-items)
        (iter:for got-item in inner-got-items)
        (is test-item (rmq-message-body got-item))
        (ack-message *checking-node* got-item)))))

(subtest "full node path - place fail"
  (let ((test-items '("1" "2" "3" "4" "5")))
    (iter:iterate
      (iter:for item in test-items)
      (send-message *node* *source-queue* item))

    (sleep 1)

    (iter:iterate
      (iter:repeat 5)
      (with-mocks ()
        (answer (place-items _ items)
          `(:error "test" :items ,(getf items :items)))
        (ok (not (getf (run-iteration *node*) :success)))))

    (sleep 1)

    (let* ((got-items (pull-items *checking-node*))
           (inner-got-items (getf got-items :items)))
      (ok (getf got-items :success))
      (is (length inner-got-items) (length test-items))
      (iter:iterate
        (iter:for test-item in test-items)
        (iter:for got-item in inner-got-items)
        (is (format nil "test ~a" test-item) (rmq-message-body got-item))
        (ack-message *checking-node* got-item)))))

(delete-queue *node* *source-queue*)
(delete-queue *node* *dest-queue*)
(delete-queue *node* *fail-queue*)
(shutdown *node*)
(shutdown *checking-node*)

(finalize)
