(defpackage monomyth/tests/rmq-worker
  (:use :cl :prove :cl-rabbit :monomyth/rmq-worker :monomyth/worker :monomyth/mmop
        :monomyth/rmq-node-recipe :monomyth/rmq-node))
(in-package :monomyth/tests/rmq-worker)

(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))

(plan nil)
(vom:config t :info)

(subtest "worker starts/shutdown"
  (bt:make-thread
   #'(lambda ()
       (pzmq:with-context nil
         (pzmq:with-socket master :router
           (pzmq:bind master "tcp://*:55555")
           (let ((msg (mmop-m:pull-master-message master)))
             (is-type msg 'mmop-m:worker-ready-v0)
             (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0
                                         (mmop-m:worker-ready-v0-client-id msg))))))))
  (let ((wrkr (build-rmq-worker :host *rmq-host*)))
    (start-worker wrkr "tcp://localhost:55555")
    (run-worker wrkr)
    (stop-worker wrkr)
    (pass "worker stopped")))

(defparameter *source-queue* (format nil "test-source-~d" (get-universal-time)))
(defparameter *dest-queue* (format nil "test-dest-~d" (get-universal-time)))

(subtest "worker can start node"
  (let ((recipe (build-rmq-node-recipe :test "#'(lambda (x) (format nil \"test ~a\" x))"
                                       *source-queue* *dest-queue*)))
    (bt:make-thread
     #'(lambda ()
         (pzmq:with-context nil
           (pzmq:with-socket master :router
             (pzmq:bind master "tcp://*:55555")
             (let* ((msg (mmop-m:pull-master-message master))
                    (id (mmop-m:worker-ready-v0-client-id msg)))
               (is-type msg 'mmop-m:worker-ready-v0)
               (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe))
               (let ((res-msg (mmop-m:pull-master-message master)))
                 (is-type res-msg 'mmop-m:start-node-success-v0)
                 (is (mmop-m:start-node-success-v0-type res-msg) "TEST"))
               (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0 id)))))))
    (let ((wrkr (build-rmq-worker :host *rmq-host*)))
      (start-worker wrkr "tcp://localhost:55555")
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped"))))

(subtest "worker catches bad fn"
  (let ((recipe (build-rmq-node-recipe :test "#'(lambda (x) (format nil \"test ~a\" x)"
                                       *source-queue* *dest-queue*)))
    (bt:make-thread
     #'(lambda ()
         (pzmq:with-context nil
           (pzmq:with-socket master :router
             (pzmq:bind master "tcp://*:55555")
             (let* ((msg (mmop-m:pull-master-message master))
                    (id (mmop-m:worker-ready-v0-client-id msg)))
               (is-type msg 'mmop-m:worker-ready-v0)
               (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe))
               (let ((res-msg (mmop-m:pull-master-message master)))
                 (is-type res-msg 'mmop-m:start-node-failure-v0)
                 (is (mmop-m:start-node-failure-v0-type res-msg) "TEST")
                 (is (mmop-m:start-node-failure-v0-reason-cat res-msg)
                     "function read")
                 (is (mmop-m:start-node-failure-v0-reason-msg res-msg)
                     "end of file (mismatched forms)"))
               (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0 id)))))))
    (let ((wrkr (build-rmq-worker :host *rmq-host*)))
      (start-worker wrkr "tcp://localhost:55555")
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped"))))

(skip 1 "worker catches bad recipe type")

(subtest "worker processes data - single node"
  (let ((recipe1 (build-rmq-node-recipe :test "#'(lambda (x) (format nil \"test ~a\" x))"
                                        *source-queue* *dest-queue* 5))
        (conn (let ((conn (setup-connection :host (uiop:getenv "TEST_RMQ"))))
                (if (getf conn :success)
                    (getf conn :conn)
                    (error (getf conn :error)))))
        (items '("1" "3" "testing" "is" "boring" "these" "should" "all" "be processed")))
    (with-channel (conn 1)
      (iter:iterate
        (iter:for item in items)
        (basic-publish conn 1 :routing-key *source-queue* :body item)))
    (destroy-connection conn)
    (bt:make-thread
     #'(lambda ()
         (pzmq:with-context nil
           (pzmq:with-socket master :router
             (pzmq:bind master "tcp://*:55555")
             (let* ((msg (mmop-m:pull-master-message master))
                    (id (mmop-m:worker-ready-v0-client-id msg)))
               (is-type msg 'mmop-m:worker-ready-v0)
               (send-msg master *mmop-v0* (mmop-m:make-start-node-v0 id recipe1))
               (let ((res-msg (mmop-m:pull-master-message master)))
                 (is-type res-msg 'mmop-m:start-node-success-v0)
                 (is (mmop-m:start-node-success-v0-type res-msg) "TEST"))
               (sleep 1)
               (send-msg master *mmop-v0* (mmop-m:make-shutdown-worker-v0 id)))))))
    (let ((wrkr (build-rmq-worker :host *rmq-host*)))
      (start-worker wrkr "tcp://localhost:55555")
      (run-worker wrkr)
      (stop-worker wrkr)
      (pass "worker stopped"))

    (setf conn (let ((conn (setup-connection :host (uiop:getenv "TEST_RMQ"))))
                 (if (getf conn :success)
                     (getf conn :conn)
                     (error (getf conn :error)))))
    (with-channel (conn 1)
      (basic-consume conn 1 *dest-queue*)
      (iter:iterate
        (iter:for item in items)
        (iter:for got = (consume-message conn))
        (is (babel:octets-to-string (message/body (envelope/message got)))
            (format nil "test ~a" item))
        (basic-ack conn 1 (envelope/delivery-tag got))))))

(defparameter *conn* (let ((conn (setup-connection :host (uiop:getenv "TEST_RMQ"))))
                       (if (getf conn :success)
                           (getf conn :conn)
                           (error (getf conn :error)))))
(with-channel (*conn* 1)
  (queue-delete *conn* 1 *source-queue*)
  (queue-delete *conn* 1 *dest-queue*)
  (queue-delete *conn* 1 "TEST-fail"))
(destroy-connection *conn*)

(finalize)
