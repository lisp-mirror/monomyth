(defpackage monomyth/tests/rmq-worker
  (:use :cl :prove :monomyth/worker :monomyth/rmq-worker :monomyth/rmq-node-recipe :alexandria
        :monomyth/rmq-node))
(in-package :monomyth/tests/rmq-worker)

(plan nil)

(subtest "successful worker pass"
  (let ((worker (build-rmq-worker "127.0.0.1" 4 :host (uiop:getenv "TEST_RMQ")
                                                :port 5672 :username "guest" :password "guest"))
        (queue-1 (format nil "test-queue-1-~a" (get-universal-time)))
        (queue-2 (format nil "test-queue-2-~a" (get-universal-time)))
        (queue-3 (format nil "test-queue-3-~a" (get-universal-time)))
        (queue-4 (format nil "test-queue-4-~a" (get-universal-time)))
        (items '("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")))
    (print 1)
    (start-worker worker)
    (print 2)
    (build-node worker (build-rmq-node-recipe :type1 #'(lambda (x) (format nil "test1 ~a" x))
                                              queue-1 queue-2 1))
    (print 3)
    (build-node worker (build-rmq-node-recipe :type2 #'(lambda (x) (format nil "test2 ~a" x))
                                              queue-2 queue-3 1))
    (print 4)
    (build-node worker (build-rmq-node-recipe :type3 #'(lambda (x) (format nil "test3 ~a" x))
                                              queue-3 queue-4 1))
    (print 5)
    (let ((access-node (second (hash-table-plist (worker/nodes worker)))))
      (iter:iterate
        (iter:for i in items)
        (send-message access-node queue-1 i))
      (sleep 100000000)

      (delete-queue access-node queue-1)
      (delete-queue access-node queue-2)
      (delete-queue access-node queue-3)
      (delete-queue access-node queue-4)
      (delete-queue access-node "TYPE1-fail")
      (delete-queue access-node "TYPE2-fail")
      (delete-queue access-node "TYPE3-fail")
      (stop-worker worker))))

(finalize)
