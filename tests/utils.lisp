(defpackage monomyth/tests/utils
  (:use :cl :monomyth/rmq-worker :monomyth/rmq-node-recipe :monomyth/rmq-node
        :monomyth/node :monomyth/worker :stmx :monomyth/node-recipe :rove)
  (:shadow :closer-mop)
  (:export
   *rmq-host*
   *rmq-user*
   *rmq-pass*
   testing-node
   testing-node1
   testing-node2
   testing-node3
   test-recipe
   test-recipe1
   test-recipe2
   test-recipe3
   build-test-node
   build-test-node1
   build-test-recipe
   build-test-recipe1
   build-test-recipe2
   build-test-recipe3
   test-request-success
   test-shutdown-success))
(in-package :monomyth/tests/utils)

(defparameter *rmq-host* (uiop:getenv "TEST_RMQ"))
(defparameter *rmq-user* (uiop:getenv "TEST_RMQ_DEFAULT_USER"))
(defparameter *rmq-pass* (uiop:getenv "TEST_RMQ_DEFAULT_PASS"))

(transactional
    (defclass testing-node (rmq-node) ()))

(defclass test-recipe (rmq-node-recipe) ())

(defun build-test-node (name source dest fail size host user pass)
  (make-instance
   'testing-node :name name :source source :dest dest :fail fail :batch-size size
                 :type :test :conn (setup-connection :host host :username user
                                                     :password pass)))

(defun build-test-recipe (source dest)
  (make-instance 'test-recipe :source source :dest dest :type :test))

(defmethod transform-fn ((node testing-node) item)
  (format nil "test ~a" item))

(defmethod build-node ((worker rmq-worker) (recipe test-recipe))
  (build-test-node (name-node recipe) (rmq-node-recipe/source-queue recipe)
                   (rmq-node-recipe/dest-queue recipe) (name-fail-queue recipe)
                   (node-recipe/batch-size recipe) (rmq-worker/host worker)
                   (rmq-worker/username worker) (rmq-worker/password worker)))

(transactional
    (defclass testing-node1 (rmq-node) ()))

(defclass test-recipe1 (rmq-node-recipe) ())

(defun build-test-node1 (name source dest fail size host user pass)
  (make-instance 'testing-node1 :name name :source source :dest dest :fail fail
                                :batch-size size :type :test1
                                :conn (setup-connection :host host
                                                        :username user
                                                        :password pass)))

(defun build-test-recipe1 (source dest batch)
  (make-instance 'test-recipe1 :source source :dest dest :type :test1 :batch-size batch))

(defmethod transform-fn ((node testing-node1) item)
  (format nil "test1 ~a" item))

(defmethod build-node ((worker rmq-worker) (recipe test-recipe1))
  (build-test-node1 (name-node recipe) (rmq-node-recipe/source-queue recipe)
                    (rmq-node-recipe/dest-queue recipe) (name-fail-queue recipe)
                    (node-recipe/batch-size recipe) (rmq-worker/host worker)
                    (rmq-worker/username worker) (rmq-worker/password worker)))

(transactional
    (defclass testing-node2 (rmq-node) ()))

(defclass test-recipe2 (rmq-node-recipe) ())

(defun build-test-node2 (name source dest fail size host user pass)
  (make-instance 'testing-node2 :name name :source source :dest dest :fail fail
                                :batch-size size :type :test2
                                :conn (setup-connection :host host
                                                        :username user
                                                        :password pass)))

(defun build-test-recipe2 (source dest batch)
  (make-instance 'test-recipe2 :source source :dest dest :type :test2 :batch-size batch))

(defmethod transform-fn ((node testing-node2) item)
  (format nil "test2 ~a" item))

(defmethod build-node ((worker rmq-worker) (recipe test-recipe2))
  (build-test-node2 (name-node recipe) (rmq-node-recipe/source-queue recipe)
                    (rmq-node-recipe/dest-queue recipe) (name-fail-queue recipe)
                    (node-recipe/batch-size recipe) (rmq-worker/host worker)
                    (rmq-worker/username worker) (rmq-worker/password worker)))

(transactional
    (defclass testing-node3 (rmq-node) ()))

(defclass test-recipe3 (rmq-node-recipe) ())

(defun build-test-node3 (name source dest fail size host user pass)
  (make-instance 'testing-node3 :name name :source source :dest dest :fail fail
                                :batch-size size :type :test3
                                :conn (setup-connection :host host
                                                        :username user
                                                        :password pass)))

(defun build-test-recipe3 (source dest batch)
  (make-instance 'test-recipe3 :source source :dest dest :batch-size batch
                               :type :test3))

(defmethod transform-fn ((node testing-node3) item)
  (format nil "test3 ~a" item))

(defmethod build-node ((worker rmq-worker) (recipe test-recipe3))
  (build-test-node3 (name-node recipe) (rmq-node-recipe/source-queue recipe)
                    (rmq-node-recipe/dest-queue recipe) (name-fail-queue recipe)
                    (node-recipe/batch-size recipe) (rmq-worker/host worker)
                    (rmq-worker/username worker) (rmq-worker/password worker)))

(defun test-request-success (socket)
  (adt:match mmop-c:received-mmop (mmop-c:pull-control-message socket)
    ((mmop-c:start-node-request-success-v0) (pass "request succeeded message"))
    (_ (fail "unexpected message type"))))

(defun test-shutdown-success (socket)
  (adt:match mmop-c:received-mmop (mmop-c:pull-control-message socket)
    ((mmop-c:stop-worker-request-success-v0) (pass "stop worker succeeded message"))
    (_ (fail "unexpected message type"))))
