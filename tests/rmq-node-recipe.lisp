(defpackage monomyth/tests/rmq-node-recipe
  (:use :cl :rove :monomyth/node-recipe :monomyth/rmq-node-recipe))
(in-package :monomyth/tests/rmq-node-recipe)

(deftest rmq-node-recipe
  (testing "can serialize and deserialize rmq-node-recipe"
    (let* ((recipe (make-instance 'rmq-node-recipe :type :test :source "test-s" :dest "test-d"))
           (new-recipe (deserialize-recipe (serialize-recipe recipe))))
      (ok (eql (node-recipe/type recipe) (node-recipe/type new-recipe)))
      (ok (= (node-recipe/batch-size recipe) (node-recipe/batch-size new-recipe)))
      (ok (string= (rmq-node-recipe/source-queue recipe) (rmq-node-recipe/source-queue new-recipe)))
      (ok (string= (rmq-node-recipe/dest-queue recipe) (rmq-node-recipe/dest-queue new-recipe))))))
