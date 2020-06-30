(defpackage monomyth/tests/rmq-node-recipe
  (:use :cl :prove :monomyth/node-recipe :monomyth/rmq-node-recipe))
(in-package :monomyth/tests/rmq-node-recipe)

(plan 1)

(subtest "can serialize and deserialize rmq-node-recipe"
  (let* ((recipe (build-rmq-node-recipe :test "#'(lambda (x) (1+ x))" "test-s" "test-d" 100))
         (new-recipe (deserialize-recipe (serialize-recipe recipe))))
    (is (node-recipe/type recipe) (node-recipe/type new-recipe))
    (is (node-recipe/transform-fn recipe) (node-recipe/transform-fn new-recipe))
    (is (node-recipe/batch-size recipe) (node-recipe/batch-size new-recipe))
    (is (rmq-node-recipe/source-queue recipe) (rmq-node-recipe/source-queue new-recipe))
    (is (rmq-node-recipe/dest-queue recipe) (rmq-node-recipe/dest-queue new-recipe))))

(finalize)
