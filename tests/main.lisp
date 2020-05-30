(defpackage monomyth/tests/main
  (:use :cl
        :monomyth
        :rove))
(in-package :monomyth/tests/main)

;; NOTE: To run this test file, execute `(asdf:test-system :monomyth)' in your Lisp.

(deftest test-target-1
  (testing "should (= 1 1) to be true"
    (ok (= 1 1))))
