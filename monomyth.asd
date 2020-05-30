(defsystem "monomyth"
  :version "0.1.0"
  :author ""
  :license ""
  :depends-on ()
  :components ((:module "src"
                :components
                ((:file "main"))))
  :description ""
  :in-order-to ((test-op (test-op "monomyth/tests"))))

(defsystem "monomyth/tests"
  :author ""
  :license ""
  :depends-on ("monomyth"
               "rove")
  :components ((:module "tests"
                :components
                ((:file "main"))))
  :description "Test system for monomyth"
  :perform (test-op (op c) (symbol-call :rove :run c)))
