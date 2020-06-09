(defsystem "monomyth"
  :version "0.1.0"
  :author ""
  :license ""
  :class :package-inferred-system
  :depends-on (:lfarm-server
               :lfarm-client
               :uuid
               :cl-cpus
               :iterate
               :vom
               :lfarm-admin
               :cl-rabbit
               :babel
               :folio2)
  :description ""
  :in-order-to ((test-op (test-op "monomyth/tests"))))

(defsystem "monomyth/tests"
  :author ""
  :license ""
  :depends-on (:monomyth
               :prove
               :cl-mock)
  :defsystem-depends-on (:prove-asdf)
  :components ((:module "tests"
                :components
                ((:test-file "rmq-node"))))
  :description "Test system for monomyth"
  :perform (test-op (op c) (funcall (intern #.(string :run) :prove) c)))
