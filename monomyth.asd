(defsystem "monomyth"
  :version "0.1.0"
  :author ""
  :license ""
  :components ((:module "src"
                :components
                ((:file "main")
                 (:file "mmop")
                 (:file "mmop-worker"
                  :components ("mmop"))
                 (:file "mmop-master"
                  :components ("mmop"))
                 (:file "node")
                 (:file "rmq-node"
                  :depends-on ("node"))
                 (:file "node-recipe")
                 (:file "rmq-node-recipe"
                  :depends-on ("node-recipe"))
                 (:file "worker"
                  :depends-on ("node"))
                 (:file "rmq-worker"
                  :depends-on ("worker" "node" "rmq-node" "node-recipe" "rmq-node-recipe"))
                 (:file "master"))))
  :depends-on (:lfarm-server
               :lfarm-client
               :flexi-streams
               :cl-store
               :rutils
               :trivia
               :alexandria
               :pzmq
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
                ((:test-file "rmq-node")
                 (:test-file "rmq-node-recipe")
                 (:test-file "mmop"))))
  :description "Test system for monomyth"
  :perform (test-op (op c) (funcall (intern #.(string :run) :prove) c)))
