(defsystem "monomyth"
  :version "0.1.0"
  :author ""
  :license ""
  :components ((:module "src"
                :components
                ((:file "node-recipe")
                 (:file "mmop")
                 (:file "mmop-worker"
                  :components ("mmop"))
                 (:file "mmop-master"
                  :components ("mmop" "node-recipe"))
                 (:file "node")
                 (:file "rmq-node"
                  :depends-on ("node"))
                 (:file "rmq-node-recipe"
                  :depends-on ("node-recipe"))
                 (:file "worker"
                  :depends-on ("node" "mmop" "mmop-worker"))
                 (:file "rmq-worker"
                  :depends-on ("worker" "node" "rmq-node" "node-recipe" "rmq-node-recipe"))
                 (:file "master"))))
  :depends-on (:flexi-streams
               :cl-store
               :rutils
               :trivia
               :alexandria
               :pzmq
               :uuid
               :iterate
               :vom
               :cl-rabbit
               :babel)
  :description ""
  :in-order-to ((test-op (test-op "monomyth/tests"))))

(defsystem "monomyth/tests"
  :author ""
  :license ""
  :depends-on (:monomyth
               :rove
               :cl-mock)
  :components ((:module "tests"
                :components
                ((:file "rmq-node")
                 (:file "rmq-node-recipe")
                 (:file "mmop")
                 (:file "rmq-worker"))))
  :description "Test system for monomyth"
  :perform (test-op (op c) (symbol-call :rove '#:run c)))

(defsystem "monomyth/communication-tests-master"
  :author ""
  :license ""
  :depends-on (:monomyth
               :prove)
  :defsystem-depends-on (:prove-asdf)
  :components ((:module "communication-tests/master"
                :components
                ((:test-file "mmop")
                 (:test-file "rmq-worker"))))
  :description "Test system for monomyth for inter machine communication, master perspective"
  :perform (test-op (op c) (funcall (intern #.(string :run) :prove) c)))

(defsystem "monomyth/communication-tests-worker"
  :author ""
  :license ""
  :depends-on (:monomyth
               :prove)
  :defsystem-depends-on (:prove-asdf)
  :components ((:module "communication-tests/worker"
                :components
                ((:test-file "mmop")
                 (:test-file "rmq-worker"))))
  :description "Test system for monomyth for inter machine communication, worker perspective"
  :perform (test-op (op c) (funcall (intern #.(string :run) :prove) c)))
