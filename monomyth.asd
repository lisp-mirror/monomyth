(defsystem "monomyth"
  :version "0.1.0"
  :author "Paul Ricks"
  :license "MPL 2.0"
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
               :stmx
               :closer-mop
               :rutils
               :trivia
               :alexandria
               :pzmq
               :uuid
               :iterate
               :verbose
               :cl-rabbit
               :babel)
  :description "A distributed data processing library for CL"
  :in-order-to ((test-op (test-op "monomyth/tests"))))

(defsystem "monomyth/tests"
  :author "Paul Ricks"
  :license "MPL 2.2"
  :depends-on (:monomyth
               :rove
               :cl-mock)
  :components ((:module "tests"
                :components
                ((:file "utils")
                 (:file "rmq-node"
                  :depends-on ("utils"))
                 (:file "rmq-node-recipe")
                 (:file "mmop"
                  :depends-on ("utils"))
                 (:file "rmq-worker"
                  :depends-on ("utils"))
                 (:file "master"
                  :depends-on ("utils")))))
  :description "Test system for monomyth"
  :perform (test-op (op c) (symbol-call :rove '#:run c)))

(defsystem "monomyth/communication-tests-master"
  :author "Paul Ricks"
  :license "MPL 2.0"
  :depends-on (:monomyth
               :monomyth/tests
               :rove)
  :components ((:module "communication-tests/master"
                :components
                ((:file "mmop")
                 (:file "rmq-worker"))))
  :description "Test system for monomyth for inter machine communication, master perspective"
  :perform (test-op (op c) (symbol-call :rove '#:run c)))

(defsystem "monomyth/communication-tests-worker"
  :author "Paul Ricks"
  :license "MPL 2.0"
  :depends-on (:monomyth
               :monomyth/tests
               :rove)
  :components ((:module "communication-tests/worker"
                :components
                ((:file "mmop")
                 (:file "rmq-worker"))))
  :description "Test system for monomyth for inter machine communication, worker perspective"
  :perform (test-op (op c) (symbol-call :rove '#:run c)))

(defsystem "monomyth/processing-tests-master"
  :author "Paul Ricks"
  :license "MPL 2.0"
  :depends-on (:monomyth
               :rove)
  :components ((:module "processing-tests"
                :components
                ((:file "utils")
                 (:file "master"
                  :depends-on ("utils")))))
  :description "Test system for monomyth for modeling actual data processing,
master perspective"
  :perform (test-op (op c) (symbol-call :rove '#:run c)))

(defsystem "monomyth/processing-tests-worker"
  :author "Paul Ricks"
  :license "MPL 2.0"
  :depends-on (:monomyth
               :rove)
  :components ((:module "processing-tests"
                :components
                ((:file "utils")
                 (:file "worker"
                  :depends-on ("utils")))))
  :description "Test system for monomyth for modeling actual data processing,
worker perspective"
  :perform (test-op (op c) (symbol-call :rove '#:run c)))
