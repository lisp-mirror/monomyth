(defsystem "monomyth"
  :version "0.3.1"
  :author "Paul Ricks"
  :license "MPL 2.0"
  :components ((:module "src"
                :components
                ((:file "node-recipe")
                 (:file "mmop")
                 (:file "mmop-worker"
                  :components ("mmop" "node-recipe"))
                 (:file "mmop-master"
                  :components ("mmop" "node-recipe"))
                 (:file "mmop-control"
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
                 (:file "master")
                 (:file "dsl"
                  :depends-on ("node-recipe" "rmq-node-recipe" "node" "rmq-node" "worker" "rmq-worker" "master"))))

               (:module "src/control-api"
                :components ((:file "main"))))
  :depends-on (:flexi-streams
               :cl-store
               :stmx
               :closer-mop
               :rutils
               :trivia
               :optima
               :jonathan
               :fset
               :alexandria
               :cl-algebraic-data-type
               :pzmq
               :uuid
               :iterate
               :verbose
               :cl-rabbit
               :lucerne
               :woo
               :babel)
  :description "A distributed data processing library for CL"
  :in-order-to ((test-op (test-op "monomyth/tests"))))

(defsystem "monomyth/tests"
  :author "Paul Ricks"
  :license "MPL 2.0"
  :depends-on (:monomyth
               :rove
               :quri
               :dexador
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
                  :depends-on ("utils"))
                 (:file "control-api"))))
  :description "Test system for monomyth"
  :perform (test-op (op c) (symbol-call :rove '#:run c)))

(defsystem "monomyth/example-master"
  :author "Paul Ricks"
  :license "MPL 2.0"
  :depends-on (:monomyth
               :rove)
  :components ((:module "example"
                :components
                ((:file "utils")
                 (:file "master"
                  :depends-on ("utils")))))
  :description "Example system for monomyth for modeling actual data processing,
master perspective"
  :perform (test-op (op c) (symbol-call :rove '#:run c)))

(defsystem "monomyth/example-worker"
  :author "Paul Ricks"
  :license "MPL 2.0"
  :depends-on (:monomyth
               :rove)
  :components ((:module "example"
                :components
                ((:file "utils")
                 (:file "worker"
                  :depends-on ("utils")))))
  :description "Example system for monomyth for modeling actual data processing,
worker perspective"
  :perform (test-op (op c) (symbol-call :rove '#:run c)))
