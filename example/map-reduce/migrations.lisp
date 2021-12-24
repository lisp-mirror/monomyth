(defpackage monomyth/map-reduce/migrations
  (:use :cl :cl-migratum.provider.local-path :cl-migratum
        :cl-migratum.driver.sql :monomyth/map-reduce/db))
(in-package :monomyth/map-reduce/migrations)

(defparameter *migrations-path* (uiop:getenv "MIGRATIONS_PATH"))

(defparameter *migration-provider* (make-local-path-provider *migrations-path*))

(defparameter *migration-driver* (make-sql-driver *migration-provider* (connect-to-db)))

(defun init-migrations ()
  (provider-init *migration-provider*)
  (driver-init *migration-driver*))

(defun list-all-migrations ()
  (provider-list-migrations *migration-provider*))

(defun list-pending-migrations ()
  (list-pending *migration-driver*))

(defun migrations-up ()
  (apply-pending *migration-driver*))

(defun migrations-down ()
  (revert-last *migration-driver* :count (length (list-all-migrations))))

(defun display-migration-state ()
  (display-applied *migration-driver*)
  (display-pending *migration-driver*))

(defun add-migration (name)
  (provider-create-migration *migration-provider* :description name))

(defun shutdown-migraions ()
  (driver-shutdown *migration-driver*)
  (provider-shutdown *migration-provider*))
