(defpackage monomyth/map-reduce/db
  (:use :cl)
  (:export :connect-to-db
           :get-all-counts
           :add-word-counts))
(in-package :monomyth/map-reduce/db)

(defparameter *postgres-db* "monomyth")
(defparameter *postgres-host* "localhost")
(defparameter *postgres-port* 5432)
(defparameter *postgres-user* "user")
(defparameter *postgres-pass* "password")
(defparameter *postgres-ssl* :no)

(defun connect-to-db ()
  "connects to the postgres db set up by docker"
  (dbi:connect :postgres
               :database-name *postgres-db*
               :host *postgres-host*
               :port *postgres-port*
               :username *postgres-user*
               :password *postgres-pass*
               :use-ssl *postgres-ssl*))

(defparameter *get-all-counts-query*
  "SELECT word, word_count FROM map_reduce.word_counts")
(defun get-all-counts (conn)
  "pulls all the known word counts from the database"
  (dbi:fetch-all (dbi:execute (dbi:prepare conn *get-all-counts-query*))))

(defparameter *add-word-counts-query-start*
  "
INSERT INTO map_reduce.word_counts (word, word_count)
VALUES ")
(defparameter *add-word-counts-query-end*
  "
ON CONFLICT (word)
DO UPDATE SET word_count = EXCLUDED.word_count + word_counts.word_count")
(defparameter *values-pair* "(? ,?)")
(defun add-word-counts (conn vals)
  "upserts a new word count value into the database"
  (let* ((values
           (iter:iterate
             (iter:repeat (/ (length vals) 2))
             (iter:collect *values-pair*)))
         (values-str (str:join ", " values))
         (query (str:concat *add-word-counts-query-start*
                            values-str
                            *add-word-counts-query-end*)))
    (dbi:fetch-all (dbi:execute (dbi:prepare conn query) vals))))
