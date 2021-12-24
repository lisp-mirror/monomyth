CREATE TABLE map_reduce.word_counts (
       record_id BIGSERIAL,
       word VARCHAR NOT NULL,
       word_count INTEGER NOT NULL,

       CONSTRAINT word_counts_pk PRIMARY KEY (record_id)
);
