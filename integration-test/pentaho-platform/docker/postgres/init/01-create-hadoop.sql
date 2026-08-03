CREATE TABLE IF NOT EXISTS public.hadoop (
    hadoop_column integer NOT NULL,
    CONSTRAINT hadoop_pk PRIMARY KEY (hadoop_column)
);

INSERT INTO public.hadoop (hadoop_column)
VALUES (1), (2)
ON CONFLICT (hadoop_column) DO NOTHING;