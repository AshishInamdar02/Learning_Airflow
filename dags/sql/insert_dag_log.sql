-- Adding delete insert to avoid duplication of data in the table

DELETE FROM dag_logs WHERE run_date = '{{ds}}' AND dag_id = '{{dag.dag_id}}';

INSERT INTO dag_logs (run_date, dag_id) VALUES ('{{ds}}', '{{dag.dag_id}}');