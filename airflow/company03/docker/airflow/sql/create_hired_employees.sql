CREATE TABLE IF NOT EXISTS hired_employees (
  id              int         DEFAULT NULL,
  name            varchar(50) DEFAULT NULL,
  datetime        timestamp   DEFAULT NULL,
  department_id   int         DEFAULT NULL,
  job_id          int         DEFAULT NULL
) ;
