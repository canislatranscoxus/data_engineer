drop table if exists events_raw;

create table events_raw (
     user_id         char(50)
    ,ip              char(15)
    ,timestamp       char(30)
    ,http_request    char(50)
    ,http_response   int
    ,lat             float4
    ,lng             float4
    ,user_agent      char(100)
    ,num_bytes       int
);