drop table if exists events_per_user;

create table events_per_user (
 user_id     char(50)
,page_views  int
,total_bytes int
,max_bytes   int
,min_bytes   int
);