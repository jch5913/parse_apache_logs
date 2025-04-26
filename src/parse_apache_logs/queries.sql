create database apache_access_logs;

create table log_data (
  data_id bigint generated always as identity primary key
  ,ip_address varchar(50)
  ,request_time timestamp
  ,request_method text
  ,request_path text
  ,http_version varchar(20)
  ,status_code int
  ,response_size bigint
  ,referer text
  ,user_agent text
);


create procedure insert_log_data (
  ip_address varchar(50)
  ,request_time timestamp
  ,request_method text
  ,request_path text
  ,http_version varchar(20)
  ,status_code varchar(20)
  ,response_size varchar(20)
  ,referer text
  ,user_agent text
)
language plpgsql
as $$
begin

  ip_address = NULLIF(ip_address, '-');
  request_method = NULLIF(request_method, '-');
  request_path = NULLIF(request_path, '-');
  http_version = NULLIF(http_version, '-');
  status_code = NULLIF(status_code, '-');
  response_size = NULLIF(response_size, '-');
  referer = NULLIF(referer, '-');
  user_agent = NULLIF(user_agent, '-');

  insert into log_data (ip_address, request_time, request_method, request_path, http_version, status_code, response_size, referer, user_agent)
  values (ip_address, request_time, request_method, request_path, http_version, cast(status_code as int), cast(response_size as bigint), referer, user_agent);

end; $$;


create view log_data_counts
as
  select count(*) as request_count, count(distinct ip_address) as unique_ip_count from log_data;


create view log_data_top_ip_address
as
  select ip_address, count(*) as request_count from log_data group by ip_address order by request_count desc limit 10;


create view log_data_top_request_path
as
  select request_path, count(*) as request_count from log_data group by request_path order by request_count desc limit 10;


create view log_data_busy_hour
as
  select date_trunc('hour', cast(request_time as time)) as request_hour, count(*) as request_count from log_data group by date_trunc('hour', cast(request_time as time)) order by request_count desc limit 1;
