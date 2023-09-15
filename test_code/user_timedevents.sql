-- /* create tables for user timed events sql questions */

drop table if exists user_timedevents;

CREATE TABLE IF NOT EXISTS user_timedevents 
(
	userid integer,
	acct_create_date timestamp without time zone, 
	save_home_date timestamp without time zone,
	state varchar(2)
);


COPY user_timedevents 
FROM '/<mypath>/user_timeevents.csv' DELIMITERS E'\t' 
CSV HEADER
;


--Table 1 has userID and State, Table 2 has UserID, saved home, Date of saving home. 


drop table if exists user_state;

CREATE TABLE IF NOT EXISTS user_state
as
select userid, acct_create_date, state
from user_timedevents
;

drop table if exists user_open_save;

CREATE TABLE IF NOT EXISTS user_open_save
as
select userid, save_home_date
from user_timedevents
;

--/*How to Calculate Percent Calculation*/
--Table 1 has userID and State, Table 2 has UserID, saved home, Date of saving home. 
--Write me a query that tells me what percent of users in a state have saved a home.

select
t2.state
,count(distinct case when t1.save_home_date is not null then t1.userid end) uusers_w_saved_home
,count(distinct t1.userid) unique_users
,100.0 * count(distinct case when t1.save_home_date is not null then t1.userid end) 
/count(distinct t1.userid) pct_uusers_w_saved_home
from user_open_save t1
left join user_state t2 on t1.userid=t2.userid
group by 1
order by 1
;


--/* Average Date Diff */
--Question: "Table 1 has UserID and save_home_date, Table 2 has User ID, state, and save_home_date. 
--Write me a function that tells me the average time it takes between a user makes an account and saves a home."

select
date_trunc('MONTH',t2.acct_create_date) month_opened
,count(distinct t1.userid) uusers
,count(distinct case when t2.acct_create_date is not null then t1.userid end) uusers_w_acct_created
,count(distinct case when t1.save_home_date is not null then t1.userid end) uusers_w_save_home
,avg(case when t1.save_home_date is not null then DATE_PART('Day', t1.save_home_date::TIMESTAMP - t2.acct_create_date::TIMESTAMP) end) avg_saved_date
from user_open_save t1
left join user_state t2 on t1.userid=t2.userid
where 1=1
and t2.acct_create_date is not null
group by 1
order by 1
;
