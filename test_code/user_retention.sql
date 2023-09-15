-- /* create tables for user retention  sql questions */

drop table if exists user_retention;

CREATE TABLE IF NOT EXISTS user_retention 
(
	activity_date timestamp without time zone,
	userid int,
	sales int
);


COPY user_retention 
FROM '/Users/ouonomos/git_repo/redshift_dev/test_data/user_retention_test.csv' DELIMITERS E'\t' 
CSV HEADER
;


-- /* may have your retention please */

-- find users joining in a given month for the first time

select
DATE_TRUNC('month', t1.activity_date) active_month
,count(distinct t1.userid) uusers
,count(distinct case when first_active_month=DATE_TRUNC('month', t1.activity_date) then t1.userid end) new_users
from
(
	SELECT 
	t1.userid
	,t1.activity_date
	,min(DATE_TRUNC('month', t1.activity_date)) over (partition by t1.userid) first_active_month -- note that you have to force an eval
		-- at the user level in order to get their global first activity date
	from user_retention t1
) t1
group by 1 
order by 1 desc
;

-- find count of users who have not been seen in 3 days

select
t1.activity_date
,count(distinct t1.userid) uusers
,count(distinct case when DATE_PART('Day', t1.activity_date::TIMESTAMP - t1.lag_activity_date::TIMESTAMP)=4 then t1.userid end) lost_3_days
from 
(
	select
	t1.userid
	,date_trunc('day',t1.activity_date) activity_date
	,lag(date_trunc('day',t1.activity_date),1) over (partition by t1.userid order by t1.activity_date) lag_activity_date
	from user_retention t1
) t1
group by 1
order by 1
;

-- retained users from previous month

select
distinct
t1.active_month
,count(distinct t1.userid) unique_users
,count(distinct t2.userid) retained_users 
from
(
	SELECT 
	distinct
	date_trunc('month',t1.activity_date) active_month
	,t1.userid
	from user_retention t1
) t1
left join -- you could also do this without the nesting, like:
	--"left join user_retention t2 on t1.userid=t2.userid and DATE_PART('Month', t1.active_month::TIMESTAMP) - DATE_PART('Month',t2.active_month::TIMESTAMP)=1"
	--but I feel the nesting makes it more readable
(
	SELECT 
	distinct
	date_trunc('month',t1.activity_date) active_month
	,t1.userid
	from user_retention t1
) t2 on t1.userid=t2.userid
and DATE_PART('Month', t1.active_month::TIMESTAMP) - DATE_PART('Month',t2.active_month::TIMESTAMP)=1 -- in other words, they were in the previous month's data set as well
group by 1
order by 1
;



