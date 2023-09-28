--Hard: Reporting Adjacent Date Intervals

--Table: running_processes

--+--------------+---------+
--| Column Name  | Type    |
--+--------------+---------+
--|process_id    | int    |
--|run_date    | date    |
--+--------------+---------+
--– process_id*run_date permutations are globally unique.
--this table contains the state of all running processes on each day


--Table: cancelled_processes
--+--------------+---------+
--| Column Name  | Type    |
--+--------------+---------+
--| process_id | int    |
--| cancelled_date | date    |
--+--------------+---------+
--– process_id*cancelled_date permutations are globally unique.
--this table contains the state of all canceled processes on each day
 
--A system is running or canceling each task every day. Every task is independent of the previous tasks. The tasks can fail or succeed.

--Write a solution to report the period_state for each continuous interval of days in the period from 2020-01-01 to 2020-12-31.

--period_state is 'failed' if tasks in this interval failed or 'succeeded' if tasks in this interval succeeded. Interval of days are retrieved as start_date and end_date.

--Return the result table ordered by start_date.

--The result format is in the following example.

--Example 1:

--Inputs: 

--running_processes table:
+-------------------+
| process_id | run_date |
+-------------------+
| 123	| 2019-12-28 |
| 123	| 2019-12-29 |
| 123	| 2020-01-04 |
| 123	| 2020-01-05 |
+-------------------+

--cancelled_processes table:
+-------------------+
| process_id | canceled_date |
+-------------------+
| 123	|  2019-12-30 |
| 123	|  2019-12-31 |
| 123	|  2019-01-01 |
| 123	|  2020-01-02 |
| 123	|  2020-01-03 |
| 123	|  2020-01-06 |
+-------------------+

--Output: 
+--------------+--------------+--------------+
| period_state | start_date   | end_date     |
+--------------+--------------+--------------+
| running    | 2020-01-01   | 2020-01-03   |
| canceled       | 2020-01-04   | 2020-01-05   |
| running    | 2020-01-06   | 2020-01-06   |
+--------------+--------------+--------------+
-- ANSWER

DROP TABLE IF EXISTS running_processes;

CREATE TABLE running_processes
(
	process_id int,
	run_date date
)
;

INSERT INTO running_processes
VALUES
(123,'2019-12-28'),
(123,'2019-12-29'),
(123,'2020-01-04'),
(123,'2020-01-05')
;

DROP TABLE IF EXISTS cancelled_processes;

CREATE TABLE cancelled_processes
(
	process_id int,
	canceled_date date
)
;

INSERT INTO cancelled_processes
VALUES
(123,'2019-12-30'),
(123,'2019-12-31'),
(123,'2019-01-01'),
(123,'2020-01-02'),
(123,'2020-01-03'),
(123,'2020-01-06')
;

drop table if exists date_changes;

create table date_changes as
select
t1.*
,lag(t1.date,1) over (partition by status order by date) lag1_date
from
(
	select 
	t1.run_date as date
	,'running' as status
	from running_processes t1
	
	UNION ALL
	
	select 
	t1.canceled_date as date
	,'cancelled' as status
	from cancelled_processes t1
) t1
;



select
t1.interval_id
,t1.status as period_state
,min(date) as start_date
,max(date) as end_date
from
(
	select
	t1.*
	,sum(coalesce(t1.day_lag_flg,0)) over (order by t1.date rows unbounded preceding) interval_id
	from
	(
		select 
		t1.*
		,case when date_part('day',date)-date_part('day',lag1_date)!=1 then 1 else 0 end day_lag_flg
		from date_changes t1
	) t1
) t1
where date>='2020-01-01' and date<='2020-12-31'
group by 1,2

--Percent Calculations

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
FROM '/Users/ouonomos/git_repo/redshift_dev/test_data/user_timeevents.csv' DELIMITERS E'\t' 
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

--Answer:

select
t2.state
,count(distinct case when t1.save_home_date is not null then t1.userid end) uusers_w_saved_home
,count(distinct t1.userid) unique_users
,round(100.0 * count(distinct case when t1.save_home_date is not null then t1.userid end) 
/count(distinct t1.userid),1) pct_uusers_w_saved_home
from user_open_save t1
left join user_state t2 on t1.userid=t2.userid
group by 1
order by 1
;




