--/SQL Cheat Sheet Based on Real Life Situations, and Pesky Interview Questions/
--/Note: postgres-flavored but adaptable to most flavors./
--/Note: have a look, for each question, at the table, which you can trace back to ... if you need to fake the data from scratch./

--/LOADING SECTION/
--/ASSUMES YOU'VE CREATED THE TEXT FILES AS LISTED/
--/SEE <> FOR CODE TO FAKE DATA FROM SCRATCH/
-- /* create table for session sql questions */

drop table if exists test_sessiondata;

CREATE TABLE IF NOT EXISTS test_sessiondata 
(
	userid integer,
	sessionid integer,
	pagetime timestamp without time zone, 
	user_agent_string text
);


COPY test_sessiondata 
FROM '/<mypath>/test_sessiondata.csv' DELIMITERS E'\t' 
CSV HEADER
;

-- /*the sessionizer */
-- / this is a method for creating a unique session id across raw session data/

select
t1.* 
-- postgres doesn't have an ignore nulls option, so you have to use coalesce
-- other thing is to use unbounded preceding, so that it will sum across all rows in the interval, keeping the previous same value across
-- the partition

, sum(coalesce(session_flag,0)) OVER (ORDER BY t1.userid, t1.pagetime RANGE UNBOUNDED PRECEDING) AS sessionid
from 
(
	select
	t1.*
	,case when t1.min_lag>30 then 1 -- here is where you look back to flag based on time interval between events
		when t1.userid<>lag(t1.userid,1) over (order by t1.userid, t1.pagetime) then 1
		end session_flag 
	from
	(
		select
		t1.*
		,DATE_PART('Minute', t1.pagetime::TIMESTAMP - time_lag::TIMESTAMP) min_lag -- subtracting current time from previous row time in minutes
		from
		(
			select
			t1.userid
			,t1.pagetime
			,lag(t1.pagetime,1) over (partition by t1.userid order by t1.pagetime) as time_lag
			from test_sessiondata t1
			order by 1,3
			limit 1000
		) t1
	) t1
) t1
;

-- /* data set exclusion */
--“Question: Given 2 data sets, write me a query that combines the datasets but excludes customers who are in both.”

create table if not exists tab1
as
select * from test_sessiondata order by random() 
limit 100;

create table if not exists tab2
as
select * from test_sessiondata order by random() 
limit 100;

-- /first do it using union and except: /

select *
from
(
	--combine all the rows, and incidentally de-dupe them
Select 
t1.userid
from tab1 t1

UNION -- you could also use “ALL” here if you don’t care about dupes

Select 
t2.userid
from tab2 t2
) t1

EXCEPT --now use except to exclude all those that are in common from both tables

(
Select 
t1.userid
from tab1 t1

INTERSECT --finds all rows that are in common from both tables


Select 
t2.userid
from tab2 t2
)

--“Is there another way to do that query?”
--/Second method: union left anti-joins/

Select 
t1.userid
from tab1 t1
left join tab2 t2 on t1.userid=t2.userid 
where t2.userid is null -- the so called “anti-join”

UNION

Select 
t2.userid
from tab2 t2
left join tab1 t1 on t2.userid=t1.userid 
where t1.userid is null 
;



-- /* create tables for user company table sql questions */

drop table if exists test_companyuserdata;

CREATE TABLE IF NOT EXISTS test_companyuserdata 
(
	userid integer,
	name text,	
	mgr_id integer,	
	dept_id	integer,
	dept text,	
	project varchar(6),
	revenue integer
);


COPY test_companyuserdata 
FROM '/Users/ouonomos/git_repo/redshift_dev/test_data/test_companyuserdata.csv' DELIMITERS E'\t' 
CSV HEADER
;


drop table if exists test2_companyuserdata;

CREATE TABLE IF NOT EXISTS test2_companyuserdata
as 
select userid, name, mgr_id, dept_id, dept from test_companyuserdata;


drop table if exists test3_userproj;

CREATE TABLE IF NOT EXISTS test3_userproj
as 
select userid, project from test_companyuserdata
union all
(select userid, 'proj45' project from test_companyuserdata limit 10)
;

--/*CATCH DUPLICATES|HAVING*/
--Note: Assuming that empid can be duplicated.

select 
t1.userid
,count(*) row_count
from test3_userproj t1
group by 1
having count(*)>1
order by 1 desc
;


-- /* the inside outsky */
-- /* this simple QA query is a staple */
-- /* use this to see which rows have the greatest counts of duplicates  */

select 
departmentcount
,count(distinct userid) unique_users
from
(
	SELECT
	t1.userid
	,count(distinct t1.dept) departmentcount
	FROM test_companyuserdata t1
	group by 1
) t1
group by 1
order by 2 desc
;

--/* Queries using NTH, NO “TOP” OR “LIMIT”*/
--/*“Write a query to find the 3rd highest salary from a table without top or limit keyword.”*/

select
t1.*
from
(
	select
	t1.userid
	,t1.salary
	,dense_rank() over (order by t1.salary desc) sal_rank -- dense rank bc probably there are duplicate salaries
	from test_companyuserdata t1
	order by 2 desc -- could have multiple rows with same salary
) t1
where t1.sal_rank=3
;


--/*MODULO| filter ONLY ODD, or EVEN ROWS*/
--“Write an SQL query to fetch only odd rows from the table.”
--Note: 
--“tell me about the ordering principle for these rows” – bc if you don’t uniquely order them you won’t get same result every run
--“using row_num as index”

select
t1.*
from
(
	select
	t1.userid
	,row_number() over () row_number -- will create a unique num for every row when not partitioned
	from test2_companyuserdata t1
) t1
where t1.row_number % 2=1 -- divide it by 2 and if it’s odd should have 1 as residual
order by t1.row_number
;

--All Rows Above Average 
--Note: case where nested sql is efficient thing.

SELECT
t1.userid
,t1.salary
,avg(t1.salary) over () avg_salary
FROM test_companyuserdata t1 
WHERE t1.salary > (SELECT AVG(t1.salary) FROM test_companyuserdata t1)
;

--/*Group-by All Rows Above Average */
--“Select all rows where employee salary is above dept avg”

select
t1.*
from
(
	SELECT
	t1.userid,
	t1.salary,
	t1.dept,
	avg(t1.salary) over (partition by t1.dept) dept_avt_sal
	FROM test_companyuserdata t1
) t1
where t1.salary>t1.dept_avt_sal -- you can't have windows in where statement
;


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


-- /* create tables for tree hierarchy sql questions */


drop table if exists user_hierarchy;

CREATE TABLE IF NOT EXISTS user_hierarchy 
(
	mgr_id int,
	userid int
);


COPY user_hierarchy 
FROM '/Users/ouonomos/git_repo/reshift_dev/test_data/tree_text_for_sql.csv' DELIMITERS E'\t' 
CSV HEADER
;



--/*YOY METRICS */

WITH year_metrics AS (
  SELECT
    extract(year from day) as year,
    SUM(daily_amount) as year_amount
  FROM sales
  GROUP BY year)

SELECT
  year,
  year_amount,
  LAG(year_amount) OVER (ORDER BY year) AS revenue_previous_year,
  year_amount - LAG(year_amount) OVER (ORDER BY year) as yoy_diff_value,
  ((year_amount - LAG(year_amount) OVER (ORDER BY year) ) /
     LAG(year_amount) OVER (ORDER BY year)) as yoy_diff_perc
FROM year_metrics
ORDER BY 1
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



--/ANNOYING INTERVIEW QUESTION SECTION/


--/* SELECT ALL WHERE NOT QUALIFIED ON SECOND TABLE|ANTIJOIN */
	--See above tables.
	--“SQL query to fetch all the employees who are not working on any project.”
	--Notes: 
	--“Tell me about the data completeness of these tables.  Do both tables have all employees?”

Select 
t1.userid
,t2.project
from test2_companyuserdata t1
left join test3_userproj t2 on t1.userid=t2.userid
where t2.project is null -- again, the anti-join
;

--/* tree node analysis*/

--Each node in the tree can be one of three types#
	--& "Leaf": if the node is a leaf node
	--& "Root": if the node is the root of the tree
	--& "Inner": If the node is neither a leaf node nor a root node.
	
--Write an SQL query to report the type of each node in the tree. Return the result table in any order.
--id is the primary key column for this table.
--Each row of this table contains information about the id of a node and the id of its parent node in a tree. 
--The given structure is always a valid tree. 

--if a row has no parent, it is a root
--if a row has a parent and no children, it is a child
--else it is an inner node

select
t1.*
,sum(nodect) over () totalnodes
from
(
	select
	t1.node_type
	,count(distinct t1.nodeid) nodect
	from
	(
		select 
		t1.userid
		-- basically, using joins to answer: 
			-- does this person manage anyone? 
			-- is this person managed by anyone
		,case when t3.userid is null then 'Root'
		when t3.mgr_id is not null and t2.mgr_id is null then 'Leaf'
		when t3.mgr_id is not null and t2.mgr_id is not null then 'Inner'
		else 'Other' end node_type
		-- note this step is necessary when top of tree (root) is only in userid or mgr_id column.  
			-- in this data set, they are present as the mgr of others, but not as a userid (with no mgr_id)
			-- when evaluating trees, ask about how this situation is represented in the data
		,case when t3.userid is null then t1.mgr_id else t1.userid end nodeid
		from user_hierarchy t1
		left join user_hierarchy t2 on t1.userid=t2.mgr_id -- is the mgr of someone 
		left join user_hierarchy t3 on t1.mgr_id=t3.userid -- someone is the mgr of this person
		where 1=1
	
	) t1
	where 1=1
	group by 1
) t1
;


--/* RUNNING AVERAGE */

SELECT
  day, -- Note: consider whether all days are present
  daily_amount,
  AVG (daily_amount) OVER (ORDER BY day ROWS 6 PRECEDING)
    AS moving_average
FROM sales
DELTA TO PREV TIME|ROW

SELECT
  day,
  daily_amount,
  daily_amount - LAG(daily_amount) OVER (ORDER BY day) AS delta_yesterday_today
FROM sales
;




