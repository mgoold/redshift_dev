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
-- /* use this to see the degree to which multiples have the greatest degree of usage  */
-- /* a common use case would be the ranking of the number of unique items purchased at checkout, by user count */

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
