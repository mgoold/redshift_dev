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
