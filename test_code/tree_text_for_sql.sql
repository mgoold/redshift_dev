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
