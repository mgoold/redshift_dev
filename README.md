# Redshift Devlopment Practice

## This repo is dedicated to doing postgresql code and includes the python code for faking the data to use in practice.
### Assumptions
This practice assumes that: 
* you've run the notebooks to create the redshift data to load into the tables.  The related code for each exercise 
* you've gone through the set up of postgresql on your computer, so that you can load the practice data per the included sql code.  I recommend this [one](https://www.youtube.com/watch?v=zOL-_UHw510), which will get you from download on a mac to adding data to a table:   .  Note that there are copy statements to load your data in each of the below sections as well.  Personally, I found the free trial of Postico to be an easy way to get started.

### Repo Organization
The sections below are organized around the data used to generate them, and somewhat on the topical area (e.g. dealing with session data).
Each section below has the following pattern:
* jupyter notebook used to generate practice data
* practice data sheet name
* sql file with related practice
* questions covered in this section

## Sections
### Working with session data.
* Notebook to Generate Code: [test_sessiondata.ipynb](https://github.com/mgoold/redshift_dev/blob/main/test_code/test_sessiondata.ipynb)
* Sql file with related practice: [test_sessiondata.sql](https://github.com/mgoold/redshift_dev/blob/main/test_code/test_sessiondata.sql)
* Practice Data Sheet Name: test_sessiondata
* Topics in This Section:
  * Creating Session ID on Raw Session Data
  * Given 2 data sets, write me a query that combines the datasets but excludes customers who are in both.

### Working with user timed event data.
* Notebook to Generate Code: [user_timeevents.ipynb](https://github.com/mgoold/redshift_dev/blob/main/test_code/user_timeevents.ipynb)
* Sql file with related practice: [user_timedevents.sql](https://github.com/mgoold/redshift_dev/blob/main/test_code/user_timedevents.sql)
* Practice Data Sheet Name: user_timeevents
* Topics in This Section:
  * Calculate percentage metrics
  * Calculate average date difference

### Working with company user event data.
* Notebook to Generate Code: [test_companyuserdata.ipynb](https://github.com/mgoold/redshift_dev/blob/main/test_code/test_companyuserdata.ipynb)
* Sql file with related practice: [test_companyuserdata.sql](https://github.com/mgoold/redshift_dev/blob/main/test_code/test_companyuserdata.sql)
* Practice Data Sheet Name: test_companyuserdata
* Topics in This Section:
  * Catching duplicate rows.
  * Reviewing duplicates by rank
  * Finding Nth Row
  * Finding even or odd rows
  * Finding rows with value above average

### Working with tree data.
* Notebook to Generate Code: [tree_text_for_sql.ipynb](https://github.com/mgoold/redshift_dev/blob/main/test_code/tree_text_for_sql.ipynb)
* Sql file with related practice: [tree_text_for_sql.sql](https://github.com/mgoold/redshift_dev/blob/main/test_code/tree_text_for_sql.sql)
* Practice Data Sheet Name: tree_text_for_sql
* Topics in This Section:
  * distinguishing root, inner, and leaf nodes

### Working with tree data.
* Notebook to Generate Code: [user_retention_test.ipynb](https://github.com/mgoold/redshift_dev/blob/main/test_code/user_retention_test.ipynb)
* Sql file with related practice: [user_retention_test.sql](https://github.com/mgoold/redshift_dev/blob/main/test_code/user_retention_test.sql)
* Practice Data Sheet Name: user_retention_test
  * finding users who are entering for the first time
  * finding users who have not been seen for n days
  * finding users retained from previous month





