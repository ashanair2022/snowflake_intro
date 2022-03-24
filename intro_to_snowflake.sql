//First couple of steps
//Execute queries one by one by placing the cursor within a query and pressing ctrl-enter

//Use a role suitable for managing privileges
USE ROLE SECURITYADMIN;

//Create the data engineer role, grant it privileges on the warehouse
//Then grant the role to yourself
CREATE ROLE DATA_ENGINEER;
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ENGINEER;
GRANT ROLE DATA_ENGINEER TO USER <YOUR_USERNAME>;

//Let's try to use the role to create a database to store our transformations

USE ROLE DATA_ENGINEER;
CREATE DATABASE INTERMEDIATE_STAGE;

//The error message we are getting shows that we are dealing with an account level permission issue
//Let's grant it the ability to create a database
USE ROLE SECURITYADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE DATA_ENGINEER;


//The reason the above command failed is that some privileges like this can be 
//granted only by specific roles, some can be granted by SYSADMIN and others only by ACCOUNTADMIN
USE ROLE SYSADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE DATA_ENGINEER;


//NOW IT'S TIME TO CREATE OUR DATABASE AND TABLES
USE ROLE DATA_ENGINEER;
CREATE DATABASE INTERMEDIATE_STAGE;

//Now just before we start transforming data let's talk about one more thing related to permissions
//If you change back to SYSADMIN and try to use the database we have just created you will notice that it's not visible.
//So how do we enable our administrators to see objects that less privileged roles make? With inheritance.
//https://docs.snowflake.com/en/user-guide/security-access-control-overview.html#role-hierarchy-and-privilege-inheritance

USE ROLE SYSADMIN;
USE DATABASE INTERMEDIATE_STAGE;

//Solution can go here
USE ROLE SECURITYADMIN;
GRANT ROLE DATA_ENGINEER TO ROLE SYSADMIN;


//Now the Sysadmin should be able to oversee our work.
//Run the commands in lines 39-40 to validate that.

//To continue we are going to create two more necessary objects within our database
//A schema and a table.
//There won't be a permission issue since the data_engineer role is the owner of the database and has all the privileges on it.
//You can hover above the database name on the list to confirm.


//When creating objects you can either use the full namespace like INTERMEDIATE_STAGE.TPCH_SF10
//Or use the appropriate object before executing a command.
USE ROLE DATA_ENGINEER;
CREATE SCHEMA INTERMEDIATE_STAGE.TPCH_SF10;
CREATE SCHEMA INTERMEDIATE_STAGE.TPCH_SF100;

CREATE TABLE INTERMEDIATE_STAGE.TPCH_SF10.URGENT_ORDERS_PER_CUSTOMER(
CUSTOMER_KEY NUMBER,
CUSTOMER_NAME TEXT,
ORDER_COUNT NUMBER
);

CREATE TABLE INTERMEDIATE_STAGE.TPCH_SF100.URGENT_ORDERS_PER_CUSTOMER(
CUSTOMER_KEY NUMBER,
CUSTOMER_NAME TEXT,
ORDER_COUNT NUMBER
);

INSERT INTO INTERMEDIATE_STAGE.TPCH_SF10.URGENT_ORDERS_PER_CUSTOMER
WITH STAGE AS (
SELECT C.C_CUSTKEY, C.C_NAME, O.O_ORDERPRIORITY, COUNT(O.O_ORDERKEY) as ORDER_COUNT
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."CUSTOMER" as C
JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."ORDERS" as O ON C.C_CUSTKEY = O.O_CUSTKEY
GROUP BY 1,2,3)
SELECT C_CUSTKEY, C_NAME, ORDER_COUNT
FROM STAGE
WHERE O_ORDERPRIORITY = '1-URGENT';

INSERT INTO INTERMEDIATE_STAGE.TPCH_SF100.URGENT_ORDERS_PER_CUSTOMER
WITH STAGE AS (
SELECT C.C_CUSTKEY, C.C_NAME, O.O_ORDERPRIORITY, COUNT(O.O_ORDERKEY) as ORDER_COUNT
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."CUSTOMER" as C
JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."ORDERS" as O ON C.C_CUSTKEY = O.O_CUSTKEY
GROUP BY 1,2,3)
SELECT C_CUSTKEY, C_NAME, ORDER_COUNT
FROM STAGE
WHERE O_ORDERPRIORITY = '1-URGENT';

//This does what we want it to do temporarily but the next day we will need to add only new orders somehow.
//To overcome this we can create a view.

CREATE VIEW INTERMEDIATE_STAGE.TPCH_SF10.URGENT_ORDERS_PER_CUSTOMER_VIEW AS
WITH STAGE AS (
SELECT C.C_CUSTKEY, C.C_NAME, O.O_ORDERPRIORITY, COUNT(O.O_ORDERKEY) as ORDER_COUNT
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."CUSTOMER" as C
JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."ORDERS" as O ON C.C_CUSTKEY = O.O_CUSTKEY
GROUP BY 1,2,3)
SELECT C_CUSTKEY, C_NAME, ORDER_COUNT
FROM STAGE
WHERE O_ORDERPRIORITY = '1-URGENT';

CREATE VIEW INTERMEDIATE_STAGE.TPCH_SF100.URGENT_ORDERS_PER_CUSTOMER_VIEW AS
WITH STAGE AS (
SELECT C.C_CUSTKEY, C.C_NAME, O.O_ORDERPRIORITY, COUNT(O.O_ORDERKEY) as ORDER_COUNT
FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."CUSTOMER" as C
JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF100"."ORDERS" as O ON C.C_CUSTKEY = O.O_CUSTKEY
GROUP BY 1,2,3)
SELECT C_CUSTKEY, C_NAME, ORDER_COUNT
FROM STAGE
WHERE O_ORDERPRIORITY = '1-URGENT';

//Let's see the difference in execution time
SELECT *
FROM INTERMEDIATE_STAGE.TPCH_SF10.URGENT_ORDERS_PER_CUSTOMER_VIEW
WHERE ORDER_COUNT > 3

SELECT *
FROM INTERMEDIATE_STAGE.TPCH_SF100.URGENT_ORDERS_PER_CUSTOMER_VIEW
WHERE ORDER_COUNT> 3


//now let's execute this query for all the different sizes
USE SCHEMA snowflake_sample_data.tpch_sf1;   -- or snowflake_sample_data.{tpch_sf10 | tpch_sf100 | tpch_sf1000}

SELECT
       l_returnflag,
       l_linestatus,
       SUM(l_quantity) AS sum_qty,
       SUM(l_extendedprice) AS sum_base_price,
       SUM(l_extendedprice * (1-l_discount)) AS sum_disc_price,
       SUM(l_extendedprice * (1-l_discount) * (1+l_tax)) AS sum_charge,
       AVG(l_quantity) AS avg_qty,
       AVG(l_extendedprice) AS avg_price,
       AVG(l_discount) AS avg_disc,
       COUNT(*) AS count_order
 FROM
       lineitem
 WHERE
       l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
 GROUP BY
       l_returnflag,
       l_linestatus
 ORDER BY
       l_returnflag,
       l_linestatus;
       
//For loads that are taking too much time we can create and allocate to our users bigger warehouses
USE ROLE SYSADMIN;
CREATE WAREHOUSE COMPUTE_WH_HEAVY warehouse_size=large initially_suspended=true auto_suspend=60;
USE ROLE SECURITYADMIN;
GRANT ALL ON WAREHOUSE COMPUTE_WH_HEAVY TO ROLE DATA_ENGINEER;

USE ROLE DATA_ENGINEER;
USE WAREHOUSE COMPUTE_WH_HEAVY;
//Let's see how much time it takes now to execute the query for tpch_sf1000


//Final step is to see how we can manage data governance 
//in the reporting layer for various roles

//First let's create a table containing the results of our analytics query
CREATE SCHEMA INTERMEDIATE_STAGE.TPCH_SF1000;
CREATE TABLE INTERMEDIATE_STAGE.TPCH_SF1000.REPORTING AS
SELECT
       l_returnflag,
       l_linestatus,
       SUM(l_quantity) AS sum_qty,
       SUM(l_extendedprice) AS sum_base_price,
       SUM(l_extendedprice * (1-l_discount)) AS sum_disc_price,
       SUM(l_extendedprice * (1-l_discount) * (1+l_tax)) AS sum_charge,
       AVG(l_quantity) AS avg_qty,
       AVG(l_extendedprice) AS avg_price,
       AVG(l_discount) AS avg_disc,
       COUNT(*) AS count_order
 FROM
       snowflake_sample_data.tpch_sf1.lineitem
 WHERE
       l_shipdate <= dateadd(day, -90, to_date('1998-12-01'))
 GROUP BY
       l_returnflag,
       l_linestatus
 ORDER BY
       l_returnflag,
       l_linestatus;
       
//
USE ROLE SECURITYADMIN;      
CREATE ROLE MASKING_ADMIN;

GRANT CREATE masking policy ON SCHEMA "INTERMEDIATE_STAGE"."TPCH_SF1000" TO ROLE MASKING_ADMIN;

//Change to account admin for an account level privilege
USE ROLE ACCOUNTADMIN;
GRANT apply masking policy ON account TO ROLE MASKING_ADMIN;

//Change back to security admin for a routine privilege assignment
//From the below code just keep in mind the creation pf the masking policy.
//We are not interested in the permissions grants.
USE ROLE SECURITYADMIN;
GRANT ROLE MASKING_ADMIN TO USER <YOUR_USERNAME>;
GRANT USAGE ON DATABASE INTERMEDIATE_STAGE TO ROLE MASKING_ADMIN;
GRANT USAGE ON SCHEMA INTERMEDIATE_STAGE.TPCH_SF1000 TO ROLE MASKING_ADMIN;

USE ROLE MASKING_ADMIN;
USE DATABASE INTERMEDIATE_STAGE;
USE SCHEMA TPCH_SF1000;

CREATE OR REPLACE masking policy numbers_mask AS (val number) RETURNS number ->
  CASE
    WHEN current_role() IN ('SYSADMIN') THEN val
    ELSE 1111
  END;
  
//Let's apply this to our reporting table
ALTER TABLE IF EXISTS INTERMEDIATE_STAGE.TPCH_SF1000.REPORTING MODIFY COLUMN sum_qty SET masking policy numbers_mask;

//Alternate between data_engineer and sysadmin roles while clicking preview in "INTERMEDIATE_STAGE"."TPCH_SF1000"."REPORTING" and see what comes up.
