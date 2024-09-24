-- Databricks notebook source
drop table if exists demo_db.fire_service_calls_tbl
drop view if exists demo_db;

-- COMMAND ----------

-- MAGIC %fs rm -r /user/hive/warehouse/demo_db.db

-- COMMAND ----------

create database if not exists demo_db

-- COMMAND ----------

create table if not exists demo_db.fire_service_calls_tbl(
  CallNumber integer,
  UnitID string,
  IncidentNumber integer,
  CallType string,
  CallDate string,
  WatchDate string,
  CallFinalDisposition string,
  AvailableDtTm string,
  Address string,
  City string,
  Zipcode integer,
  Battalion string,
  StationArea string,
  Box string,
  OriginalPriority string,
  Priority string,
  FinalPriority integer,
  ALSUnit boolean,
  CallTypeGroup string,
  NumAlarms integer,
  UnitType string,
  UnitSequenceInCallDispatch integer,
  FirePreventionDistrict string,
  SupervisorDistrict string,
  Neighborhood string,
  Location string,
  RowID string,
  Delay float
) using parquet

-- what does using parquet means here??? every database table, internally stores the data in a file. Here we want spark to use parquet format.  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.catalog.clearCache()
-- MAGIC

-- COMMAND ----------

insert into demo_db.fire_service_calls_tbl 
values(1234, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
null, null, null, null, null, null, null, null, null)

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl

-- COMMAND ----------

show databases

-- COMMAND ----------

show tables in demo_db

-- COMMAND ----------

-- There are many ways to insert data into the tables.For now we will insert records into the table from another table

show databases


-- COMMAND ----------

use demo_db;
show tables;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/user/hive/warehouse/demo_db.db/fire_service_calls_tbl")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/demo_db.db/fire_service_calls_tbl", recurse=True)
-- MAGIC

-- COMMAND ----------

truncate table demo_db.fire_service_calls_tbl

-- truncate removes the rows...here we used truncate not remove., since we do not have delete capability in Apache spark.

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl

-- COMMAND ----------

insert into demo_db.fire_service_calls_tbl
select * from global_temp.fire_service_calls_views

-- COMMAND ----------

select * from demo_db.fire_service_calls_tbl

-- COMMAND ----------


