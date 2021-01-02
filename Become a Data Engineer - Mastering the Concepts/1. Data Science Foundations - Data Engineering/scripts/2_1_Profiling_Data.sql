-- 2.1 Profiling Data

-- read csv data from local to hdfs

-- on command line
	hadoop fs -mkdir /data
	hadoop fs -mkdir /data/sales
	hadoop fs -mkdir /data/clients

-- on command line 
	-- cd to the data folder
	unzip raw.zip

	-- cd to the raw folder
	cd raw

	-- where the raw sales files exist
	hadoop fs -put * /data/sales/

	-- might get warnings, all is well though
	16/11/10 13:34:00 WARN hdfs.DFSClient: Caught exception 
	java.lang.InterruptedException
	at java.lang.Object.wait(Native Method)
	at java.lang.Thread.join(Thread.java:1281)
	at java.lang.Thread.join(Thread.java:1355)
	at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.closeResponder(DFSOutputStream.java:862)
	at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.endBlock(DFSOutputStream.java:600)
	at org.apache.hadoop.hdfs.DFSOutputStream$DataStreamer.run(DFSOutputStream.java:789)


	-- where the clients csv file resides
	hadoop fs -put clients.csv /data/clients

-- run this in hive to install csv serde
-- after putting it in the /user/clouder folder in hdfs

add jar hdfs:///user/cloudera/csv-serde-1.1.2-0.11.0-all.jar;



-- setup back_office database
create database back_office;
use back_office;

create external table stage_sales (
	RowID string,
	OrderID string,
	OrderDate string,
	OrderMonthYear string,
	Quantity string,
	Quote string,
	DiscountPct string,
	Rate string,
	SaleAmount string,
	CustomerName string,
	CompanyName string,
	Sector string,
	Industry string,
	City string,
	ZipCode string,
	State string,
	Region string,
	ProjectCompleteDate string,
	DaystoComplete string,
	ProductKey string,
	ProductCategory string,
	ProductSubCategory string,
	Consultant string,
	Manager string,
	HourlyWage string,
	RowCount string,
	WageMargin string)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = '"',
  "escapeChar"    = "\\"
)
LOCATION '/data/sales/'
tblproperties('skip.header.line.count'='1', 'serialization.null.format'='');

-- create audit table

create database audit;

create table audit.audit_log
(db string,
tbl string,
audit_type string,
message string,
tbl_key string,
cnt int,
ts timestamp);

-- 2.1.2

-- do a row count for the table
insert into table audit.audit_log
select 'back_office'
, 'stage_sales'
, 'table row count'
, cast(null as string)
, 'orderid'
, count(*)
, from_unixtime(unix_timestamp())
from back_office.stage_sales

