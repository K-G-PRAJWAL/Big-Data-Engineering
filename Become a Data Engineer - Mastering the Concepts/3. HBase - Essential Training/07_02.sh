
# create Phoenix version of sales table
create view "sales" (
"row" VARCHAR primary key,
"order"."orderID" VARCHAR,
"order"."orderDate" VARCHAR,
"order"."shipDate" VARCHAR,
"order"."shipMode" VARCHAR,
"order"."profit" VARCHAR,
"order"."quantity" VARCHAR,
"order"."sales" VARCHAR);

# see 10 row
select * from "sales" limit 10;

# select top 10 rows by highest profit
select * from "sales" order by "profit" desc limit 10;

# filter results
# identifiers are double quoted and values single quoted
select * from "sales" WHERE "orderID"='CA-2011-100006' limit 10;

# change output format to see all fields
!outputformat vertical
select * from "sales" WHERE "orderID"='CA-2011-100006' limit 10;

!outputformat vertical
# switch output back to normal
!outputformat table

# get distinct list of shipping modes
select distinct "shipMode" from "sales";

# get count of each ship modes
select "shipMode", count(1)
from "sales"
group by "shipMode"
order by count(1) desc;
