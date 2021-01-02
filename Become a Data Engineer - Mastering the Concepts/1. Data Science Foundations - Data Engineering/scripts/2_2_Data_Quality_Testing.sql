-- 2.2 Data Quality Testing

-- 2.2.1

-- do a count where orderid is blank
insert into table audit.audit_log
select 'back_office'
, 'stage_sales'
, 'null business key count'
, 'business key is orderid'
, 'orderid'
, count(*)
, from_unixtime(unix_timestamp())
from back_office.stage_sales
where orderid = '';

-- where orderdate is blank
insert into table audit.audit_log
select 'back_office'
, 'stage_sales'
, 'null value'
, 'orderdate is null table key is orderid'
, orderid
, cast(null as int)
, from_unixtime(unix_timestamp())
from back_office.stage_sales
where orderdate = '';

-- where saleamount is blank
insert into table audit.audit_log
select 'back_office'
, 'stage_sales'
, 'null value'
, 'saleamount is null table key is orderid'
, orderid
, cast(null as int)
, from_unixtime(unix_timestamp())
from back_office.stage_sales
where saleamount = '';

-- 2.2.2

-- where city is missing

insert into table audit.audit_log
select 'back_office'
, 'stage_sales'
, 'null value'
, 'city is null table key is orderid'
, orderid
, cast(null as int)
, from_unixtime(unix_timestamp())
from back_office.stage_sales
where city = '';

-- where state is missing

insert into table audit.audit_log
select 'back_office'
, 'stage_sales'
, 'null value'
, 'state is null table key is orderid'
, orderid
, cast(null as int)
, from_unixtime(unix_timestamp())
from back_office.stage_sales
where state = '';

-- where zipcode is missing

insert into table audit.audit_log
select 'back_office'
, 'stage_sales'
, 'null value'
, 'zipcode is null table key is orderid'
, orderid
, cast(null as int)
, from_unixtime(unix_timestamp())
from back_office.stage_sales
where zipcode = '';

-- 2.2.3

-- where orderdate is invalid
insert into table audit.audit_log
select 'back_office'
, 'stage_sales'
, 'null/invalid value'
, 'orderdate is null/invalid table key is orderid'
, orderid
, cast(null as int)
, from_unixtime(unix_timestamp())
from back_office.stage_sales
where from_unixtime(unix_timestamp(orderdate, 'yyyy-MM-dd')) is null;

-- where projectcompletedate is invalid
insert into table audit.audit_log
select 'back_office'
, 'stage_sales'
, 'null/invalid value'
, 'projectcompletedate is null/invalid table key is orderid'
, orderid
, cast(null as int)
, from_unixtime(unix_timestamp())
from back_office.stage_sales
where from_unixtime(unix_timestamp(projectcompletedate, 'yyyy-MM-dd')) is null;

