-- 5.3 Loading Fact Tables

-- Part 1: Create Fact Table
create table default.sales_all_years (
	RowID smallint,
	ClientID int,
	OrderID int,
	OrderDate date,
	OrderMonthYear date,
	Quantity int,
	Quote float,
	DiscountPct float,
	Rate float,
	SaleAmount float,
	CustomerName string,
	CompanyName string,
	Sector string,
	Industry string,
	City string,
	ZipCode string,
	State string,
	Region string,
	ProjectCompleteDate date,
	DaystoComplete int,
	ProductKey string,
	ProductCategory string,
	ProductSubCategory string,
	Consultant string,
	Manager string,
	HourlyWage float,
	RowCount int,
	WageMargin float);
	

-- 5.3.1 go from cleansed to the production database

use default;

insert overwrite table default.sales_all_years
select RowID,
	ClientID,
    OrderID,
    OrderDate,
    OrderMonthYear,
    Quantity,
    Quote,
    DiscountPct,
    Rate,
    SaleAmount,
    CustomerName,
    CompanyName,
    Sector,
    Industry,
    City,
    ZipCode,
    State,
    Region,
    ProjectCompleteDate,
    DaysToComplete,
	ProductKey,
    ProductCategory,
    ProductSubCategory,
    Consultant,
    Manager,
    HourlyWage,
    RowCount,
    WageMargin
from back_office.cleansed_sales

-- 5.3.2 do a row count after it loads and audit the count

insert into table audit.audit_log
select 'default'
, 'sales_all_years'
, 'row count'
, ''
, 'orderid'
, count(*)
, from_unixtime(unix_timestamp())
from default.sales_all_years
