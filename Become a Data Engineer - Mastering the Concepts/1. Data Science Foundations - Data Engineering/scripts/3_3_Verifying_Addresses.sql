-- 3.3 Verifying Addresses

-- on command line
	hadoop fs -mkdir /data/zipcode

-- load zipcode table
	hadoop fs -put zip_code_database.csv /data/zipcode

use default;

create table zipcode_lookup(
    zipcode string,
    type string,
    decommissioned string,
    primary_city string,
    acceptable_cities string,
    unacceptable_cities string,
    state string,
    county string,
    timezone string,
    area_codes string,
    world_region string,
    country string,
    latitude float,
    longitude float,
    irs_estimated_population_2014 bigint)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = '"',
  "escapeChar"    = "\\"
)
location '/data/zipcode'
tblproperties('skip.header.line.count'='1', 'serialization.null.format'='');

-- Find all rows where city or state differ

select  s.city as s_city
    ,z.primary_city as z_city
    , s.state as s_state
    , z.state as z_state
    from back_office.cleansed_sales s left join
    zipcode_lookup z on s.zipcode = z.zipcode
    where s.City <> z.primary_city or s.state <> z.state

-- Fill in zip codes from zipcode look up table

insert overwrite table back_office.cleansed_sales
select  RowID,
	null, --client id
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
    coalesce(z.primary_city, s.city) as City,
    s.ZipCode,
    coalesce(z.state, s.state) as State,
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
    from back_office.cleansed_sales s 
    left join default.zipcode_lookup z on s.zipcode = z.zipcode

