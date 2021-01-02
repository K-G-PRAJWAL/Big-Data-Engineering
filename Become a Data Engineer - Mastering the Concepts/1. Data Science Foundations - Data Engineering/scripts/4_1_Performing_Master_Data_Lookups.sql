
-- Part 1: Create Client Tables

create table back_office.stage_clients (
    ClientID            string,
    Name                string,
    Symbol              string,
    LastSale            string,
    MarketCapLabel      string,
    MarketCapAmount     string,
    IPOyear             string,
    Sector              string,
    industry            string,
    SummaryQuote        string
)
row format serde 'com.bizo.hive.serde.csv.CSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = '"',
  "escapeChar"    = "\\"
)
location '/data/clients/'
tblproperties('serialization.null.format'='');

-- Part 2: Add data types for clients table

create table back_office.cleansed_clients (
    ClientID            int,
    Name                string,
    Symbol              string,
    LastSale            double,
    MarketCapLabel      string,
    MarketCapAmount     bigint,
    IPOyear             int,
    Sector              string,
    industry            string,
    SummaryQuote        string
)

-- Part 3: Load cleansed_clients
insert into back_office.cleansed_clients
select    cast(ClientID as int) as ClientID,
    Name,
    Symbol,
    LastSale,
    MarketCapLabel,
    cast(MarketCapAmount as bigint) as MarketCapAmount,
    cast(IPOyear as int) as IPOyear,
    Sector              string,
    industry            string,
    SummaryQuote        string 
from back_office.stage_clients


-- Part 4: Update sales table with clientIDs
insert overwrite table back_office.cleansed_sales
select  RowID,
	c.ClientID,
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
    s.Sector,
    s.Industry,
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
    from back_office.cleansed_sales s
    left join back_office.cleansed_clients c on s.companyname = c.name;
