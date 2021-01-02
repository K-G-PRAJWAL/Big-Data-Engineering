-- 3.2 Handling Missing Values

-- here's filtering the insert with the blank orderids
use back_office;

-- Part 1 : Filter Rows with Missing OrderID

insert overwrite table cleansed_sales
select cast(RowID as smallint) as RowID,
    cast(null as int) as ClientID,
    cast(OrderID as int) as OrderID,
    cast(OrderDate as date) as OrderDate,
    cast(OrderMonthYear as date) as OrderMonthYear,
    cast(Quantity as int) as Quantity,
    cast(Quote as float) as Quote,
    cast(DiscountPct as float) as DiscountPct,
    cast(Rate as float) as Rate,
    cast(SaleAmount as float) as SaleAmount,
    CustomerName,
    CompanyName,
    Sector,
    Industry,
    City,
    ZipCode,
    State,
    Region,
    cast(ProjectCompleteDate as date) as ProjectCompleteDate,
    cast(DaystoComplete as int) as DaysToComplete,
    ProductKey,
    ProductCategory,
    ProductSubCategory,
    Consultant,
    Manager,
    cast(HourlyWage as float) as HourlyWage,
    cast(RowCount as int) as RowCount,
    cast(WageMargin as float) as WageMargin
    from back_office.stage_sales
    where OrderID <> '';


-- Part 2 : Replace Invalid Dates with '9999-12-31'

insert overwrite table cleansed_sales
select  RowID,
	null, -- ClientID
    OrderID,
    case when OrderDate = '' then '9999-12-31' 
         else OrderDate end as OrderDate,
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
    from sales_all_years;

-- Part 3 : Replace blank product keys with #Unknown

insert overwrite table cleansed_sales
select  RowID,
	null, -- ClientID
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
    case when ProductKey = '' then '#Unknown' 
         else ProductKey end as ProductKey,
    ProductCategory,
    ProductSubCategory,
    Consultant,
    Manager,
    HourlyWage,
    RowCount,
    WageMargin
    from sales_all_years;

-- Final Script: Do all 3 in 1 step

insert overwrite table cleansed_sales
select  RowID,
	null, -- ClientID
    OrderID,
    case when OrderDate = '' then '9999-12-31' 
         else OrderDate end as OrderDate,
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
    case when ProductKey = '' then '#Unknown' 
         else ProductKey end as ProductKey,
    ProductCategory,
    ProductSubCategory,
    Consultant,
    Manager,
    HourlyWage,
    RowCount,
    WageMargin
    from back_office.stage_sales
    where OrderID <> '';
