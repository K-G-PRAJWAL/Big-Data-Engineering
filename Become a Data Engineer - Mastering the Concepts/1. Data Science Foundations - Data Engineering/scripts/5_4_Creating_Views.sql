-- 5.4 Creating Views

use default;

create view vw_sales_all_years as
select  RowID,
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
    WageMargin,
    datediff(projectcompletedate, orderdate) as ProjectDuration,
    (saleamount * wagemargin) as ProfitAmount
from sales_all_years;

-- now check how your view looks with the added calculated columns

select *
from vw_sales_all_years;

-- 5.3.2 denormalize client dimension onto sales fact

create view vw_sales_flattened as
select  RowID,
	s.ClientID,
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
    WageMargin,
    datediff(projectcompletedate, orderdate) as ProjectDuration,
    (saleamount * wagemargin) as ProfitAmount,
    c.symbol,
    c.lastsale,
    c.marketcaplabel,
    c.marketcapamount,
    c.ipoyear,
    c.summaryquote
from sales_all_years s
left join clients c on s.clientid = c.clientid;

-- now check your view

select *
from vw_sales_flattened;

-- 5.3.3 created aggregated table for monthly metrics

create view vw_sales_monthly as
select OrderMonth
, OrderYear
, FirstOfTheMonth
,count(*) as OrderCount
,sum(SaleAmount) as SalesTotal
,avg(Quantity) as AvgOrderSize
from
(
select month(orderdate) as OrderMonth
,year(orderdate) as OrderYear
,date_add(orderdate,-(day(orderdate)-1)) as FirstOfTheMonth
,SaleAmount
,Quantity
from sales_all_years
) a
group by OrderMonth
, OrderYear
, FirstOfTheMonth

-- now check your view

select *
from vw_sales_monthly;
