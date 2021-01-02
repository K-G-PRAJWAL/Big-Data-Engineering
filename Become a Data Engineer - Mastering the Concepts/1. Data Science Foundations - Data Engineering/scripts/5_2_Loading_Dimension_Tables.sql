-- 5.2 Loading Dimension Tables
use default;

-- Part 1: Create Client Dimension Table
create table clients (
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
);

-- Part 2: Load Client Table

insert overwrite table default.clients
select ClientID,
    Name,
    Symbol,
    LastSale,
    MarketCapLabel,
    MarketCapAmount,
    IPOyear,
    Sector,
    industry,
    SummaryQuote 
from back_office.cleansed_clients
