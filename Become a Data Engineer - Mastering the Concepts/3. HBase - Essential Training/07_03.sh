# truncate order date to month
select substr("orderDate",1,7) from "sales" limit 10;

# get count by month
select substr("orderDate",1,7) as mt, count(1) as orders
from "sales"
group by substr("orderDate",1,7)
order by mt
limit 15;

# create new table
create table "monthlyOrders" ( "mt" varchar primary key, "orders" integer );

# upsert data to new table
upsert into "monthlyOrders"
select substr("orderDate",1,7) as mt, count(1) as orders
from "sales"
group by substr("orderDate",1,7)
order by mt;
