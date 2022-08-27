create database miniproject_2;
use miniproject_2;

########################################################################

#1. Join all the tables and create a new table called combined_table.


CREATE TABLE combined_table AS SELECT
mf.*, cd.Customer_Name, cd.Province, cd.Region, cd.Customer_Segment, od.Order_ID, od.Order_Date, od.Order_Priority,
pd.Product_Category, pd.Product_Sub_Category, sd.Ship_Mode, sd.Ship_Date 
FROM market_fact mf
JOIN cust_dimen cd ON mf.Cust_id = cd.Cust_id
JOIN orders_dimen od ON mf.Ord_id = od.Ord_id
JOIN prod_dimen pd on mf.Prod_id = pd.Prod_id
JOIN shipping_dimen sd ON mf.Ship_id = sd.Ship_id;
SELECT * FROM combined_table;
DESC combined_table;


######################################################################

#2. Find the top 3 customers who have the maximum number of orders

select  mf.cust_id, customer_name,count(distinct ord_id)nn from market_fact mf join cust_dimen cd on 
mf.cust_id = cd.cust_id group by cust_id,customer_name order by nn desc limit 3; 

######################################################################

#3 Create a new column DaysTakenForDelivery that contains the date difference of Order_Date and Ship_Date.

set sql_safe_updates = 0;
UPDATE combined_table SET Ship_Date = STR_TO_DATE(Ship_Date, '%d-%m-%Y');
ALTER TABLE combined_table MODIFY Ship_Date DATE;
UPDATE combined_table SET Order_Date = STR_TO_DATE(Order_Date, '%d-%m-%Y');
ALTER TABLE combined_table MODIFY Order_Date DATE;
ALTER TABLE combined_table ADD COLUMN DaysTakenForDelivery INT;
UPDATE combined_table SET DaysTakenForDelivery = DATEDIFF(Ship_Date, Order_Date);

######################################################################

# 4. Find the customer whose order took the maximum time to get delivered.

SELECT Customer_Name, MAX(DaysTakenForDelivery) FROM combined_table;


######################################################################

# 5. Retrieve total sales made by each product from the data (use Windows function)

SELECT DISTINCT Prod_id, SUM(Sales)OVER(PARTITION BY Prod_id)TotalSales FROM combined_table;


######################################################################

# 6. Retrieve total profit made from each product from the data (use windows function)

SELECT DISTINCT Prod_id, SUM(Profit)OVER(PARTITION BY Prod_id)TotalProfit FROM combined_table;

######################################################################

# 7. Count the total number of unique customers in January and how many of them came back in other months over the entire year in 2011

SELECT * FROM (SELECT (SELECT COUNT(dcn) 
FROM (SELECT DISTINCT Customer_Name dcn 
FROM combined_table 
WHERE MONTHNAME(Order_Date)='January')t1)total_unique_jan,
CASE WHEN(
COUNT(DISTINCT MONTH(Order_Date) IN (2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12) AND YEAR(Order_Date)='2011') )
THEN COUNT(Customer_Name)
ELSE 0 END as Return_Customer
FROM combined_table)t2;

######################################################################

# 8. Retrieve month-by-month customer retention rate since the start of the business.(using views)

create or replace view v1 as 
select cust_id,customer_name,order_date,month(order_Date)abc from combined_table;

select abc Month,100-per Retention_rate from(
select * from (
select *,max(c1) over(partition by abc),c1/max(c1) over(partition by abc)*100 per 
from (select abc,category,count(*)c1 
from(select  cust_id,customer_name,abc, case when dd<=1 then 'retained' when dd>1 then 'irregular' else 'churned' end category
from (select a-month(order_date)dd,cust_id,customer_name,month(order_date)abc 
from (select cust_id,customer_name,monthname(order_date),month(order_date)abc,year(order_date),order_date,
lead(month(order_date))over(partition by cust_id,year(order_date) order by month(order_date))a
from v1)t)t1 order by abc)uff group by abc,category with rollup order by abc)uff2)uff3 where category='churned')uff4;


######################################################################
