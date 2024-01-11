## Three channels - distributer , retail , direct 
## platforms - brick and Mortal , E-commerce

## profit and loss statement 

## pre-invoice deduction , promotional offers + placement fees (product location) + 
## performance rebate (good sales) = post-invoice deduction

# datawarehouse 
 -- SALES SOFTWARE , SURWAYS, CUSTOMER RELATIONSHIP MANAGEMENT (SOFTWARE ENGINEERS) >>> ETL 
 -- (ETL) CURRENCY normalisaton , derived columns , aggregation >>> DATAWAREHOUSE (DATA ENGINEERS) , ANALYTICS, DS ML 
 -- 
# ETL 
# OLTP - online transaction processing 
# OLAP- online analytical processing

select distinct region from dim_customer;

# PRODUCT HEIRARCHY - DIVISION -> SEGMENT -> CATEGORY -> PRODUCT -> VARIENT

# fiscal year = september to august 

# PROJECT MANAGEMENT METHODOLOGY - AGILE DEVELOPMENT ---> SCRUM AND KANBAN

-- croma india fy 2021
-- Month
-- Product name 
-- varient
-- sold quantity
-- gross price per item
-- gross price total 

select * from dim_customer
where customer like "%croma%"  and  market = "India";

-- croma india customer id = 90002002

select * from fact_sales_monthly
where customer_code = 90002002
	and date between '2020-9-1' and '2021-8-1'
    order by date desc;
    
-- 9,10,11 --> Q1
-- 12,1,2 --> Q2
-- 3,4,5  --> Q3
-- 6,7,8  --> Q4

# product, variant, gross_price per item, gross price total 

select sm.*, p.product, p.variant, g.gross_price, 
		ROUND((sm.sold_quantity * g.gross_price),2) as gross_price_total
 from fact_sales_monthly sm
join dim_product p on p.product_code = sm.product_code
join fact_gross_price g 
on 
	g.product_code = sm.product_code and 
	g.fiscal_year = get_fiscal_year(sm.date)
where 
customer_code = 90002002 and
	get_fiscal_year(date) = 2021 
    order by date asc;
 
select gbd0041.get_fiscal_quarter("2020-09-02");
select date_add("2020-10-1", interval 4 month);

select month("2019-5-16");

   
## where 
-- customer_code = 90002002 and
	-- get_fiscal_year(date) = 2021 and 
    -- get_fiscal_quarter(date) = "Q3"
    -- order by date asc

-- TASK 2 : 1. Month 2. total gross sales amount to croma india in this month

# customer code croma = 90002002

SELECT s.date, 
round(sum(g.gross_price* s.sold_quantity),2) as total_sales
 FROM fact_sales_monthly s
 join fact_gross_price g
 on s.product_code  = g.product_code and 
 get_fiscal_year(s.date) = g.fiscal_year 
where customer_code = 90002002
group by s.date
order by s.date asc;

# gengerate a yearly report for croma india where there are two columns
# 1. fiscal year 2. total gross sales amount in that year from croma

SELECT get_fiscal_year(s.date) as fiscal_year, 
round(sum(g.gross_price* s.sold_quantity),2) as total_sales
 FROM fact_sales_monthly s
 join fact_gross_price g
 on s.product_code  = g.product_code and 
 get_fiscal_year(s.date) = g.fiscal_year 
where customer_code = 90002002
group by get_fiscal_year(s.date)
order by fiscal_year asc;


---------------## STORED PROSEDURE ##----------------------------------------------------
# Create a stored proc that can determine the market badge based on the following logic
# if total sold quantity > 5 million that market is considered gold else silver.

# input : market, fiscal year   output: market_badge  -- India , 2020 ----> GOLD

select sum(s.sold_quantity) as sold
from fact_sales_monthly s
join dim_customer c
on s.customer_code = c.customer_code
where get_fiscal_year(s.date) = 2021 and market = "Indonesia"
group by c.market;


# 9. PROBLEM STATEMENT AND PRE-INVOICE DISCOUNT REPORT

# TOP MARKET , TOP PRODUCTS , TOP CUSTOMERS
WITH CTE AS (
select sm.*, p.product, p.variant, g.gross_price, 
		ROUND((sm.sold_quantity * g.gross_price),2) as gross_price_total,
        pre.pre_invoice_discount_pct
 from fact_sales_monthly sm
join dim_product p on p.product_code = sm.product_code
join fact_gross_price g 
on 
	g.product_code = sm.product_code and 
	g.fiscal_year = sm.fiscal_year
JOIN fact_pre_invoice_deductions pre
on 
	pre.customer_code = sm.customer_code AND
	pre.fiscal_year = sm.fiscal_year
where 
	sm.fiscal_year = 2021 
    order by date asc)
    SELECT *,
    (gross_price_total - gross_price_total * pre_invoice_discount_pct) as net_invoice_sale 
    FROM CTE;

# VIEWS INTRODUCTION 

select *, 
	(1 -  pre_invoice_discount_pct)* gross_price_total as net_invoice_sale,
    (po.discounts_pct + po.other_deductions_pct) as post_invoice_discount_pct
    from sales_preinv_discount sm
 join fact_post_invoice_deductions po 
 on sm.date = po.date and
 sm.product_code = po.product_code and 
 sm.customer_code = po.customer_code;
 
 
 select * , 
		(1- post_invoice_discount_pct) * net_invoice_sale as net_sales
 from sales_postinv_discount;    
    
    select * from net_sales;
    
   select s.date, s.fiscal_year, s.customer_code, 
			s.product_code, c.customer, c.market, 
            p.product, p.variant, s.sold_quantity,
            g.gross_price as gross_price_per_item,
            (s.sold_quantity * g.gross_price) as gross_price_total
   from fact_sales_monthly s
   join  fact_gross_price g
		on s.product_code = g.product_code and 
        s.fiscal_year = g.fiscal_year
   join  dim_customer c
		on s.customer_code = c.customer_code
   join  dim_product  p
        on s.product_code = p.product_code;
  
  
  ## TOP MARKETS
  
    select 
		market, 
        round(sum(net_sales) / 1000000,2) as net_sales_mln
    from gdb0041.net_sales
    where fiscal_year = 2020
    group by market
    order by net_sales_mln desc
    limit 5;

 ## TOP CUSTOMER
select * from net_sales;
select 
		CUSTOMER, 
        round(sum(net_sales) / 1000000,2) as net_sales_mln
    from gdb0041.net_sales n 
    join dim_customer c 
    on n.customer_code = c.customer_code
    where fiscal_year = 2021
    group by customer
    order by net_sales_mln desc
    limit 5;

## TOP PRODUCTS 

select 
	 n.PRODUCT,        
	 round(sum(net_sales) / 1000000, 2) as net_sales_mln
from gdb0041.net_sales n 
join dim_product p 
    on n.product_code = p.product_code
    where fiscal_year = 2021
group by n.product
order by net_sales_mln desc
limit 5;

##  WINDOW FUNCTIONS 
# OVER CLAUSE
#65800
# overall percentage

select *, amount *  100 / sum(amount) over() as pct 
 from expenses  
 order by category;

# % per category

select *, amount * 100 / sum(amount) over(partition by category) as pct 
 from expenses  
 order by category;

# CUMULATIVE BY CATEGORY

select date, category , sum(amount) 
	over(partition by category order by date) as total_amount_category
	from expenses  
order by category, date;

## NET SALE GLOBAL MARKET SHARE IN % 

-- A BAR CHART FOR FY-2021 FOR TOP MARKETS BY % NET SALES

with cte1 as (
select 
		CUSTOMER, 
        round(sum(net_sales) / 1000000,2) as net_sales_mln
    from gdb0041.net_sales n 
    join dim_customer c 
		on n.customer_code = c.customer_code
    where fiscal_year = 2021  
    group by customer
    )
    select *, net_sales_mln * 100 / sum(net_sales_mln) over() as pct from cte1 
    group by customer
    order by net_sales_mln desc;

-- NET SALES BY REGION

with cte1 as (
select 
		CUSTOMER, c.region, 
        round(sum(net_sales) / 1000000, 2) as net_sales_mln
    from gdb0041.net_sales n 
    join dim_customer c 
		on n.customer_code = c.customer_code
    where fiscal_year = 2021  
    group by customer, region
    )
    select *, net_sales_mln * 100 / sum(net_sales_mln) over(partition by region) as pct from cte1 
   order by region, net_sales_mln desc;

select s.*, c.region , concat(s.market, " - ", c.region) as region_market
 from net_sales s
 join dim_customer c 
 on c.customer_code = s.customer_code;
 
 --------------------  ## RANK, DENCE RANK ROW NUMBER -------------------------------------
 
 ## SHOW TOP  2 EXPENCES IN EACH CATEGORY
 with cte1 as(
 SELECT *,
	row_number () over(partition by category order by amount desc ) as rn,
	dense_rank () over(partition by category order by amount desc) as dnrk,
    rank () over(partition by category order by amount desc) as rnk
 FROM expenses
 ORDER BY category asc)
 select * from cte1 where rnk <= 2;
 
 select * from student_marks;
 
 SELECT *,
	row_number () over(order by marks desc ) as rn,
	dense_rank () over(order by marks desc) as dnrk,
    rank () over(order by marks desc) as rnk
 FROM student_marks;
 
 # TOP N PRODUCTS IN WHICH EACH DIVISION BY THEIR QUANTITY SOLD

with cte1 as( 
select p.division, p.product , sum(sold_quantity) total_quantity,
dense_rank () over(partition by division order by sum(sold_quantity) desc) as rnk
  from fact_sales_monthly s
join dim_product p
	on s.product_code = p.product_code
    where fiscal_year = 2021
 group by p.product, p.division
 )
 select *
	from cte1 
	where rnk <= 3;
 
# TOP 2 MARKETS IN EVERY REGION BY THEIR GROSS SALES AMOUNT IN FY = 2021
 with cte1 as(
 SELECT s.market, c.region, 
	 round(sum(gross_price_total) / 1000000, 2 ) as gross_sales_mln
	 FROM 
	 gross_sales s
	 join dim_customer c 
	 on s.customer_code = c.customer_code
	 where fiscal_year = 2021
	 group by s.market, c.region),
     cte2 as 
     (
 select *,
 dense_rank () over(partition by region order by gross_sales_mln desc) as rnk
 from cte1)
 select * from cte2 
 where rnk <= 2;
 
 ---------------------------- FORECAST (SUPPLY_CHAIN) -------------------------------------
 
 create table fact_act_est
 (
 SELECT 
  s.date,
  s.product_code,
  s.customer_code,
  s.sold_quantity,
  f.forecast_quantity
 FROM  fact_sales_monthly s
 left join fact_forecast_monthly f 
 using (date, product_code, customer_code)
union
SELECT 
f.date,
  f.product_code,
  f.customer_code,
  s.sold_quantity,
  f.forecast_quantity
	 FROM  fact_forecast_monthly f
	 left join fact_sales_monthly s
	 using (date, product_code, customer_code)
 );
 
 update fact_act_est
 set sold_quantity = 0
 where sold_quantity is null;
 
 
  update fact_act_est
 set forecast_quantity = 0
 where forecast_quantity is null;
 
 ----------------------- TRIGGERS --------------------------------------------------------
 
show triggers;

------------------- DATABASE_EVENTS ------------------------------------------------------

# SQL EVENT TO EXECUTE DATA ON PARTICULAR TIME 
# DATABASE SHCEDULE MAINTANANCE
# GENERATING AGGREGATED DATA LIKE TRIGGERS
# CLEAR LOGS - 

---------------------- FORECAST ACCURACY (1-abs_err_pct)-----------------------------------------------------
with cte1 as
(
Select 
	customer_code, 
	sum(sold_quantity) as total_sold, sum(forecast_quantity) as total_forecast,
	sum((forecast_quantity - sold_quantity)) as net_err,
    sum((forecast_quantity - sold_quantity)) * 100 / sum(forecast_quantity) as net_err_pct,
    sum(abs(forecast_quantity - sold_quantity)) as abs_err,
    sum(abs(forecast_quantity - sold_quantity)) * 100 / sum(forecast_quantity) as abs_err_pct
    from fact_act_est s
where s.fiscal_year = 2021 
group by customer_code)
select e.*, c.customer, c.market,
	if (abs_err_pct > 100,0,100-abs_err_pct) as forecast_accuracy 
from cte1 e
	join dim_customer c
	on c.customer_code = e.customer_code
order by forecast_accuracy  desc ;
	
---------------------- temporary_table ---------------------------------------

# it is valid only for the current session in new tab
# cte is valid for the specific statement 

---------------- CTE, TEMPORARY_TABLE ------------------------------------------

# which customers forecast accuracy has dropped from 2020 to 2021.
# columns : customer code, customer name, market, forecast_accuracy_2020, forecast_accuracy_2021

create table f2021 (
with cte1 as
(
Select 
	customer_code, 
	sum(sold_quantity) as total_sold, sum(forecast_quantity) as total_forecast,
	sum((forecast_quantity - sold_quantity)) as net_err,
    sum((forecast_quantity - sold_quantity)) * 100 / sum(forecast_quantity) as net_err_pct,
    sum(abs(forecast_quantity - sold_quantity)) as abs_err,
    sum(abs(forecast_quantity - sold_quantity)) * 100 / sum(forecast_quantity) as abs_err_pct
    from fact_act_est s
where s.fiscal_year = 2021 
group by customer_code)
select e.*, c.customer, c.market,
	if (abs_err_pct > 100,0,100-abs_err_pct) as forecast_accuracy 
from cte1 e
	join dim_customer c
	on c.customer_code = e.customer_code
order by forecast_accuracy  desc ) ;

create table f2020 (
with cte1 as
(
Select 
	customer_code, 
	sum(sold_quantity) as total_sold, sum(forecast_quantity) as total_forecast,
	sum((forecast_quantity - sold_quantity)) as net_err,
    sum((forecast_quantity - sold_quantity)) * 100 / sum(forecast_quantity) as net_err_pct,
    sum(abs(forecast_quantity - sold_quantity)) as abs_err,
    sum(abs(forecast_quantity - sold_quantity)) * 100 / sum(forecast_quantity) as abs_err_pct
    from fact_act_est s
where s.fiscal_year = 2020
group by customer_code)
select e.*, c.customer, c.market,
	if (abs_err_pct > 100,0,100-abs_err_pct) as forecast_accuracy 
from cte1 e
	join dim_customer c
	on c.customer_code = e.customer_code
order by forecast_accuracy  desc ) ;

# columns : customer code, customer name, market, forecast_accuracy_2020, forecast_accuracy_2021

select a.customer_code, a.customer, a.market, 
	a.forecast_accuracy as f2020 , b.forecast_accuracy as f2021
from f2020 a
	join f2021 b 
	on a.customer_code = b.customer_code 
where a.forecast_accuracy > b.forecast_accuracy
order by f2020 asc ;

---------------------------------------------------------------------------------------

# SUBQUERY VS CTE - CTE IS MORE READABLE THAN SUN QUERY - READABLE, REUSABLE, RECURSION SUPPORT.
# WE CAN REFER CTE IN OTHER CTE. NESTED CTE.alter
# SUB QUERY CAN BE USED IN SELECT, WHERE CLUASE  

# TEMPORARY_TABLE VS VIEWS




 
























