create database data_cleaning_files
--------------------------------Customers---------------------------------------------------------
select * from Customers$


select * into customers_clean from Customers$

select * from customers_clean
select count(distinct Custid) from customers_clean
select count(* )from customers_clean

select count(gender) from customers_clean
where Gender='m'


--deleted those customers which are not in orders table
delete FROM customers_clean
WHERE Custid  not IN (select distinct Customer_id from orders_clean)


--------------------------------Orders Table------------------------------------------------------
select * from orders$

select * into orders_clean from orders$

select * from orders_clean

--make bill_timestamp in proper format
UPDATE orders_clean
SET bill_date_timestamp = TRY_CONVERT(DATETIME, bill_date_timestamp)
WHERE ISDATE(bill_date_timestamp) = 1;

--dlt dates which are not part of analysis.
DELETE FROM orders_clean--------------->(4289 rows affected)
WHERE bill_date_timestamp NOT BETWEEN CAST('2021-09-01' AS DATETIME) AND CAST('2023-10-31' AS DATETIME)

--same orderid having  different dates---->298 rows are there
SELECT order_id,COUNT(DISTINCT bill_date_timestamp) AS different_dates
FROM orders_clean
GROUP BY order_id
HAVING COUNT(DISTINCT bill_date_timestamp) > 1;

---> fixed this issue:
WITH latest_dates AS (
    SELECT order_id, MAX(bill_date_timestamp) AS latest_date
    FROM orders_clean
    GROUP BY order_id
)
UPDATE o
SET o.bill_date_timestamp = l.latest_date
FROM orders_clean o
JOIN latest_dates l ON o.order_id = l.order_id;

--same order ids having different customers
SELECT order_id
FROM orders_clean
GROUP BY order_id
HAVING COUNT(DISTINCT customer_id) > 1;

-->Fixed this issue by removing those order ids for which the total amount and payment value is different
SELECT o.order_id, o.customer_id, ROUND(o. [Total Amount], 2) AS total_amount,  ROUND(p.payment_value, 2) AS payment_value
FROM orders_clean o
JOIN orderpayments_clean p ON o.order_id = p.order_id
WHERE o.order_id IN (
    SELECT order_id
    FROM orders_clean
    GROUP BY order_id
    HAVING COUNT(DISTINCT customer_id) > 1
)
AND ROUND(o. [Total Amount], 2) != ROUND(p.payment_value, 2);

-- now deleting such rows

DELETE o
FROM orders_clean o
JOIN orderpayments_clean  p ON o.order_id = p.order_id
WHERE o.order_id IN (
    SELECT order_id
    FROM orders_clean
    GROUP BY order_id
    HAVING COUNT(DISTINCT customer_id) > 1
)
AND ROUND(o.[Total Amount], 2) != ROUND(p.payment_value, 2);

--fix the cummulative sum of quantity issue
WITH ranked_orders AS (
SELECT *,ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity DESC) AS rn
FROM orders_clean
)
delete o
FROM orders_clean o
JOIN ranked_orders r
    ON o.order_id = r.order_id
   AND o.product_id = r.product_id
   AND o.quantity = r.quantity  -- to target the exact duplicate row
WHERE r.rn > 1 

--fix those issues where total amount <> payment value and keep 3rs margin
-->checking
WITH order_totals AS (
SELECT order_id, ROUND(SUM([Total Amount]), 2) AS order_total
FROM orders_clean
GROUP BY order_id
),
payment_totals AS (
SELECT order_id, ROUND(SUM(payment_value), 2) AS payment_total
FROM orderpayments_clean
GROUP BY order_id
)
SELECT o.order_id, o.order_total, p.payment_total
FROM order_totals o
JOIN payment_totals p ON o.order_id = p.order_id
WHERE o.order_total != p.payment_total and abs(o.order_id-p.order_id)>3

-->fixing problem
WITH order_totals AS (
SELECT order_id, ROUND(SUM([Total Amount]), 2) AS order_total
FROM orders_clean
GROUP BY order_id
),
payment_totals AS (
    SELECT order_id, ROUND(SUM(payment_value), 2) AS payment_total
    FROM orderpayments_clean
    GROUP BY order_id
),
mismatched_orders AS (
SELECT o.order_id
FROM order_totals o
JOIN payment_totals p ON o.order_id = p.order_id
WHERE o.order_total != p.payment_total and abs(o.order_id-p.order_id)>3
)
SELECT o.customer_id,o.order_id,o.product_id,o.channel,o.Delivered_StoreID,o.bill_date_timestamp,o.[Cost Per Unit],
o.mrp,o.discount,
CASE WHEN m.order_id IS NOT NULL THEN 1 ELSE o.quantity END AS quantity,
CASE WHEN m.order_id IS NOT NULL THEN ROUND((o.mrp - o.discount) * 1, 2) ELSE ROUND(o.[Total Amount], 2)END AS total_amount
INTO orders_clean_temp
FROM orders_clean o
LEFT JOIN mismatched_orders m ON o.order_id = m.order_id;

select * from orders_clean_temp t

DELETE FROM orders_clean;

INSERT INTO orders_clean (
customer_id, order_id, product_id, channel,Delivered_StoreID, bill_date_timestamp,[Cost Per Unit],
mrp, discount, quantity, [Total Amount]
)
SELECT customer_id, order_id, product_id, channel, Delivered_StoreID, bill_date_timestamp,
[Cost Per Unit], mrp, discount, quantity, total_amount
FROM orders_clean_temp;

DROP TABLE orders_clean_temp;

WITH order_totals AS (
SELECT order_id, ROUND(SUM([Total Amount]), 2) AS order_total
FROM orders_clean
GROUP BY order_id
),
payment_totals AS (
    SELECT order_id, ROUND(SUM(payment_value), 2) AS payment_total
    FROM orderpayments_clean
    GROUP BY order_id
),
mismatched_orders AS (
SELECT o.order_id
FROM order_totals o
JOIN payment_totals p ON o.order_id = p.order_id
WHERE o.order_total != p.payment_total and abs(o.order_id-p.order_id)>3
)
DELETE FROM orders_clean
WHERE order_id IN (SELECT order_id FROM mismatched_orders);

--deleting order id of orders table which are not in payment table

delete FROM orders_clean
WHERE order_id NOT IN (SELECT DISTINCT order_id FROM orderpayments_clean)

-- checking for same order id but differnt store id where channel is instore

SELECT order_id
FROM orders_clean
WHERE channel = 'Instore'
GROUP BY order_id
HAVING COUNT(DISTINCT Delivered_StoreID) > 1;


-- Get the preferred store_id for each order_id (instore only)
WITH ranked_stores AS (
SELECT order_id, Delivered_StoreID,SUM([Total Amount]) AS total_amt,
RANK() OVER ( PARTITION BY order_id  ORDER BY SUM([Total Amount]) DESC,MIN(Delivered_StoreID) ASC  -- Tie breaker: smaller store_id
        ) AS rk
    FROM orders_clean
    WHERE channel = 'Instore'
    GROUP BY order_id, Delivered_StoreID
),
preferred_store AS (
    SELECT order_id, Delivered_StoreID
    FROM ranked_stores
    WHERE rk = 1
)
UPDATE o
SET Delivered_StoreID = p.Delivered_StoreID
FROM orders_clean o
JOIN preferred_store p
    ON o.order_id = p.order_id
WHERE o.channel = 'Instore';




-------------------------------------Order Payments-------------------------------------
select * from OrderPayments$

select * into orderpayments_clean from OrderPayments$

select * from orderpayments_clean

---deleted payment value=0
delete from orderpayments_clean
where payment_value is null or payment_value =0

---deleted duplicate rows--->(614 rows affected)

with duplicates as(
select *,ROW_NUMBER()over(partition by order_id,payment_type,payment_value order by (select null))as rnk
from orderpayments_clean
) 
delete from duplicates
where rnk>1


---deleted those orderids which are not present in orders table
DELETE FROM orderpayments_clean
WHERE order_id NOT IN (SELECT DISTINCT order_id FROM orders_clean )


--created view so that there cab be unique order ids in order payment
CREATE VIEW orderpayments_clean_v AS
SELECT order_id,
SUM(CASE WHEN payment_type = 'credit_card' THEN payment_value ELSE 0 END) AS credit_card,
SUM(CASE WHEN payment_type = 'UPI/Cash' THEN payment_value ELSE 0 END) AS upi_cash,
SUM(CASE WHEN payment_type = 'debit_card' THEN payment_value ELSE 0 END) AS debit_card,
SUM(CASE WHEN payment_type = 'voucher' THEN payment_value ELSE 0 END) AS voucher,
SUM(payment_value) AS total_payment
FROM orderpayments_clean
GROUP BY order_id

select * from orderpayments_clean_v
------------------------------------Order Review-------------------------------------------
select * from OrderReview_Ratings$

select * into order_review_clean from OrderReview_Ratings$

select * from order_review_clean


--deleted duplicate rows
;WITH duplicate AS(
select  *, ROW_NUMBER() OVER(PARTITION BY order_id, customer_satisfaction_score order by order_id) as rn
from order_review_clean) 
delete from duplicate
where rn>1

--one orderid is having different ratings so will fix it by taking its avg.
SELECT order_id,AVG(customer_satisfaction_score) AS avg_score-->created temp table
INTO #avg_ratings
FROM order_review_clean
GROUP BY order_id;

DELETE FROM order_review_clean;--dlted clean table

INSERT INTO order_review_clean (order_id, customer_satisfaction_score)-->avg rating to the clean table again
SELECT order_id, avg_score
FROM #avg_ratings;

DROP TABLE #avg_ratings;---> dropped temp table

select * from order_review_clean


--delted those orderids which are not present in orders table
DELETE FROM order_review_clean
WHERE order_id NOT IN (SELECT DISTINCT order_id FROM orders_clean)

--------------------------------ProductInfo--------------------------------------------------------
select * from ProductsInfo$

select product_id,Category into products_clean from ProductsInfo$

select * from products_clean


--giving name = Unknown to that category whose value is #N/A
update products_clean set Category='Unknown'--->(623 rows affected)
where Category='#N/A'

----------------------------------StoreInfo ----------------------------------------------------
select * from ['Stores Info$']

select * into stores_clean from ['Stores Info$']

select * from stores_clean

--deleted duplicate rows
;with cte1 as(        ---------------> dlted duplicate storeid 
select * from (
select * , ROW_NUMBER()over(partition by storeid order by (select null)) as rnk 
from stores_clean
) as x 
) 
delete from cte1
where rnk >1

-----------final clean tables
select * from customers_clean
select o.order_id,sum(payment_value),sum([Total Amount]) from orders_clean o
inner join orderpayments_clean as c
on o.order_id=c.order_id
group by o.order_id
having sum([Total Amount])<>sum(payment_value)
order by order_id
select * from orderpayments_clean
select * from order_review_clean
select * from products_clean
select * from stores_clean

select count(distinct order_id)  from orders_clean
select count(distinct order_id)  from orderpayments_clean
select count(distinct order_id)  from order_review_clean