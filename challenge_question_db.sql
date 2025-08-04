use challenge1;

show tables;

select * from members;
select * from sales;
select * from menu;

-- 1. What is the total amount each customer spent at the restaurant?
select m.customer_id, count(s.product_id) as total_order_on_A , sum(me.price) as total_price 
from members m
left join sales s
on m.customer_id = s.customer_id 
left join menu me
on s.product_id = me.product_id
group by m.customer_id
order by total_price desc;

-- 2. How many days has each customer visited the restaurant?
-- distinct order_dates per customer_id
SELECT 
  customer_id,
  COUNT(DISTINCT order_date) AS visit_days
FROM sales
GROUP BY customer_id;

-- 3. What was the first item from the menu purchased by each customer?
-- profuct_name, product_id, customer_id, order_date asc
SELECT customer_id, product_id, product_name, order_date
FROM (
    SELECT 
        s.customer_id,
        m.product_id,
        m.product_name,
        s.order_date,
        ROW_NUMBER() OVER (
            PARTITION BY s.customer_id 
            ORDER BY s.order_date ASC
        ) AS rn
    FROM sales s
    JOIN menu m ON s.product_id = m.product_id
) AS ranked
WHERE rn = 1;


-- 4. What is the most purchased item on the menu and how many times was it purchased by all customers?

product_id which has count(product_id) as order_number_of_purchase 
order by order_number_of_purchase desc

select s.product_id, m.product_name, count(s.product_id) as order_number_of_purchase 
from menu m 
join sales s
on s.product_id = m.product_id
group by s.product_id, m.product_name
order by order_number_of_purchase desc
limit 1;

-- 5. Which item was the most popular for each customer?


(select s.customer_id, s.product_id ,count(s.product_id) as popular_product
from sales s
Group by s.customer_id, s.product_id
order by popular_product) 





-- 6. Which item was purchased first by the customer after they became a member?

use challenge1;

select * from 
(select s.customer_id, s.order_date, s.product_id,
row_number() over( partition by s.customer_id order by s.order_date asc)  as rw
from sales s
join members m 
on s.customer_id = m.customer_id
where s.order_date >= m.join_date )
as row_num
-- limit 1;
where rw=1;


-- 7. Which item was purchased just before the customer became a member?
-- logic--last item from menu table where (left join) where customer is not member 
-- steps: sapai customer le kineko produdt ani order_date le, 


SELECT *
FROM (
    SELECT s.customer_id, s.product_id, s.order_date,
           ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.order_date DESC) AS rn
    FROM sales s
    LEFT JOIN members m ON s.customer_id = m.customer_id
    WHERE s.order_date <= m.join_date
) AS query_rw
WHERE rn = 1;

-- 8. What is the total items and amount spent for each member before they became a member?
-- count of product id , sum(menu), first_day as they dis transaction as menber
exlude the product_id and price when mem.join_date >= sales.order_date

select distinct(s.customer_id), s.order_date,  

-- (select count(m.product_id), sum(m.price), s.customer_id
-- from sales s
-- left join menu m 
-- on s.product_id = m.product_id
-- where m.join_date >= s.order_date)


-- let's find all transac tion done by customer before becoming a member

Select s.customer_id, count(s.product_id) as total_items , sum(m.price) as total_amount
from sales s
left join members mem
on s.customer_id  = mem.customer_id 
join menu m on s.product_id = m.product_id
where  s.order_date < mem.join_date
group by s.customer_id;


-- 9.  If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?

create  temporary table menu_points  as 
select *, 
case
when product_name ='sushi'  then 20
else 10
end as point_per_dollor
from menu; 
select * from menu_points;

-- which customer has spend how much of total amount 


select s.customer_id, sum(mp.price * mp.point_per_dollor) as total_pints_ear, sum(mp.price) as total_price 
from menu_points mp
join sales s 
on mp.product_id = s.product_id 
group by s.customer_id;



-- 10. In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?

select s.customer_id, 
sum (
 case 
	when s.order_date between m.join_date and date_add (m.join_date, Interval 6 day)
	then mp*2* 
	case 
		when mp.product_name = 'sushi' then 10 
	end
 else mp.price *
	case mp.product_name = 'sushi' then 20 
	else 10 
	end 
 end 
 )as total_points_jan
 from sales s 
 join menu mp on s.product_id = mp.product_id 
 where 
	 s.customer_id in ( A, B)  and 
     s.order_date between '2021-1-1'  and '2021-1-31'
     group by 
     s.customer_id;
 )
 
 
 
 
 
 
 
 SELECT 
    s.customer_id,
    SUM(
        CASE
            -- If within 7 days of joining (inclusive), all items earn 2x
            WHEN s.order_date BETWEEN m.join_date AND DATE_ADD(m.join_date, INTERVAL 6 DAY) THEN mp.price * 2 * 
                CASE 
                    WHEN mp.product_name = 'sushi' THEN 10
                    ELSE 10
                END
            -- After 7 days, sushi = 20, others = 10
            ELSE mp.price * 
                CASE 
                    WHEN mp.product_name = 'sushi' THEN 20
                    ELSE 10
                END
        END
    ) AS total_points_jan
FROM 
    sales s
JOIN 
    menu mp ON s.product_id = mp.product_id
JOIN 
    members m ON s.customer_id = m.customer_id
WHERE 
    s.customer_id IN ('A', 'B') AND
    s.order_date BETWEEN '2021-01-01' AND '2021-01-31'
GROUP BY 
    s.customer_id;







-- from sale, c.order_date (only first 7 days) 




merge target_table  as t
using source_table s 
on s.join_key = t.join_key 
when matched then update 
set t.price = s.price, t.product_name = s.product_name 
when not matched by target_table then 
insert values(s.product_id, s.product_name, s_price)
when not matched by source  then 
delete ;
