use classicmodels

-- Find the customer names , their total order amount and number of orders placed
select c.customerNumber, c.customerName,  
count(distinct o.orderNumber) as total_orders,
sum(od.quantityOrdered * od.priceEach) as total_amount
from customers c
left join orders o  on c.customernumber = o.customernumber
join orderdetails od on o.ordernumber = od.ordernumber
group by c.customerNumber
order by total_amount desc

-- sub query
select customerNumber, customerName,  
count(distinct orderNumber) as total_orders,
sum(total_amount) as total_amount_sum
from ( select c.customerNumber, c.customerName,  
 o.orderNumber,
od.quantityOrdered * od.priceEach as total_amount
from customers c
left join orders o  on c.customernumber = o.customernumber
join orderdetails od on o.ordernumber = od.ordernumber
) as sub
group by customerNumber
order by total_amount_sum desc


-- corelated sub query
select c.customerNumber, c.customerName,  
(select count(distinct o.orderNumber) from orders o 
where o.customerNumber = c.customerNumber) as total_orders,
(select sum(od.quantityOrdered * od.priceEach) from orders o  
join orderdetails od on o.ordernumber = od.ordernumber ) as total_amount
from customers c
group by c.customerNumber

select * from customers

select customername , creditlimit, country, 
row_number()  over (partition by country order by creditlimit desc) as 'RW', 
rank() over (partition by country order by creditlimit desc) as 'Rank1',
dense_rank() over (partition by country order by creditlimit desc) as 'DR' 
from customers
where RW=2


with abc as (
select customername , creditlimit, country, 
row_number()  over (partition by country order by creditlimit desc) as 'RW', 
rank() over (partition by country order by creditlimit desc) as 'Rank1',
dense_rank() over (partition by country order by creditlimit desc) as 'DR' 
from customers
)
select * from abc where RW=2


SELECT OrderNumber, QuantityOrdered,
CASE
    WHEN QuantityOrdered > 30 THEN "The quantity is greater than 30"
    WHEN QuantityOrdered = 30 THEN "The quantity is 30"
    ELSE "The quantity is under 30"
END as 'Remarks'
 FROM OrderDetails;

