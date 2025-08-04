-- 1. create customer table with cid PK,name,email,lastChange Timestamp columns.
-- Note : whenever the sql table is updated or deleted, the lastChange timestamp column should update to current timestamp.
-- 2. insert 3 records in customers table with id 1,2,3.
-- 3. Create customer_backup table same as customer table with same data.
-- 4. update email of cid=1 in customer table
-- 5. delete cid=2 in customer table
-- 6. Insert cid=4 in customer table
-- 7. Use merge statement to update customer_backup table using customer table.
use challenge1;

create table customer (
cid int primary key NOT NULL,
name varchar(20),
email varchar (30),
lastChange_tp timestamp default current_timestamp
);

-- Creating trigger for updating the table 
DELIMITER $$

CREATE TRIGGER trg_customer_update
BEFORE UPDATE ON customer
FOR EACH ROW
BEGIN
    SET NEW.lastChange = CURRENT_TIMESTAMP;
END $$

DELIMITER ;







insert into customer (cid, name, email)
values(1, 'Anjana', 'abc@gmail.com'),
(2, 'Aman', 'xyz@gmail.com'),
(3, 'Ayush', 'tel@gmail.com');

create table  customer_backup like customer;
insert into customer_backup (select * from customer);


update  customer 
set email = 'naya@gmail.com'
where cid= 1;

delete from   customer 
where cid= 2;

insert into customer (cid, name, email)
values(4, 'insert', 'email@email.com');

-- 7. Use merge statement to update customer_backup table using customer table.

select * from customer;
Select * from customer_backup;

MERGE customer_backup AS b
USING customer AS c
ON b.cid = c.cid
WHEN MATCHED THEN
    UPDATE SET 
        b.name = c.name,
        b.email = c.email,
        b.lastChange = c.lastChange
WHEN NOT MATCHED BY TARGET THEN
    INSERT (cid, name, email, lastChange)
    VALUES (c.cid, c.name, c.email, c.lastChange)
WHEN NOT MATCHED BY SOURCE THEN
    DELETE;





