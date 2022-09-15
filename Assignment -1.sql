1. #To download the data
 https://github.com/shashank-mishra219/Hive-Class/blob/main/sales_order_data.csv
 
 2. #Once the file is been downloaded, convert the file into CSV and move the file to cloudera with the help of filezilla
 #Once the file is been moved then store the file in hdfs location
 
 #to move the file from local machine to hdfs, first create a dir to which the file should me moved
 
 #create a new directory
 hadoop fs -mkdir /sales
 
 #to move the file from local to hdfs
 hadoop fs -put /'your-file-path'/sales_data.csv /sales
 
 3. Create a internal hive table "sales_order_csv" which will store csv data sales_order_csv .. make sure to skip header row while creating table
 #create your database
 create database sales;
 
 #choose your database
 use sales;

#create your table
create table sales_order
(
ordernumber   int,
quantityordered   int,
priceeach   float,
orderlinenumber   int,
sales   int,
status   string,
qtr_id   int,
month_id   int,
year_id   int,
productline   string,
msrp   string,
productcode   string,
phone   string,
city   string,
state   string,
postalcode   string,
country   string,
territory   string,
contactlastname   string,
contactfirstname   string,
dealsize   string
)
row format delimited
fields terminated by ','
tblproperties ("skip.header.line.count"="1");


4. Load data from hadoop hdfs path into "sales_order"
load data inpath '/sales/sales_data.csv' into table sales_order;

5. Create an internal hive table which will store data in ORC format "sales_order_orc"

create table sales_order_orc
(
ordernumber   int,
quantityordered   int,
priceeach   float,
orderlinenumber   int,
sales   int,
status   string,
qtr_id   int,
month_id   int,
year_id   int,
productline   string,
msrp   string,
productcode   string,
phone   string,
city   string,
state   string,
postalcode   string,
country   string,
territory   string,
contactlastname   string,
contactfirstname   string,
dealsize   string
) 
stored as orc;


 6. Load data from "sales_order" into "sales_order_orc"
 
 #here we load the data already existing table sales_order into orc table
 Insert into table sales_order_orc select * from sales_order;
 
 
 Perform below menioned queries on "sales_order_orc" table :
 
 a. Calculatye total sales per year

select year_id, sum(sales) t_sales from sales_order_orc
group  by year_id;


b. Find a product for which maximum orders were placed

select a.productline,a.quantityordered from sales_order_orc a 
left join  
(select max(quantityordered) max_o from sales_order_orc) b 
on 
(a.quantityordered=b.max_o);


c. Calculate the total sales for each quarter

select qtr_id, min(sales) t_sales from sales_order_orc
group  by qtr_id;


d. In which quarter sales was minimum

select qtr_id, t_sales 
from (
  select qtr_id, sum(sales) t_sales from sales_order_orc
  group  by qtr_id
) min
sort by t_sales asc
 limit 1;

e. In which country sales was maximum and in which country sales was minimum

select country,max(sales) sales from sales_order
group by country
order by sales desc
limit 1
union all
select country,min(sales) sales from sales_order
group by country
order by sales asc
limit 1;

f. Calculate quartelry sales for each city

select city,qtr_id, sum(sales) t_sales from sales_order_orc
group  by city,qtr_id;

h. Find a month for each year in which maximum number of quantities were sold

select year_id, month_id, quantityordered from (
select year_id, month_id, quantityordered ,rank() over (partition by year_id, month_id order by cast(quantityordered as int) desc) y from
( 
SELECT year_id, month_id, sum(quantityordered)  quantityordered from  sales_order 
group by  year_id,month_id   )  x) x
WHERE x.y = 1;
