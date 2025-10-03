-- # Project Overview  ------------------------------------

-- This project analyzes an Amazon sales dataset containing product details, customer details, location details, sales details  
-- The main objectives are:  
-- Clean and preprocess raw sales data (handling missing values, type conversion, and formatting).  
-- Explore sales distribution across categories, subcategories, and payment methods.    
-- - Generate meaningful insights on customer preferences, pricing patterns, and sales performance.  
-- - Provide visualizations to communicate trends effectively.

-- # Key Findings ---------------------------------------
-- - Electronics and Home&Kitchen dominate sales (87.85% of products rated "Good")
-- - USA generates the highest sales volume $ 8.2M followed by UK ($5M)
-- - Home Theater/TV and Mobile Accessories are top-selling subcategories
-- - Cash on Delivery (COD) & Net Banking are the most popular payment method (27.8%)
-- - Age group 30-39 generates highest sales ($5.3M)
-- - 0%-5% of discount Shares the maxmimum sales amount
-- - Higher discounts slightly increase quantity sold but reduce total revenue
-- - Peak performance (2.58M) year - 2024, but having highest inconsistency in demand .
-- - Peak Month: Highest sales in Month july (2,711,100).
-- - 2024: Highest sales (9.15M), indicating peak performance.
-- - 2025: Sharp drop compared to year 2023 and 2024.

# CREATING DATABASE
CREATE DATABASE AMAZONSALESPROJECT;

USE AMAZONSALESPROJECT;
# CREATING TABLE FOR IMPORT OF DATA 
CREATE TABLE dim_product (product_id VARCHAR(40),product_name VARCHAR(500),
category VARCHAR(300),discounted_price VARCHAR(25),actual_price VARCHAR(25),    
discount_percentage VARCHAR(25),rating VARCHAR(25),rating_count VARCHAR(50),
about_product TEXT,user_id TEXT,user_name TEXT,review_id TEXT,review_title TEXT,review_content TEXT,          
img_link VARCHAR(500),product_link VARCHAR(500)
);
# GETTING THE RAW DATA FROM PANDAS USING SQL ALCHEMY

# CHECHING THE RAW DATA
SELECT * from DIM_PRODUCT ;
set sql_safe_updates = 0 ;

# cleaning the raw data 
update dim_product
set discounted_price = replace(discounted_price ,'₹','');
update dim_product
set actual_price = replace(actual_price ,'₹', '');
update dim_product
set discounted_price = replace(discounted_price ,',','');
update dim_product
set actual_price = replace(actual_price ,',', '');
update dim_product
set discount_percentage = replace(discount_percentage ,'%', '');
update dim_product
set rating_count = replace(rating_count ,',', '');	


# in rating we have a invalid value like "|" so we have to first assign a value to it 
select * from dim_product where  rating  NOT REGEXP '^[0-9]*\.?[0-9]+$';

##to replace "|" unsual value from rating column we check the rating of this 
#particular product from the amazon website and fill that with the rating
# mentioned in the website
update dim_product
set rating = replace(rating , "|", 4);

# filtering out null values from table 
select * from dim_product where rating_count is null ;

# imputing null values since we had already calculated the null values to fill in these using python 
update dim_product
set rating_count = 5179 where rating_count is null ;

# now lets add product_sub_category and product_main_category 
alter table dim_product
add column product_main_category varchar(300);
alter table dim_product
add column product_sub_category varchar(300);

UPDATE dim_product
SET product_main_category = SUBSTRING_INDEX(category, '|', 1);

update dim_product
set product_sub_category = substring_index(substring_index(category,"|",2),"|",-1);

# removeing extra unwanted columns 
alter table dim_product
drop column category,
drop column about_product,
drop column
user_id, drop column 
user_name, drop column 
review_id, drop column 
review_title, drop column
review_content,  drop column
Jimg_link,  drop column
product_link ;

# creating new column difference price

alter table dim_product
add column difference_price decimal(10,2);


# altering data type of actual price and disounted price data type before calculating difference price

ALTER TABLE dim_product
MODIFY actual_price DECIMAL(10,2),
MODIFY discounted_price DECIMAL(10,2);

update dim_product
set difference_price = actual_price - discounted_price;
select * from dim_product;

update dim_product
set discount_percentage = discount_percentage/100;


# altering the data type of rest cleaned columns 
alter table dim_product
modify discount_percentage decimal(10,2),
modify rating decimal (4,2),
modify rating_count int;


# DELETE DUPLICATE ROWS FROM PRODUCT TABLE 
# SINCE THERE IS NOT DUPLICATE IDENTIFIER WE WILL ADD A UNIQUE COLUMN 
# IN TABLE TO DROP THE DUPLICATE ROWS 

ALTER TABLE DIM_PRODUCT
ADD COLUMN ROW_ID INT AUTO_INCREMENT PRIMARY KEY ;

# DROPPING ALL THE DUPLICATE ROWS 
with cal1 as (
select ROW_ID,product_id,product_name, row_number() over (partition by product_id,product_name
ORDER BY ROW_ID )
 as duplicate_rows
from dim_product)
delete FROM DIM_PRODUCT WHERE ROW_ID IN 
(select ROW_ID from cal1 where duplicate_rows > 1);

# dropping the extra column row_id 
alter table dim_product
drop column row_id ;

# adding primary key contraint for product id 
alter table dim_product
add constraint product_key
primary key(product_id);

# CREATING TABLES FOR IMPORTING DATA (FOR LOCATION , CUSTOMER , FACT_SALES)
CREATE TABLE dim_customer (
    customer_id VARCHAR(40) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    gender VARCHAR(30),
    age  INT,
    email  VARCHAR(255) UNIQUE,
    phone VARCHAR(20) UNIQUE,
    country VARCHAR(100),
    state  VARCHAR(100),
    city  VARCHAR(100),
    loyalty_member  VARCHAR(40)
);
CREATE TABLE dim_location (
    location_id   VARCHAR(40) PRIMARY KEY,
    country  VARCHAR(100),
    state  VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    region VARCHAR(100)
);

CREATE TABLE fact_sales (
sale_id VARCHAR(40) PRIMARY KEY,
product_id VARCHAR(40) NOT NULL,
customer_id VARCHAR(40) NOT NULL,
location_id VARCHAR(40) NOT NULL,
order_date_timestamp DATETIME NOT NULL,
order_date DATE NOT NULL,
quantity INT NOT NULL,
unit_price DECIMAL(10,2) NOT NULL,
total_amount DECIMAL(12,2) NOT NULL,
discount_applied DECIMAL(5,2),
payment_method VARCHAR(30),
delivery_status VARCHAR(30),
FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
FOREIGN KEY (location_id) REFERENCES dim_location(location_id)
);
#----------------------------------------------------------------------------------
# EXPLORING THE DATA
#__________________________________________________________________________________


# TOP 5 MAX DISCOUNTED PRODUCT
SELECT 
DISCOUNTED_PRICE ,
PRODUCT_NAME FROM DIM_PRODUCT
ORDER BY DISCOUNTED_PRICE DESC 
LIMIT 5;

# TOP 5 MIN DISCOUNTED PRODUCT
SELECT DISCOUNTED_PRICE,
PRODUCT_NAME FROM DIM_PRODUCT
ORDER BY DISCOUNTED_PRICE ASC 
LIMIT 5 ;

# TOP 5 PRODUCTS BY HIGHEST DIFFERENCE BETWEEN ACTUAL PRICE AND DISCOUNTED PRICE
SELECT 
PRODUCT_NAME , 
ACTUAL_PRICE-DISCOUNTED_PRICE AS PRICE_DIFFERENCE
FROM DIM_PRODUCT
ORDER BY PRICE_DIFFERENCE DESC 
LIMIT 5 ;

# TOP 5 HIGHEST RATED PRODUCT NAME 
SELECT
PRODUCT_NAME ,
RATING FROM DIM_PRODUCT 
ORDER BY RATING DESC 
LIMIT 5 ;

# DISTRIBUTION OF RATING SCORE IN PERCENTAGE 
# CREATING NEW COLUMN FOR RATING SCORE 


ALTER TABLE DIM_PRODUCT
ADD COLUMN RATING_SCORE VARCHAR(25);

UPDATE dim_product
SET rating_score = CASE 
    WHEN rating < 2 THEN 'POOR'
    WHEN rating >= 2 AND rating < 3 THEN 'BELOW_AVERAGE'
    WHEN rating >= 3 AND rating < 4 THEN 'AVERAGE'
    WHEN rating >= 4 AND rating < 5 THEN 'GOOD'
    ELSE 'EXCELLENT'
END;
WITH cal AS (
    SELECT 
        rating_score,
        SUM(rating_count) AS total_count
    FROM dim_product
    GROUP BY rating_score
)
SELECT 
    rating_score,
concat(ROUND((total_count * 100.0 / (SELECT SUM(rating_count) FROM dim_product)), 2),"%") AS percentage
FROM cal
ORDER BY percentage DESC;

#_________________________________________________________________________________

# SUM OF SALES , UNITS SOLD , TOTAL ORDERS BY COUNTRY 
SELECT L.COUNTRY ,SUM(F.TOTAL_AMOUNT) AS SALES , count(distinct F.SALE_ID) AS TOTAL_ORDERS,
SUM(F.QUANTITY) AS UNITS_SOLD FROM FACT_SALES AS F JOIN DIM_LOCATION AS L 
ON F.LOCATION_ID = L.LOCATION_ID GROUP BY L.COUNTRY; 



# SUM OF SALES BY PRODUCT MAIN CATEGORY AND COUNTRY 
WITH CAL AS (
SELECT L.COUNTRY,P.PRODUCT_MAIN_CATEGORY,SUM(S.TOTAL_AMOUNT) AS SALES
FROM FACT_SALES AS S
JOIN DIM_LOCATION AS L ON S.LOCATION_ID = L.LOCATION_ID
JOIN DIM_PRODUCT AS P ON P.PRODUCT_ID = S.PRODUCT_ID
GROUP BY L.COUNTRY, P.PRODUCT_MAIN_CATEGORY),
CAL1 AS (SELECT COUNTRY,PRODUCT_MAIN_CATEGORY,SALES,DENSE_RANK() OVER 
(PARTITION BY COUNTRY 
ORDER BY SALES DESC) AS TOPSALES FROM CAL)
SELECT COUNTRY, PRODUCT_MAIN_CATEGORY, SALES
FROM CAL1
WHERE TOPSALES = 1
ORDER BY COUNTRY;

# HIGHEST SELLING PRODUCT SUBCATEGORY IN EACH COUNTRY
WITH CAL AS (
SELECT SUM(S.TOTAL_AMOUNT) AS SALES , P.PRODUCT_SUB_CATEGORY, L.COUNTRY
FROM FACT_SALES AS S JOIN DIM_PRODUCT AS P ON P.PRODUCT_ID = S.PRODUCT_ID 
JOIN DIM_LOCATION AS L ON L.LOCATION_ID = S.LOCATION_ID 
GROUP BY L.COUNTRY , P.PRODUCT_SUB_CATEGORY ) 
,CAL1 AS (SELECT COUNTRY , PRODUCT_SUB_CATEGORY , SALES , DENSE_RANK() OVER(PARTITION BY 
COUNTRY ORDER BY SALES DESC ) AS TOPSALES FROM CAL)
SELECT COUNTRY , PRODUCT_SUB_CATEGORY, SALES FROM CAL1 WHERE TOPSALES = 1 ;

# HIGHEST SELLING PRODUCT IN EACH CATEGORY
WITH CAL AS (
    SELECT 
        SUM(S.TOTAL_AMOUNT) AS SALES,
        P.PRODUCT_NAME,
        L.COUNTRY
    FROM FACT_SALES AS S
    JOIN DIM_PRODUCT AS P 
        ON S.PRODUCT_ID = P.PRODUCT_ID
    JOIN DIM_LOCATION AS L 
        ON S.LOCATION_ID = L.LOCATION_ID
    GROUP BY L.COUNTRY, P.PRODUCT_NAME), 
CAL1 AS (SELECT COUNTRY, PRODUCT_NAME, SALES,
        DENSE_RANK() OVER(PARTITION BY COUNTRY ORDER BY SALES DESC) AS TOPSALE
    FROM CAL)
SELECT COUNTRY, PRODUCT_NAME, SALES
FROM CAL1 
WHERE TOPSALE = 1;

# GENDER WISE SALES 
SELECT ROUND(SUM(S.TOTAL_AMOUNT)) AS SALES , C.GENDER
FROM FACT_SALES AS S JOIN DIM_CUSTOMER AS C 
ON C.CUSTOMER_ID = S.CUSTOMER_ID
GROUP BY C.GENDER;

# SUM OF SALES BY CUSTOMER'S AGE GROUP
WITH CAL AS (
SELECT S.TOTAL_AMOUNT AS SALES ,
CASE WHEN C.AGE <20 THEN "<20" WHEN C.AGE BETWEEN 20 AND 29 THEN "20-29"
WHEN C.AGE BETWEEN 30 AND 39 THEN "30-39" WHEN C.AGE BETWEEN 40 AND 49 THEN 
"40-49" WHEN C.AGE BETWEEN 50 AND 59 THEN "50-59" ELSE "60+"
END AS AGE_GROUP FROM FACT_SALES AS S JOIN 
DIM_CUSTOMER AS C ON S.CUSTOMER_ID =C.CUSTOMER_ID)
SELECT ROUND(SUM(SALES)) AS SALES, AGE_GROUP
FROM CAL GROUP BY AGE_GROUP ORDER BY SALES DESC;

# SALES BY PAYMENT METHODS 
SELECT CONCAT(ROUND(SUM(TOTAL_AMOUNT)/(SELECT SUM(TOTAL_AMOUNT) FROM FACT_SALES)*100),"%") AS SALES,PAYMENT_METHOD
FROM FACT_SALES GROUP BY PAYMENT_METHOD
ORDER BY SALES DESC;

# SALES BY DISCOUNT PERCENTAGE
SELECT ROUND(SUM(TOTAL_AMOUNT)) AS SALES , CONCAT(DISCOUNT_APPLIED,"%") AS `DISCOUNT %`
FROM FACT_SALES GROUP BY DISCOUNT_APPLIED ORDER BY SALES DESC;

# SALES BY YEARLY SALES 
SELECT ROUND(SUM(TOTAL_AMOUNT)) AS SALES , YEAR(ORDER_DATE) AS YEAR 
FROM FACT_SALES GROUP BY YEAR(ORDER_DATE) ORDER BY SALES DESC;

# SALES BY MONTH 
SELECT 
MONTHNAME(ORDER_DATE) AS MONTH_NAME,ROUND(SUM(TOTAL_AMOUNT)) AS SALES
FROM FACT_SALES
GROUP BY MONTH(ORDER_DATE), MONTHNAME(ORDER_DATE)
ORDER BY MONTH(ORDER_DATE);

# SALES BY QUARTER 
SELECT 
CONCAT('QTR', QUARTER(ORDER_DATE)) AS QUARTER,ROUND(SUM(TOTAL_AMOUNT)) AS SALES
FROM FACT_SALES
GROUP BY QUARTER(ORDER_DATE),CONCAT('QTR', QUARTER(ORDER_DATE))
ORDER BY QUARTER(ORDER_DATE);

# AVERAGE SALES & QUANTITY SOLD VS DISCOUNT %
SELECT ROUND(AVG(TOTAL_AMOUNT)) AS SALES 
,ROUND(AVG(QUANTITY),2) AS QUANTITY , DISCOUNT_APPLIED 
FROM FACT_SALES GROUP BY DISCOUNT_APPLIED
ORDER BY DISCOUNT_APPLIED ;

# SALES & QUANTITY SOLD VS DISCOUNT %
SELECT ROUND(SUM(TOTAL_AMOUNT),2) AS SALES ,ROUND(SUM(QUANTITY),2) AS UNITS_SOLD,
DISCOUNT_APPLIED FROM FACT_SALES GROUP BY DISCOUNT_APPLIED ORDER BY DISCOUNT_APPLIED ASC;

#TOP 5 HIGHEST SELLING PRODUCT ACROSS ALL THE PRODUCTS
SELECT P.PRODUCT_NAME ,ROUND(SUM(S.TOTAL_AMOUNT)) AS SALES FROM
FACT_SALES AS S JOIN DIM_PRODUCT AS P ON P.PRODUCT_ID = S.PRODUCT_ID
GROUP BY P.PRODUCT_NAME ORDER BY SALES DESC LIMIT 5 ;

#TOP 5 HIGHEST SELLING PRODUCT MAIN CATEGORY ACROSS ALL THE MAIN CATEGORIES
SELECT P.PRODUCT_MAIN_CATEGORY , ROUND(SUM(S.TOTAL_AMOUNT)) AS SALES FROM FACT_SALES AS S JOIN 
DIM_PRODUCT AS P ON P.PRODUCT_ID = S.PRODUCT_ID GROUP BY P.PRODUCT_MAIN_CATEGORY ORDER BY SALES DESC LIMIT 5 ;

# TOP 5 HIGHEST SELLING PRODUCT SUB CATEGORY ACROSS ALL THE SUB CATEGORIES
SELECT P.PRODUCT_SUB_CATEGORY , ROUND(SUM(TOTAL_AMOUNT)) AS SALES FROM FACT_SALES AS S JOIN 
DIM_PRODUCT AS P ON P.PRODUCT_ID = S.PRODUCT_ID GROUP BY P.PRODUCT_SUB_CATEGORY ORDER BY SALES DESC LIMIT 5 ;

# SALES BY STATE
SELECT L.STATE,ROUND(SUM(S.TOTAL_AMOUNT)) AS SALES FROM FACT_SALES AS S
JOIN DIM_LOCATION AS L ON S.LOCATION_ID = L.LOCATION_ID
GROUP BY L.STATE
ORDER BY SALES DESC;

# TOP SELLING CITIES IN EACH COUNTRY 
WITH CAL AS (
SELECT L.COUNTRY,L.CITY , ROUND(SUM(S.TOTAL_AMOUNT)) AS SALES FROM FACT_SALES AS S JOIN DIM_LOCATION AS L ON 
L.LOCATION_ID = S.LOCATION_ID GROUP BY L.COUNTRY , L.CITY )
, CAL1 AS ( SELECT COUNTRY ,CITY, SALES ,DENSE_RANK() OVER (PARTITION  BY COUNTRY ORDER BY SALES DESC) AS TOPCITY
FROM CAL)
SELECT COUNTRY,CITY,SALES FROM CAL1 WHERE TOPCITY =1;

#TOP SELLING REGION IN EACH COUNTRY
WITH CAL AS (
SELECT L.COUNTRY,L.REGION , ROUND(SUM(S.TOTAL_AMOUNT)) AS SALES FROM FACT_SALES AS S JOIN DIM_LOCATION AS L ON 
L.LOCATION_ID = S.LOCATION_ID GROUP BY L.COUNTRY , L.REGION )
, CAL1 AS ( SELECT COUNTRY ,REGION, SALES ,DENSE_RANK() OVER (PARTITION  BY COUNTRY ORDER BY SALES DESC) AS TOPREGION
FROM CAL)
SELECT COUNTRY,REGION,SALES FROM CAL1 WHERE TOPREGION =1;










