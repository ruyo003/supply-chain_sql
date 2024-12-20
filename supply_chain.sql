select *
From logistics;

-- remove duplicates
-- check for null values
-- standardise data
-- remove any unnecessary rows and columns
-- Exploratory Data Analysis(EDA)

create table logistics_staging
like logistics;


SELECT *
From logistics_staging;

INSERT logistics_staging
select *
From logistics;

-- checking for duplicates


WITH DuplicateRows AS (
	SELECT *,
		ROW_NUMBER() OVER(PARTITION BY `Product ID`, `Order ID`, `Order Date`, 
        City, Supplier, Warehouse
        ORDER BY `Order ID`) AS RowNum
	From logistics_staging
    )
SELECT *
From DuplicateRows
WHERE city = "Berlin";

CREATE TABLE `logistics_staging3` (
  `Product ID` text,
  `Order ID` text,
  `Order Date` text,
  `Promised Delivery Date` text,
  `Actual Delivery Date` text,
  `On-Time Delivery` text,
  `Order Cycle Time` int DEFAULT NULL,
  `Status` text,
  `Return Reason` text,
  `Product Category` text,
  `Quantity Ordered` int DEFAULT NULL,
  `Unit Price` double DEFAULT NULL,
  `Total Cost` double DEFAULT NULL,
  `Cost of Goods Sold (COGS)` double DEFAULT NULL,
  `Shipping Cost` double DEFAULT NULL,
  `Region` text,
  `City` text,
  `Supplier` text,
  `Warehouse` text,
  `Inventory on Hand` int DEFAULT NULL,
  `Inventory Turnover` double DEFAULT NULL,
  `Safety Stock` int DEFAULT NULL,
  `Order_Priority` text,
  `Lead_Time` int DEFAULT NULL,
  `Unit Cost` double DEFAULT NULL,
  `RowNum` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
From logistics_staging3;

INSERT INTO logistics_staging3
SELECT *,
		ROW_NUMBER() OVER(PARTITION BY `Product ID`, `Order ID`, `Order Date`, 
        City, Supplier, Warehouse
        ORDER BY `Order ID`) AS RowNum
	From logistics_staging;
    
 -- Disabling safe update and delete mode temporarily 
SET SQL_SAFE_UPDATES = 0;
    
DELETE
From logistics_staging3
WHERE RowNum >1;

-- re-enabling safe update mode 
SET SQL_SAFE_UPDATES = 1;


-- standardizing data 

Alter Table logistics_staging3
rename column `Return Reason` TO return_reason;

SELECT DISTINCT `Product Category`
From logistics_staging3
ORDER BY 1;
-- checking/converting date format and other data types 
SELECT DISTINCT `Order Date`
FROM logistics_staging3
Limit 10;

SELECT `Order Date`,
       CASE 
            WHEN `Order Date` LIKE '%/%/%' THEN STR_TO_DATE(`Order Date`, '%m/%d/%Y')
           WHEN `Order Date` LIKE '%-%-%' THEN STR_TO_DATE(`Order Date`, '%Y-%m-%d')
           ELSE NULL
       END AS order_date
FROM logistics_staging3;


ALTER TABLE logistics_staging3
MODIFY COLUMN `Order Date` DATE,
MODIFY COLUMN `Actual Delivery Date` DATE,
MODIFY COLUMN `Promised Delivery Date` DATE,
MODIFY COLUMN `Unit Price` DECIMAL(10,2),
MODIFY COLUMN `Total Cost` DECIMAL(10,2),
MODIFY COLUMN `Cost of Goods Sold (COGS)` DECIMAL(12,2),
MODIFY COLUMN `Shipping Cost` DECIMAL(10,2),
MODIFY COLUMN `Inventory Turnover` DECIMAL(10,2),
MODIFY COLUMN `Unit Cost` DECIMAL(10,2);

-- Checking for null values 
SELECT * 
FROM logistics_staging3
WHERE `Order Date` IS NULL 
   OR `Quantity Ordered` IS NULL 
   OR `Unit Cost` IS NULL 
   OR `Total Cost` IS NULL;

   
SELECT * 
FROM logistics_staging3
WHERE `Promised Delivery Date` IS NULL
AND  `Actual Delivery Date` IS NULL;

ALTER TABLE logistics_staging3
DROP RowNum;


-- EDA
SELECT *
From logistics_staging3;

SELECT MIN(`Order date`), MAX(`Order Date`)
FROM logistics_staging3;

-- Suppliers Evaluation

SELECT supplier, SUM(`Shipping Cost`)
FROM logistics_staging3
GROUP BY supplier
ORDER BY 2 DESC;

SELECT supplier, SUBSTRING(`Order date`, 1,7) AS MONTH, SUM(`Quantity Ordered`)
FROM logistics_staging3
GROUP BY MONTH, supplier
ORDER BY 3 ASC;

SELECT Supplier, YEAR(`Order date`),SUM(`Lead_Time`)
FROM logistics_staging3
GROUP BY YEAR(`Order date`), Supplier
ORDER BY 3 DESC;

WITH supplier_eval_cte (supplier, years, lead_time) AS
(
SELECT Supplier, YEAR(`Order date`),SUM(`Lead_Time`)
FROM logistics_staging3
GROUP BY YEAR(`Order date`), Supplier
ORDER BY 3 DESC

)
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY lead_time DESC) AS Ranking
FROM supplier_eval_cte
ORDER BY 4 ASC;

-- Monthly COGS Rolling total 

SELECT MAX(`Cost of Goods Sold (COGS)`)
FROM logistics_staging3;

SELECT *
From logistics_staging3
WHERE `Cost of Goods Sold (COGS)` = '36556.49';


WITH rolling_total_cte AS
(
SELECT SUBSTRING(`Order date`, 1,7) AS MONTH, SUM(`Cost of Goods Sold (COGS)`) AS total_cogs
FROM logistics_staging3
GROUP BY MONTH
ORDER BY 1 ASC
)

SELECT MONTH, total_cogs, SUM(total_cogs) OVER(ORDER BY `MONTH`)
FROM rolling_total_cte;

-- creating relevant columns
SELECT
	`Order ID`,
    `Order Date`,
    `On-Time Delivery`,
    CASE 
		WHEN `On-Time Delivery` = 'Yes' THEN 1
        WHEN `On-Time Delivery` = 'No' THEN 0
        ELSE 'Unkown'
	END AS Ontime
FROM logistics_staging3;

ALTER TABLE logistics_staging3
ADD COLUMN Ontime INT;

-- disabling safe mode 
SET SQL_SAFE_UPDATES = 0;

-- creating the ontime delivery column with yes=1 and No=0
UPDATE logistics_staging3
SET Ontime = 
	CASE
		WHEN `On-Time Delivery` = 'Yes' THEN 1
        WHEN `On-Time Delivery` = 'No' THEN 0
        ELSE 'Unkown'
	END;

-- Performing relevant calculations  
SELECT
   COUNT(`On-Time Delivery`) AS Total_deliveries
FROM logistics_staging3;

SELECT
   SUM(ontime) AS ontime_deliveries
FROM logistics_staging3;

SELECT 
    COUNT(CASE WHEN `Ontime` = 0 THEN 1 END) AS Delays
FROM logistics_staging3;

SELECT 
    COUNT(CASE WHEN `Status` = 'returned' then 1 END) AS Returned
FROM logistics_staging3;

SELECT
	SUM(`Unit Price` * `Quantity Ordered`) AS Revenue
FROM logistics_staging3;


SELECT DISTINCT(Status)
FROM logistics_staging3;

