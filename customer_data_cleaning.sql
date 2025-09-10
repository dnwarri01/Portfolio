-- DATA CLEANING PROJECT: CUSTOMERS TABLE

SELECT * 
FROM customers_raw; 

-- 1. REMOVE DUPLICATES 
-- 2. STANDARDIZE THE DATA 
-- 3. HANDLE NULLS OR BLANK VALUES
-- 4. REMOVE IRRELEVANT COLUMNS/ROWS
-- 5. CREATE CLEAN FINAL TABLE

---------------------------------------------------------
-- 1. REMOVE DUPLICATES
---------------------------------------------------------

-- First create a staging table so we don’t touch the raw data directly
CREATE TABLE customers_staging 
LIKE customers_raw; 

INSERT INTO customers_staging
SELECT * 
FROM customers_raw;

SELECT * 
FROM customers_staging;

-- Add a row number so we can identify duplicates
SELECT *,
  ROW_NUMBER() OVER(
    PARTITION BY email, first_name, last_name, signup_date
    ORDER BY customer_id
  ) AS row_num
FROM customers_staging;

-- Create a new table with row numbers so we can safely delete duplicates
CREATE TABLE customers_staging2 (
  customer_id INT,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  email VARCHAR(255),
  phone VARCHAR(50),
  country VARCHAR(100),
  signup_date VARCHAR(50),
  age VARCHAR(10),
  row_num INT
);

INSERT INTO customers_staging2
SELECT *,
  ROW_NUMBER() OVER(
    PARTITION BY email, first_name, last_name, signup_date
    ORDER BY customer_id
  ) AS row_num
FROM customers_staging;

-- Check for duplicates
SELECT * 
FROM customers_staging2
WHERE row_num > 1;

-- Delete the duplicates
DELETE 
FROM customers_staging2
WHERE row_num > 1;

-- Confirm deletion
SELECT * 
FROM customers_staging2
WHERE row_num > 1;

---------------------------------------------------------
-- 2. STANDARDIZE THE DATA
---------------------------------------------------------

-- Trim spaces from names
UPDATE customers_staging2
SET first_name = TRIM(first_name),
    last_name  = TRIM(last_name);

-- Make emails lowercase
UPDATE customers_staging2
SET email = LOWER(email);

-- Standardize countries (different spellings into one)
UPDATE customers_staging2
SET country = CASE
    WHEN country IN ('USA', 'U.S.A.', 'United States') THEN 'United States'
    WHEN country IN ('UK', 'England', 'Great Britain') THEN 'United Kingdom'
    ELSE country
END;

-- Clean phone numbers: remove special characters
UPDATE customers_staging2
SET phone = REGEXP_REPLACE(phone, '[^0-9]', '', 'g');

-- Format US phone numbers if they have exactly 10 digits
UPDATE customers_staging2
SET phone = CONCAT('(', SUBSTRING(phone,1,3), ') ',
                   SUBSTRING(phone,4,3), '-',
                   SUBSTRING(phone,7,4))
WHERE LENGTH(phone) = 10;

---------------------------------------------------------
-- 3. HANDLE NULLS OR BLANK VALUES
---------------------------------------------------------

-- Replace NULL or blank countries with "Unknown"
UPDATE customers_staging2
SET country = 'Unknown'
WHERE country IS NULL OR country = '';

-- Replace empty phone with a placeholder
UPDATE customers_staging2
SET phone = '000-000-0000'
WHERE phone IS NULL OR phone = '';

-- Convert signup_date and age into proper data types
ALTER TABLE customers_staging2 ADD COLUMN signup_date_clean DATE;
ALTER TABLE customers_staging2 ADD COLUMN age_clean INT;

UPDATE customers_staging2
SET signup_date_clean = STR_TO_DATE(signup_date, '%Y-%m-%d'); -- adjust format as needed

UPDATE customers_staging2
SET age_clean = CAST(age AS SIGNED)
WHERE age IS NOT NULL AND age != '';

-- Handle outliers in age (negative or >120)
UPDATE customers_staging2
SET age_clean = NULL
WHERE age_clean < 0 OR age_clean > 120;

---------------------------------------------------------
-- 4. REMOVE IRRELEVANT DATA
---------------------------------------------------------

-- Drop any rows with completely invalid emails
DELETE 
FROM customers_staging2
WHERE email NOT LIKE '%@%';

-- Drop columns we no longer need
ALTER TABLE customers_staging2 
DROP COLUMN row_num,
DROP COLUMN signup_date,
DROP COLUMN age;

---------------------------------------------------------
-- 5. CREATE FINAL CLEAN TABLE
---------------------------------------------------------

CREATE TABLE customers_clean AS
SELECT DISTINCT
  customer_id,
  first_name,
  last_name,
  email,
  phone,
  country,
  signup_date_clean AS signup_date,
  age_clean AS age
FROM customers_staging2;

-- Final check
SELECT * 
FROM customers_clean;

---------------------------------------------------------
-- DONE ✅ : customers_clean is now fully cleaned
---------------------------------------------------------
