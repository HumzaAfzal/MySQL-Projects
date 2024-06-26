-- Data Cleaning Project
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022/data

-- Steps to clean the data:
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Address Null Values or blank values
-- 4. Remove unnecessary Rows or Columns


SELECT * 
FROM world_layoffs_dataset;


-- Create staging table
CREATE TABLE layoffs_staging
LIKE world_layoffs_dataset;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM world_layoffs_dataset;


--  1. Remove Duplicates
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

# CTE to identify duplicate values
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num > 1;


# Creating another table to delete duplicate values using filtering on row_num column
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

# Copying data from layoff_staging table
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

# Deleting duplicate values
DELETE  
FROM layoffs_staging2
WHERE row_num > 1;

# Confirming duplicate values are deleted
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;



-- 2. Standardizing the Data
SELECT * 
FROM layoffs_staging2;


# Removing unnecessary space before or after company names
SELECT company, (TRIM(COMPANY))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);


# Setting "Cryptocurrency" and "Crypto currency" industry values to Crypto
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2;


# Checking location column values
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;


# Removing period at the end of some United States country values
SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


# Converting date column datatype from text to date
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;


-- 3. Address Null Values or blank values

# Identify rows with blank or null industry values
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

# Set blank values to nulls
UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

# Identify rows with null industry values which can be populated
SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

# Populate null industry values
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;



-- 4. Remove unnecessary Rows or Columns

# Identify rows who have NULL values for total_laid_off and percentage_laid_off columns
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
 
# Delete rows who have NULL values for total_laid_off and percentage_laid_off columns
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# Delete row_num column as it is no longer needed
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;







