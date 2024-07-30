SELECT *
FROM layoffs;

-- STEPS --
-- 1. REMOVE DUPLICATES
-- 2. STANDARDIZE THE DATA
-- 3. NULL VALUES OR BLANK VALUES
-- 4. REMOVE ANY COLUMNS

-- CREATE A TABLE THAT YOU CAN ALTER. DON'T ALTER THE RAW DATA

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

-- INSERT VALUES FROM layoffs TABLE to layoffs_staging

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging; 

-- 1. REMOVE DUPLICATES

	# We will add row numbers to the table to unique rows

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

	# Filter row numbers to check if there are duplicates by creating a CTE
    
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1
;

	# Confirm that the duplicates are indeed duplicates

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

	# After realizing that some of the companies are really close to eachother, we are going to add row nummber based on all the column names
    
SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte 
WHERE row_num > 1
;

	#Re-checking again

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

	# Trying to remove the duplicates by creating another table with additional row row_num and delete all of them that have a 2

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` bigint DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

INSERT layoffs_staging2
SELECT *,
ROW_NUMBER() 
OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

DELETE 
FROM layoffs_staging2
WHERE row_num = 2;

SELECT * 
FROM layoffs_staging2
WHERE row_num > 1
;

-- STANDARDIZING DATA

SELECT *
FROM layoffs_staging2;

	#Trimming company name to remove the spaces

SELECT company, TRIM(company)
FROM layoffs_staging2;

	#Updating the trimmed company name into the table

UPDATE layoffs_staging2
SET company = TRIM(company);

	#Checking industry - updating duplicate crypto names

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; 

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country = 'United States.'; 

	# Converting date into date time format
    
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

	#Checking stage
    
SELECT *
FROM layoffs_staging2;
    
SELECT DISTINCT stage
FROM layoffs_staging2
ORDER BY 1;

-- WORKING WITH NULLS and BLANKS

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *	
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';
    
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2;

-- Considering removing rows

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

ALTER TABLE layoffs_staging2
MODIFY COLUMN percentage_laid_off INT;



