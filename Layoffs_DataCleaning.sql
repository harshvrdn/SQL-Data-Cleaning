-- Data Cleaning
-- This is a project for raw data cleaning and sorting in SQL.
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


-- Importing and Viewing the content of the raw csv file.
SELECT *
FROM layoffs;

-- Creating another table that we can work on, to make sure we don't alter the raw data. 
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- 1. Removing Duplicates

-- Doing PARTITION BY to check for duplicates in the data
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Creating a CTE for duplicate funtion
WITH Duplicate_CTE AS
(
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM Duplicate_CTE
WHERE row_num > 1;

-- Lets check if the CTE is working fine or not
SELECT *
FROM layoffs_staging
WHERE company = 'Cazoo';

-- So we know that the CTE is working fine, we now want to delete the duplicate rows.
-- Creating another staging table

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

-- Inserting into LAYOFFS_STAGING2 the data we gathered by partitioning data of raw file in LAYOFFS_STAGING.
INSERT INTO layoffs_staging2
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;

-- Deleting the duplicate values.
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

-- Now we have deleted the duplicate values from the raw data.

-- 2. Standardizing Data

-- Triming data
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2;

-- Making sure that the names are correctly lablled
SELECT DISTINCT(company)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET company = 'Ada'
WHERE company LIKE 'Ada%' AND industry = 'Support';

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Updating the date type property
SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- 3. Dealing with NULL values

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;


SELECT DISTINCT(industry), company, location
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

-- Trying to find if any other line item might have any industry for the companies
SELECT *
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
    AND T1.location = T2.location
WHERE (T1.industry IS NULL OR T1.industry = '')
AND T2.industry IS NOT NULL;

-- Updating the industry for JUUL
UPDATE layoffs_staging2
SET industry = 'Consumer'
WHERE company = 'Juul';

SELECT *
FROM layoffs_staging2;

-- 4. Deleting any unnecessary data

-- Since we won't be needing any data with null values in layoff metric, we can remove it.
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- Since we don't need the row_num column, so we will drop it.

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


-- Finally, we have our clean data for analysis.

SELECT *
FROM layoffs_staging2;

-- End of project.