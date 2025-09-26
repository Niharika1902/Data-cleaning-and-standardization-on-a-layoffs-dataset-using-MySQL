-- SQL PROJECT

-- Imported the data 
USE world_layoffs;
SELECT * FROM layoffs;

-- Step 1 : DATA CLEANING

CREATE TABLE layoffs_staging        -- Create a Staging table 
LIKE layoffs;

INSERT layoffs_staging              -- Inserting the data from raw layoffs table 
SELECT * FROM layoffs; 

SELECT * FROM layoffs;              -- Now we are going to use this staging table further not our raw data

-- REMOVING DUPLICATES
SELECT * FROM layoffs_staging;

-- assigning row numbers to check if there are any duplicates
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- let's check if our query is correct for row number greater than 1, for that we are gonna use CTE
WITH duplicate_cte AS
(
 SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
 ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging
 )
 SELECT *
 FROM duplicate_cte
 WHERE row_num > 1; 
 
 -- checking by example
SELECT *
FROM layoffs_staging
WHERE company = 'cazoo'; -- it worked, yayy

-- let's create a staging table 2 to delete the rows having row number 2
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
  row_num INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- created a new staging table, now let's insert data into it including row numbers 
INSERT INTO layoffs_staging2 (
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
 ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
 FROM layoffs_staging
 );
 
-- and it's done, now we can delete the entries having row_num > 1, so let's do that

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;    -- 0 rows returned, I did it, deleted the duplicate entries from the table 

-- STEP 2 - Standardizing data 

UPDATE layoffs_staging2
SET company = TRIM(company);  -- removed the unnecessary spaces from company names

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;                -- found some issues with industry names crypto, let's fix it

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';   -- fixed it

-- now look at country column
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);    -- found some issue with coutry names - united stated, let's fix it

SELECT distinct country
from layoffs_staging2 order by 1;      -- fixed now

-- now let's change the datatype of DATE from text to DATE
UPDATE layoffs_staging2
SET date = STR_TO_DATE(`date`, '%m/%d/%Y');   -- changed the format, still data type is showing as text, so let's modify date column datatype

ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;             -- done now

-- I saw some null values in industry, total_laid_off and percetage laid off, let's check 

SELECT total_laid_off, percentage_laid_off
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;               -- these are useless rows 

SELECT * 
FROM layoffs_staging2 
WHERE industry IS NULL or industry = '';     

SELECT *
FROM  layoffs_staging2  WHERE company = 'Airbnb';         -- it looks like airbnb is a travel, but this one just isn't populated.

-- let's check for other company's using self join and update the null & blank rows
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '') AND (t2.industry IS NOT NULL); 

-- let's first update the blank places to NULL 

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

-- now update
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;    -- so now, its done, we updated the null values in industry

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL or industry = '';   -- oops looks like one row is yet to be updated - 'Bally''s Interactive', I think it's because it only did one layoff

-- as we don't need the rows where total_laid_off and percentage_laid_off both are NULL, we're gonna delete them 

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;    -- deleted

-- we also don't need row_num column anymore, let's drop it 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2; 

-- That's it, we got our finalised clean data. 




















