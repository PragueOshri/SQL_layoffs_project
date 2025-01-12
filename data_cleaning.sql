# Layoffs Project - Data Cleaning 
-- --------------------------------
# 1. Remove duplicates if there are any
# 2. Standardize the data (make everything write properly with the same manners).
# 3. Null values or blank values.
# 4. Remove any columns if not necessarily 
# (NOTE: we do not remove information from the raw data, we create a stage data and work on it).

# Stage 1:
-- create staging table to work on (not working on the raw table) 
CREATE TABLE world_layoffs.layoffs_staging
LIKE world_layoffs.layoffs;
INSERT world_layoffs.layoffs_staging
SELECT * 
FROM world_layoffs.layoffs;

-- create a CTE for the duplicates and create new column using ROW_NUMBER OVER all the different columns, named as row_num
WITH duplicates_cte AS (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off,
									percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
) 
SELECT * 
FROM duplicates_cte
WHERE row_num > 1;

-- checking how many rows we have where row_num > 1, and delete those using another table called layoffs_staging2.
CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM world_layoffs.layoffs_staging;
    
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num >= 2;

# Stage 2:
-- update all the companies names with TRIM in order to remove extra spaces
SELECT *
FROM world_layoffs.layoffs_staging2;

SELECT DISTINCT company
FROM world_layoffs.layoffs_staging2;

UPDATE layoffs_staging2 
SET company = TRIM(company);

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- change the date column from text format to Date format
SELECT *
FROM world_layoffs.layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

# Stage 3:
-- check for blank values and see if we can fill them up with information we do get can from the table
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- change all the blank values to NULL
UPDATE layoffs_staging2 
SET industry = NULL
WHERE industry = ''; 

SELECT * 
FROM world_layoffs.layoffs_staging2  t1
JOIN world_layoffs.layoffs_staging2  t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL ;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL ;

SELECT DISTINCT industry
FROM layoffs_staging2;
-- we can see that we still have one NULL value in the industry but we have only one row in the company
-- so the way we used before don't work this time and we will keep it that way.

# Stage 4:
-- Delete the rows we are not going to use, and unnesecary columns
SELECT *
FROM world_layoffs.layoffs_staging2 
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL ;

DELETE 
FROM world_layoffs.layoffs_staging2 
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL ;

-- to delete columns we need to use ALTER 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num ;

