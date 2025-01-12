# Exploratory Data Analysis

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- companies sum of laid_off
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- range of dates we are looking at
SELECT MIN(`date`), MAX(`date`)
FROM world_layoffs.layoffs_staging2;

-- industry with most laid_off
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry 
ORDER BY 2 DESC;

-- country with most laid_off
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country 
ORDER BY 2 DESC;

-- year with most laid_off
SELECT YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`) 
ORDER BY 1 DESC;

-- stage(seed) with most laid_off
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- looking at the total_laid_off according to year and month
SELECT SUBSTRING(`date`,1,7) AS `year_month`, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `year_month`
ORDER BY 1;

-- rolling total layoffs according to year and month
WITH rolling_total AS 
(
SELECT SUBSTRING(`date`,1,7) AS `year_month`, SUM(total_laid_off) AS total_layoff
FROM world_layoffs.layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `year_month`
ORDER BY 1
)
SELECT `year_month`, total_layoff, SUM(total_layoff) OVER(ORDER BY `year_month`) AS rolling_total_layoff
FROM rolling_total;

-- look at the company and rolling year of total layoff
SELECT company, SUBSTRING(`date`,1,7) AS `year_month`, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company, `year_month`
ORDER BY company;

-- similar idea to company and year month, but ranking dua most layoffs year
WITH company_year AS 
(
SELECT company, YEAR(`date`) AS `year`, SUM(total_laid_off) AS total_off
FROM world_layoffs.layoffs_staging2
WHERE YEAR(`date`)IS NOT NULL
GROUP BY company, `year`
ORDER BY company
), rank_copmany AS 
(
SELECT *, DENSE_RANK() OVER(PARTITION BY `year` ORDER BY total_off DESC) AS ranking
FROM company_year
ORDER BY ranking
)
SELECT * 
FROM rank_copmany
WHERE ranking <= 5
ORDER BY `year`; 

-- look at the stage where percentage_laid_off is 1 (company shut down)
WITH stage_shut_down AS
(
SELECT company, industry, total_laid_off, percentage_laid_off, stage
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY stage
)
SELECT stage, COUNT(stage) AS num_comapnies
FROM stage_shut_down
GROUP BY stage
ORDER BY num_comapnies DESC;

