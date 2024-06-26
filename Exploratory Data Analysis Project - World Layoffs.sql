-- Exploratory Data Analysis
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022/data

# Finding which companies had 100% of their workforce laid off, and determining how many employees they laid off
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;


# Finding which companies raised the most money but still laid off 100% of their workforce
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


# Finding how many employees were laid off in each country, ordered from greatest to least
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY SUM(total_laid_off) DESC;


# Finding how many employees each company laid off, ordered from greatest to least
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY SUM(total_laid_off) DESC;


# Finding how many employees were laid off in each industry, ordered from greatest to least
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;


# Determining date range of the layoffs
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;


# Determining how many employees were laid off in each year
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY YEAR(`date`) DESC;


# Determining how many employees were laid off for each company stage, ordered from greatest to least
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY SUM(total_laid_off) DESC;


# Creating rolling total of layoffs using year and month
WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY `MONTH` ASC
)
SELECT `MONTH`, total_layoffs,
SUM(total_layoffs) OVER (ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


# Finding top 5 companies with the most layoffs in each year
WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), 
Company_Year_Rank AS
(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
WHERE Ranking <= 5;







