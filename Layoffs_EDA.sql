-- Exploratory Data Aanlysis
-- SQL Project for Insight Analysis
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


-- Getting the cleaned data.
SELECT *
FROM layoffs_staging2;


-- Checking the max lay off from the data.
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;


-- Companies that laid off their entire staff
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1;
-- Companies with a percentage_laid_off = 1 means that they laid off their entire staff.
-- mostly startups and small bsuiness were the ones that had to lay off their entire staff.


-- Industry wise layoffs
SELECT industry, SUM(total_laid_off) AS CTLO
FROM layoffs_staging2
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC;
-- So it looks like Consumer and Retail had the most layoffs, followed by Transportation and Finance.


-- Lay offs with respect to Company
SELECT Company, SUM(total_laid_off) AS STLO
FROM layoffs_staging2
GROUP BY Company
ORDER BY SUM(total_laid_off) DESC;
-- Companies with the most overall layoffs are the big tech ones.
-- Amazon laid off 18,150 people, followed by Google with 12000 and Meta with 11000.


-- Getting the average number of layoffs
SELECT AVG(total_laid_off)
FROM layoffs_staging2;


-- Checking to see total layoffs per year for each companies per Industry.
WITH Industry_laidoffs AS
(
SELECT industry, company, YEAR(`date`) AS Years, Month(`date`) AS Months, SUM(total_laid_off) AS Sum_total_laid
FROM layoffs_staging2
GROUP BY industry, company, YEAR(`date`), Month(`date`)
ORDER BY SUM(total_laid_off) DESC
)
SELECT Years, Months, industry, company, SUM(Sum_total_laid) OVER(PARTITION BY Years, Months, industry, company ORDER BY Sum_total_laid DESC) AS Company_Total_Laid
FROM Industry_laidoffs
WHERE Years IS NOT NULL; 


-- Getting the Date Range when the lay-offs happened.
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;


-- Country wise layoffs
SELECT Country, SUM(total_laid_off) AS SUM_TLO
FROM layoffs_staging2
GROUP BY Country
ORDER BY SUM(total_laid_off) DESC;
-- USA had the most layoffs, followed by India and Netherlands.


-- Year wise total laid offs
SELECT YEAR(`date`), SUM(total_laid_off) AS SUM_TLO
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY SUM(total_laid_off) DESC;
-- Form this data, even if only the first three months of 2023 are here, we know that 2023 had the most layoffs. 


-- Getting Rolling Total of month by month layoffs
SELECT SUBSTRING(`date`, 1, 7) AS `Month`,  SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `Month`,  SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`, total_off, SUM(total_off) OVER(ORDER BY `Month`) AS Rolling_Total
FROM Rolling_Total;


-- Getting the data on how much each company laid off per year
WITH Company_Year (company, Years, Total_laid_off) AS 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL
GROUP BY company, YEAR(`date`)
)
SELECT *,
DENSE_RANK() OVER(PARTITION BY Years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
ORDER BY Ranking ASC;


-- Getting an Year on Year snapshot for layoffs by Companies based on the ranking provided above
WITH Company_Year (company, Years, Total_laid_off) AS 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(
SELECT *,
DENSE_RANK() OVER(PARTITION BY Years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
ORDER BY Years ASC
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <=5;
-- For 2020, Uber had the highest layoffs.
-- For 2021, Bytedance had the highest layoffs.
-- For 2022, Meta had the highest layoffs.
-- For 2023, Google had the highest layoffs.


-- End of Project.











