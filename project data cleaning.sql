-- Data Cleaning


SELECT *
FROM layoffs
;

-- remove Duplicates
-- standarize data
-- no values/blank values
-- remove any columns

CREATE TABLE layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging
;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num>1;

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
FROM layoffs_staging2
;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
;


DELETE
FROM layoffs_staging2
WHERE row_num>1
;


SELECT *
FROM layoffs_staging2
WHERE row_num=1
;

-- now step 2 standarize things

SELECT country
FROM layoffs_staging2
order by 1;

SELECT TRIM(TRailing '.' FROM country),country
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;


UPDATE layoffs_staging2
SET company=TRIM(company);

UPDATE layoffs_staging2
SET country=TRIM(TRailing '.' FROM country);

SELECT DISTINCT industry
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
order by 1;

UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%'
;

SELECT  `DATE`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET `date`=STR_TO_DATE(`date`,'%m/%d/%Y')
;


SELECT *
FROM layoffs_staging2;

--  3rd step check balnk or null
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry =''
;

SELECT *
FROM layoffs_staging2
WHERE company='Airbnb'
;

UPDATE layoffs_staging2
SET industry=NULL
where industry='';


SELECT t1.industry,t2.industry
FROM layoffs_staging2 AS t2
JOIN layoffs_staging2 AS t1
	ON t1.company =t2.company
    AND t1.location=t2.location
WHERE (t1.industry IS NULL )
AND t2.industry IS NOT NULL
;

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company =t2.company
    AND t1.location=t2.location
SET t1.industry=t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL
;


DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;