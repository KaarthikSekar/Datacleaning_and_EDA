select * from layoffs;


create table stagging_layoffs like layoffs;

insert into stagging_layoffs 

select * from stagging_layoffs;

-- data cleaning 
-- remove duplicates, standardize data, null values or blank values, remove any columns  

select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from stagging_layoffs;

with duplicates_cte as (
	select *,
	row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
	from stagging_layoffs
)

select * from duplicates_cte where row_num > 1;

select * from stagging_layoffs where company = 'Oda';

CREATE TABLE `stagging_layoffs2` (
  `company` text,
  `location` text,
  `total_laid_off` text,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * 
from stagging_layoffs2
where row_num >1;

insert into stagging_layoffs2
select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from stagging_layoffs;

delete  
from stagging_layoffs2
where row_num >1;


select company, trim(company)
from stagging_layoffs2;

update stagging_layoffs2
set company = trim(company);

select distinct(industry) from stagging_layoffs2;

select distinct(location) from stagging_layoffs2 order by 1;
select distinct(country) from stagging_layoffs2 order by 1;

select distinct(country), trim(trailing '.' from country) from stagging_layoffs2 order by 1; 

select `date`, 
str_to_date(`date`,'%m/%d/%Y')
from stagging_layoffs2;

update stagging_layoffs2
set `date` = str_to_date(`date`,'%m/%d/%Y');

select * from stagging_layoffs2;
SELECT DISTINCT `date` FROM stagging_layoffs2 LIMIT 10;


select total_laid_off, trim(total_laid_off)
from stagging_layoffs2;

update stagging_layoffs2
set total_laid_off = trim(total_laid_off);

select * from stagging_layoffs2;

alter table stagging_layoffs2
modify column `date` date;

select * from stagging_layoffs2 
where industry = '';

select * from stagging_layoffs2 
where total_laid_off = '';

select * from stagging_layoffs2;

alter table stagging_layoffs2
drop column row_num;

