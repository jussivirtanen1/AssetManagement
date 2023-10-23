-- Percentage of each asset class/industry/region/etc.
SELECT industry, sum(amount) / (SELECT SUM(amount) FROM asset_management_prod.transactions) as prc
FROM asset_management_prod.transactions
GROUP BY industry
;

-- This query checks that "prc" column sums to 1.
SELECT SUM(prc) FROM (SELECT industry, sum(amount) / (SELECT SUM(amount) FROM asset_management_prod.transactions) as prc
FROM asset_management_prod.transactions
GROUP BY industry) t1
;

-- Amount invested by industry
SELECT industry, sum(amount) as summa
FROM asset_management_p.transactions
GROUP BY industry
;
