-- Query 1: 1 (Med_city)
SELECT state_name, area, city_name FROM Med_city WHERE area = 375.78;

-- Query 2: 2 (Med_city)
SELECT state_name, area, city_name FROM Med_city WHERE area <= 1314.80 AND population > '887642';

-- Query 3: 3 (Med_city)
SELECT state_name, city_name, population FROM Med_city WHERE state_name != 'Indiana' OR population >= '372,624';

-- Query 4: 4 (Med_city)
SELECT gdp, area, state_name FROM Med_city WHERE area >= 976.15 AND state_name != 'Oregon' AND area = 375.78 AND population = '808988';

-- Query 5: 5 (Med_city)
SELECT gdp, state_name, population FROM Med_city WHERE state_name != 'Ontario' OR city_name = 'Minneapolis' OR gdp != '689000' OR gdp != '344.9';

-- Query 6: 6 (Med_city)
SELECT population, state_name, city_name FROM Med_city WHERE (state_name = 'Minnesota' AND population = '887642') OR (area >= 976.15 AND area >= 976.15);

