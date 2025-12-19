-- Query 1: 1 (Med_team)
SELECT founded_year, ownership, team_name FROM Med_team WHERE ownership != 'William Chisholm';

-- Query 2: 1 (Med_team)
SELECT ownership, championship, founded_year FROM Med_team WHERE founded_year < 1989;

-- Query 3: 2 (Med_team)
SELECT founded_year, ownership, team_name FROM Med_team WHERE ownership != 'Jim Morris' AND founded_year >= 1967;

-- Query 4: 2 (Med_team)
SELECT championship, founded_year, location FROM Med_team WHERE location = 'Brooklyn' AND location = 'Memphis';

-- Query 5: 3 (Med_team)
SELECT founded_year, team_name, location FROM Med_team WHERE founded_year < 1949 OR location != 'Oklahoma City';

-- Query 6: 3 (Med_team)
SELECT ownership, founded_year, championship FROM Med_team WHERE ownership != 'Herb Simon' OR championship = 3;

-- Query 7: 4 (Med_team)
SELECT championship, ownership, founded_year FROM Med_team WHERE ownership = 'Paul Allen' AND championship >= 1 AND ownership = 'Professional Basketball Club LLC' AND location = 'Houston';

-- Query 8: 4 (Med_team)
SELECT team_name, championship, location FROM Med_team WHERE championship > 6 AND championship >= 1 AND founded_year = 1989 AND championship <= 1;

-- Query 9: 5 (Med_team)
SELECT championship, founded_year, location FROM Med_team WHERE founded_year <= 1978 OR team_name = 'Dallas Mavericks' OR championship > 3 OR location = 'Atlanta';

-- Query 10: 5 (Med_team)
SELECT team_name, founded_year, ownership FROM Med_team WHERE founded_year > 1967 OR team_name != 'Charlotte Hornets' OR team_name = 'Detroit Pistons' OR founded_year >= 1989;

-- Query 11: 6 (Med_team)
SELECT location, founded_year, team_name FROM Med_team WHERE (founded_year >= 1949 AND founded_year = 1949) OR (ownership != 'Ted Leonsis' AND ownership != 'Professional Basketball Club LLC');

-- Query 12: 6 (Med_team)
SELECT location, founded_year, team_name FROM Med_team WHERE (founded_year > 1989 AND ownership != 'Gabe Plotkin and Rick Schnall') OR (ownership = 'Atlanta Spirit LLC' AND founded_year = 1967);

