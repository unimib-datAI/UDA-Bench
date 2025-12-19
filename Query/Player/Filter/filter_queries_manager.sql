-- Query 1: 1 (Med_manager)
SELECT age, nba_team, name FROM Med_manager WHERE nba_team != 'Cleveland Cavaliers';

-- Query 2: 2 (Med_manager)
SELECT age, nba_team, name FROM Med_manager WHERE nba_team != 'Golden State Warriors' AND age >= 67;

-- Query 3: 3 (Med_manager)
SELECT age, name, nationality FROM Med_manager WHERE age < 65 OR nationality != 'Israeli-American';

-- Query 4: 4 (Med_manager)
SELECT own_year, nba_team, age FROM Med_manager WHERE nba_team = 'Miami Heat' AND own_year >= 2008 AND nba_team = 'Philadelphia 76ers' AND nationality = 'Taiwanese-Canadian';

-- Query 5: 5 (Med_manager)
SELECT own_year, age, nationality FROM Med_manager WHERE age <= 76 OR name = 'Joseph Chung-Hsin Tsai' OR own_year > 2013 OR nationality = 'American';

-- Query 6: 6 (Med_manager)
SELECT nationality, age, name FROM Med_manager WHERE (age >= 65 AND age = 65) OR (nba_team != 'Indiana Pacers' AND nba_team != 'New Orleans Pelicans');

