-- Query 1: filter1_agg1_join1 (player, team)
SELECT player.team, MIN(player.olympic_gold_medals) AS min_player_olympic_gold_medals FROM player JOIN team ON player.team = team.team_name WHERE player.fiba_world_cup > 0 GROUP BY player.team;

-- Query 2: filter1_agg1_join2 (player, team, city, manager)
SELECT player.position, MAX(team.championship) AS max_team_championship FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE city.state_name = 'Utah' GROUP BY player.position;

-- Query 3: filter2_agg1_join1 (player, team)
SELECT player.team, AVG(player.mvp_awards) AS avg_player_mvp_awards FROM player JOIN team ON player.team = team.team_name WHERE player.college = 'University of Kentucky' AND player.age <= 66 GROUP BY player.team;

-- Query 4: filter2_agg1_join2 (player, team, city, manager)
SELECT player.team, MAX(player.olympic_gold_medals) AS max_player_olympic_gold_medals FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE city.state_name = 'Arizona' AND city.area >= 1314.80 GROUP BY player.team;

-- Query 5: filter3_agg1_join1 (player, team)
SELECT player.team, AVG(player.fiba_world_cup) AS avg_player_fiba_world_cup FROM player JOIN team ON player.team = team.team_name WHERE player.nba_championships > 0 OR player.age >= 47 GROUP BY player.team;

-- Query 6: filter3_agg1_join2 (player, team, city, manager)
SELECT player.position, AVG(player.age) AS avg_player_age FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE player.nba_championships <= 2 OR player.mvp_awards < 1 GROUP BY player.position;

-- Query 7: filter4_agg1_join1 (player, team)
SELECT player.team, AVG(player.fiba_world_cup) AS avg_player_fiba_world_cup FROM player JOIN team ON player.team = team.team_name WHERE player.olympic_gold_medals = 0 AND player.position != 'Backcourt' AND team.founded_year != 1989 GROUP BY player.team;

-- Query 8: filter4_agg1_join2 (player, team, city, manager)
SELECT player.position, AVG(city.area) AS avg_city_area FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE manager.age = 76 AND team.founded_year > 1989 AND team.team_name = 'Philadelphia 76ers' GROUP BY player.position;

-- Query 9: filter5_agg1_join1 (player, team)
SELECT player.nationality, COUNT(player.nationality) AS count_player_nationality FROM player JOIN team ON player.team = team.team_name WHERE player.nationality = 'Cameroonian-American' OR player.birth_date = '1972/3/6' OR player.age >= 47 GROUP BY player.nationality;

-- Query 10: filter5_agg1_join2 (player, team, city, manager)
SELECT player.team, SUM(city.area) AS sum_city_area FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE player.draft_year <= 1990 OR player.position = 'Backcourt' OR player.draft_pick <= 35 GROUP BY player.team;

-- Query 11: filter6_agg1_join1 (player, team)
SELECT player.position, SUM(player.mvp_awards) AS sum_player_mvp_awards FROM player JOIN team ON player.team = team.team_name WHERE (player.team != 'Milwaukee Bucks' AND player.nba_championships > 2) OR (team.founded_year != 1949 AND team.championship != 1) GROUP BY player.position;

-- Query 12: filter6_agg1_join2 (player, team, city, manager)
SELECT manager.nationality, MIN(player.olympic_gold_medals) AS min_player_olympic_gold_medals FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE (team.location = 'Brooklyn' AND player.nba_championships > 0) OR (player.team != 'Milwaukee Hawks  ' AND player.draft_pick != 35) GROUP BY manager.nationality;

