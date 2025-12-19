-- Query 1: filter1_join1 (player, team)
SELECT player.draft_pick, team.team_name, player.fiba_world_cup, team.location FROM player JOIN team ON player.team = team.team_name WHERE player.fiba_world_cup < 0;

-- Query 2: filter1_join2 (player, team, city, manager)
SELECT manager.age, team.founded_year, player.name, city.city_name FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE player.birth_date = '1994/2/2';

-- Query 3: filter2_join1 (player, team)
SELECT player.team, team.location, player.college, team.team_name FROM player JOIN team ON player.team = team.team_name WHERE player.nba_championships < 0 AND player.nationality != 'Dutch';

-- Query 4: filter2_join2 (player, team, city, manager)
SELECT manager.age, city.area, team.location, player.team FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE player.college = 'Michigan State University' AND city.state_name != 'Ohio';

-- Query 5: filter3_join1 (player, team)
SELECT player.mvp_awards, team.championship, player.draft_year, team.team_name FROM player JOIN team ON player.team = team.team_name WHERE team.location != 'Los Angeles' OR team.founded_year < 1967;

-- Query 6: filter3_join2 (player, team, city, manager)
SELECT manager.nba_team, team.ownership, player.fiba_world_cup, city.city_name FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE manager.age <= 65 OR player.olympic_gold_medals <= 1;

-- Query 7: filter4_join1 (player, team)
SELECT team.location, player.nba_championships, team.team_name, player.team FROM player JOIN team ON player.team = team.team_name WHERE player.nba_championships < 0 AND player.age >= 42 AND player.nationality = 'Slovenian';

-- Query 8: filter4_join2 (player, team, city, manager)
SELECT player.nationality, manager.nationality, city.population, team.championship FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE player.position != 'Backcourt' AND player.draft_year >= 2005 AND city.city_name != 'Minneapolis';

-- Query 9: filter5_join1 (player, team)
SELECT player.name, player.position, team.championship, team.founded_year FROM player JOIN team ON player.team = team.team_name WHERE player.name = 'Kobe Bean Bryant' OR player.fiba_world_cup >= 0 OR team.championship != 3;

-- Query 10: filter5_join2 (player, team, city, manager)
SELECT player.position, manager.age, team.championship, city.city_name FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE player.olympic_gold_medals = 0 OR player.position != 'Backcourt' OR city.gdp = '252200';

-- Query 11: filter6_join1 (player, team)
SELECT team.location, player.draft_year, team.ownership, player.name FROM player JOIN team ON player.team = team.team_name WHERE (team.founded_year <= 1989 AND player.college = 'University of Connecticut') OR (player.fiba_world_cup >= 1 AND player.team != 'Milwaukee Bucks');

-- Query 12: filter6_join2 (player, team, city, manager)
SELECT team.team_name, city.area, player.position, manager.own_year FROM player JOIN team ON player.team = team.team_name JOIN manager ON team.ownership = manager.name JOIN city ON team.location = city.city_name WHERE (player.mvp_awards <= 1 AND team.championship <= 1) OR (manager.nationality = 'Taiwanese-Canadian' AND player.mvp_awards < 0);

