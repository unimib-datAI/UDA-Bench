-- Query 1: 1 (Art)
SELECT composition, birth_date, field FROM Art WHERE field != 'Other';

-- Query 2: 1 (Art)
SELECT death_country, death_date, image_genre FROM Art WHERE zodiac = 'Libra';

-- Query 3: 1 (Art)
SELECT tone, field, nationality FROM Art WHERE nationality != 'Japanese';

-- Query 4: 1 (Art)
SELECT zodiac, art_movement, theme FROM Art WHERE birth_country = 'Bulgaria';

-- Query 5: 1 (Art)
SELECT birth_date, color, teaching FROM Art WHERE birth_date = '1905/4/19';

-- Query 6: 1 (Art)
SELECT art_institution, style, marriage FROM Art WHERE art_movement = 'Pop Art';

-- Query 7: 1 (Art)
SELECT marriage, birth_city, composition FROM Art WHERE birth_city = 'Boston';

-- Query 8: 1 (Art)
SELECT awards, zodiac, theme FROM Art WHERE death_country = 'Italy';

-- Query 9: 1 (Art)
SELECT birth_date, birth_country, color FROM Art WHERE art_institution = 'University of Minnesota';

-- Query 10: 1 (Art)
SELECT genre, style, age FROM Art WHERE name != 'Christiaan Karel Appel';

-- Query 11: 2 (Art)
SELECT marriage, object, nationality FROM Art WHERE genre != 'Symbolic' AND age >= 75;

-- Query 12: 2 (Art)
SELECT birth_continent, composition, zodiac FROM Art WHERE birth_continent = 'North America' AND birth_city = 'Rochester';

-- Query 13: 2 (Art)
SELECT object, birth_continent, birth_country FROM Art WHERE field != 'Ceramics' AND death_city != 'New York City';

-- Query 14: 2 (Art)
SELECT image_genre, birth_date, art_institution FROM Art WHERE zodiac != 'Cancer' AND death_city = 'Saint Petersburg';

-- Query 15: 2 (Art)
SELECT awards, composition, birth_date FROM Art WHERE birth_date != '1905/4/27' AND art_movement != 'Cubism';

-- Query 16: 2 (Art)
SELECT tone, death_country, birth_date FROM Art WHERE birth_date != '1848/1/13' AND death_country = 'Nigeria';

-- Query 17: 2 (Art)
SELECT image_genre, century, birth_continent FROM Art WHERE death_date != '2005/1/14' AND birth_city = 'Boulogne-sur-Mer';

-- Query 18: 2 (Art)
SELECT marriage, composition, zodiac FROM Art WHERE zodiac = 'Cancer' AND birth_city != 'Los Angeles';

-- Query 19: 2 (Art)
SELECT object, genre, art_institution FROM Art WHERE genre != 'Conceptual' AND teaching < 0;

-- Query 20: 2 (Art)
SELECT birth_continent, composition, birth_date FROM Art WHERE birth_date = '1848/1/13' AND marriage != 'Married';

-- Query 21: 3 (Art)
SELECT birth_city, death_date, tone FROM Art WHERE death_date != '1904/4/13' OR birth_city != 'Hastings';

-- Query 22: 3 (Art)
SELECT object, zodiac, genre FROM Art WHERE marriage != 'Cohabiting' OR art_institution != 'Chouinard Art Institute';

-- Query 23: 3 (Art)
SELECT teaching, death_date, color FROM Art WHERE teaching != 0 OR age != 90;

-- Query 24: 3 (Art)
SELECT art_movement, birth_date, style FROM Art WHERE art_movement = 'Generación de la Ruptura' OR death_country != 'Brazil';

-- Query 25: 3 (Art)
SELECT teaching, object, death_city FROM Art WHERE field = 'Drawing' OR death_country != 'France';

-- Query 26: 3 (Art)
SELECT death_country, tone, century FROM Art WHERE century != '20th-21st' OR name != 'Agnes Lawrence Pelton';

-- Query 27: 3 (Art)
SELECT color, genre, zodiac FROM Art WHERE zodiac != 'Cancer' OR death_city = 'Munich';

-- Query 28: 3 (Art)
SELECT death_country, style, teaching FROM Art WHERE art_movement = 'Space' OR name != 'Raoul De Keyser';

-- Query 29: 3 (Art)
SELECT image_genre, death_city, death_country FROM Art WHERE zodiac != 'Cancer' OR teaching != 0;

-- Query 30: 3 (Art)
SELECT composition, death_country, age FROM Art WHERE death_country = 'Ethiopia' OR birth_date = '1905/5/14';

-- Query 31: 4 (Art)
SELECT field, object, genre FROM Art WHERE field = 'Calligraphy' AND art_institution != 'NY' AND death_city != 'New York' AND death_country != 'Russia';

-- Query 32: 4 (Art)
SELECT nationality, color, art_institution FROM Art WHERE awards > 1 AND awards >= 0 AND age = 90 AND awards <= 0;

-- Query 33: 4 (Art)
SELECT zodiac, tone, teaching FROM Art WHERE zodiac = 'Virgo' AND century != '19th-20th' AND teaching >= 0 AND death_country != 'South Africa';

-- Query 34: 4 (Art)
SELECT death_city, tone, death_country FROM Art WHERE death_city = 'Moscow' AND art_institution = 'Hunter College' AND death_country != 'Hungary' AND death_date = '1943/1/13';

-- Query 35: 4 (Art)
SELECT art_movement, style, death_city FROM Art WHERE birth_date != '1905/4/20' AND art_institution = 'Claremont Graduate University' AND zodiac = 'Pisces' AND death_city = 'London';

-- Query 36: 4 (Art)
SELECT image_genre, death_city, birth_city FROM Art WHERE age <= 83 AND death_city != 'Cape Town' AND birth_date = '1924/2/9' AND genre = 'Figurative';

-- Query 37: 4 (Art)
SELECT color, teaching, century FROM Art WHERE awards >= 1 AND awards <= 0 AND birth_city != 'Birmingham' AND death_country = 'Switzerland';

-- Query 38: 4 (Art)
SELECT death_city, style, art_movement FROM Art WHERE name != 'Eduardo Chillida Juantegui' AND age > 90 AND birth_continent != 'Africa' AND birth_country = 'United States';

-- Query 39: 4 (Art)
SELECT birth_city, zodiac, color FROM Art WHERE birth_city = 'Bronxville' AND death_city != 'Tigre' AND birth_country = 'Switzerland' AND teaching < 0;

-- Query 40: 4 (Art)
SELECT marriage, theme, awards FROM Art WHERE birth_country != 'Spain' AND nationality != 'Jewish' AND teaching >= 0 AND teaching <= 0;

-- Query 41: 5 (Art)
SELECT death_city, image_genre, death_country FROM Art WHERE zodiac != 'Cancer' OR birth_date = '1908/12/22' OR art_institution != 'Art Students League' OR awards = 1;

-- Query 42: 5 (Art)
SELECT art_institution, nationality, image_genre FROM Art WHERE century = '19th-20th' OR birth_date != '1905/4/25' OR birth_country = 'Chile' OR birth_continent != 'South America';

-- Query 43: 5 (Art)
SELECT color, birth_city, nationality FROM Art WHERE awards = 0 OR birth_city = 'Budapest' OR zodiac != 'Aries' OR age >= 90;

-- Query 44: 5 (Art)
SELECT marriage, image_genre, death_date FROM Art WHERE century != '19th' OR name != 'Ben Shahn' OR art_institution = 'Pennsylvania Academy of the Fine Arts' OR teaching > 0;

-- Query 45: 5 (Art)
SELECT image_genre, zodiac, birth_city FROM Art WHERE zodiac = 'Leo' OR marriage = 'Cohabiting' OR marriage != 'Separated' OR death_country != 'Cuba';

-- Query 46: 5 (Art)
SELECT theme, death_country, age FROM Art WHERE death_country != 'United Kingdom' OR field != 'Collage' OR nationality != 'Australian' OR awards >= 1;

-- Query 47: 5 (Art)
SELECT object, name, age FROM Art WHERE field != 'Calligraphy' OR birth_city = 'Paris' OR nationality != 'Indian' OR awards <= 1;

-- Query 48: 5 (Art)
SELECT field, teaching, style FROM Art WHERE birth_date != '1905/4/19' OR nationality != 'Irish' OR birth_city != 'Vienna' OR age >= 75;

-- Query 49: 5 (Art)
SELECT theme, birth_date, death_country FROM Art WHERE birth_city = 'Havana' OR marriage != 'Widowed' OR teaching != 0 OR birth_city = 'Paris';

-- Query 50: 5 (Art)
SELECT name, marriage, tone FROM Art WHERE marriage != 'Cohabiting' OR field = 'Painting' OR art_movement = 'Art Nouveau' OR field = 'Ceramics';

-- Query 51: 6 (Art)
SELECT birth_country, image_genre, age FROM Art WHERE (age >= 64 AND age = 64) OR (death_city != 'Montreal' AND field != 'Calligraphy');

-- Query 52: 6 (Art)
SELECT composition, age, name FROM Art WHERE (age > 90 AND genre != 'Conceptual') OR (death_city = 'Paris' AND zodiac != 'Aries');

-- Query 53: 6 (Art)
SELECT teaching, image_genre, death_country FROM Art WHERE (zodiac = 'Aquarius' AND field = 'Ceramics') OR (teaching != 0 AND birth_city != 'San Francisco');

-- Query 54: 6 (Art)
SELECT death_date, nationality, object FROM Art WHERE (marriage = 'Cohabiting' AND birth_continent = 'Asia') OR (awards > 0 AND death_country != 'Venezuela');

-- Query 55: 6 (Art)
SELECT style, genre, zodiac FROM Art WHERE (birth_date = '1905/4/15' AND genre != 'Allegorical') OR (teaching != 0 AND birth_city = 'Havana');

-- Query 56: 6 (Art)
SELECT age, style, century FROM Art WHERE (birth_date != '1905/4/25' AND death_date = '2010/7/15') OR (awards <= 1 AND century != '20th-21st');

-- Query 57: 6 (Art)
SELECT object, death_city, teaching FROM Art WHERE (field = 'Ceramics' AND birth_country != 'British India') OR (name = 'Oscar Agustín Alejandro Schulz Solari' AND nationality != 'Ukrainian');

-- Query 58: 6 (Art)
SELECT composition, birth_country, art_movement FROM Art WHERE (art_movement != 'Abstract' AND birth_continent != 'South America') OR (death_country != 'Jordan' AND birth_date != '1905/4/15');

-- Query 59: 6 (Art)
SELECT field, color, nationality FROM Art WHERE (field = 'Textile art' AND teaching > 0) OR (age > 90 AND marriage = 'Cohabiting');

-- Query 60: 6 (Art)
SELECT century, birth_city, object FROM Art WHERE (field = 'Photography' AND nationality != 'Japanese') OR (death_date = '2010/7/15' AND awards != 0);

