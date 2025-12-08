-- Query 1: filter1_agg1 (Med_institution)
SELECT research_fields, MIN(institution_country) AS min_institution_country FROM Med_institution WHERE research_diseases != 'post-traumatic stress disorder' GROUP BY research_fields;

