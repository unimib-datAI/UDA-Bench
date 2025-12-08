-- Query 1: agg1_join1 (disease, drug)
SELECT Med_drug.prescription_status, MIN(Med_drug.recommended_usage) AS min_Med_drug_recommended_usage FROM disease JOIN drug ON disease.disease_name = drug.disease_name GROUP BY Med_drug.prescription_status;

-- Query 2: agg1_join2 (disease, drug, institution)
SELECT Med_institution.institution_type, MIN(Med_drug.recommended_usage) AS min_Med_drug_recommended_usage FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name GROUP BY Med_institution.institution_type;

