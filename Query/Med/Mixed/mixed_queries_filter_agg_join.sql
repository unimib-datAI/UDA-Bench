-- Query 1: filter1_agg1_join1 (disease, drug)
SELECT Med_drug.prescription_status, MIN(Med_drug.recommended_usage) AS min_Med_drug_recommended_usage FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_drug.manufacturer != 'GSK' GROUP BY Med_drug.prescription_status;

-- Query 2: filter1_agg1_join2 (disease, drug, institution)
SELECT Med_institution.research_fields, MAX(Med_disease.quality_of_life_impact) AS max_Med_disease_quality_of_life_impact FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_institution.leadership = 'Pierre-Yves Marcy' GROUP BY Med_institution.research_fields;

-- Query 3: filter2_agg1_join1 (disease, drug)
SELECT Med_drug.prescription_status, AVG(Med_disease.quality_of_life_impact) AS avg_Med_disease_quality_of_life_impact FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_disease.treatment_challenges != 'only_one_drug_available' AND Med_disease.drugs != 'diuretics' GROUP BY Med_drug.prescription_status;

-- Query 4: filter2_agg1_join2 (disease, drug, institution)
SELECT Med_institution.research_fields, MAX(Med_disease.diagnosis_challenges) AS max_Med_disease_diagnosis_challenges FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_institution.institution_country = 'United Kingdom' AND Med_institution.research_fields != 'gastroenterology' GROUP BY Med_institution.research_fields;

-- Query 5: filter3_agg1_join1 (disease, drug)
SELECT Med_drug.prescription_status, AVG(Med_disease.quality_of_life_impact) AS avg_Med_disease_quality_of_life_impact FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_drug.brand_name = 'Nucala' OR Med_disease.diagnostic_methods = 'genetic_testing' GROUP BY Med_drug.prescription_status;

-- Query 6: filter3_agg1_join2 (disease, drug, institution)
SELECT Med_institution.institution_type, AVG(Med_disease.epidemiology) AS avg_Med_disease_epidemiology FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_drug.brand_name != 'Daliresp' OR Med_disease.common_symptoms != 'back pain' GROUP BY Med_institution.institution_type;

-- Query 7: filter4_agg1_join1 (disease, drug)
SELECT Med_drug.prescription_status, AVG(Med_disease.epidemiology) AS avg_Med_disease_epidemiology FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_drug.pharmaceutical_form = 'tablet' AND Med_drug.indication != 'Runny nose' AND Med_disease.preventive_measures = 'meditation' GROUP BY Med_drug.prescription_status;

-- Query 8: filter4_agg1_join2 (disease, drug, institution)
SELECT Med_institution.institution_type, AVG(Med_drug.dosage_frequency) AS avg_Med_drug_dosage_frequency FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_drug.generic_name != 'Linaclotide' AND Med_disease.common_symptoms != 'headache' AND Med_institution.key_achievements = 'Less than 1% perioperative infection and readmission rates following spinal surgery' GROUP BY Med_institution.institution_type;

-- Query 9: filter5_agg1_join1 (disease, drug)
SELECT Med_drug.prescription_status, COUNT(Med_disease.common_symptoms) AS count_Med_disease_common_symptoms FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_disease.diagnostic_methods = 'genetic_testing' OR Med_disease.pathogenesis = 'traumatic' OR Med_drug.mechanism_of_action = 'PDE4 inhibitor that blocks the action of PDE4, reducing inflammation and relaxing smooth muscle in the airways' GROUP BY Med_drug.prescription_status;

-- Query 10: filter5_agg1_join2 (disease, drug, institution)
SELECT Med_drug.prescription_status, SUM(Med_institution.institution_country) AS sum_Med_institution_institution_country FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_disease.diagnosis_challenges = 'lack of early recognition' OR Med_disease.disease_name = 'Acute Intermittent Porphyria' OR Med_disease.epidemiology = '27% of constipated patients relate constipation to medications' GROUP BY Med_drug.prescription_status;

-- Query 11: filter6_agg1_join1 (disease, drug)
SELECT Med_drug.prescription_status, SUM(Med_disease.treatment_challenges) AS sum_Med_disease_treatment_challenges FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE (Med_disease.treatments != 'radiotherapy' AND Med_drug.brand_name != 'Lo Loestrin FE') OR (Med_drug.brand_name = 'Nucala' AND Med_disease.risk_factors != 'diabetes') GROUP BY Med_drug.prescription_status;

-- Query 12: filter6_agg1_join2 (disease, drug, institution)
SELECT Med_institution.institution_type, MIN(Med_disease.diagnosis_challenges) AS min_Med_disease_diagnosis_challenges FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE (Med_drug.side_effects = 'diarrhea' AND Med_drug.generic_name = 'Esomeprazole') OR (Med_institution.research_diseases = 'Gut Microbiome Imbalance' AND Med_disease.pathogenesis = 'infectious_fungal') GROUP BY Med_institution.institution_type;

