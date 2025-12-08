-- Query 1: filter1_join1 (disease, drug)
SELECT Med_disease.epidemiology, Med_drug.brand_name, Med_disease.risk_factors, Med_drug.recommended_usage FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_drug.manufacturer != 'GSK';

-- Query 2: filter1_join2 (disease, drug, institution)
SELECT Med_institution.key_achievements, Med_drug.manufacturer, Med_institution.funding_sources, Med_disease.disease_type FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_disease.pathogenesis = 'congenital';

-- Query 3: filter2_join1 (disease, drug)
SELECT Med_disease.drugs, Med_drug.disease_name, Med_disease.treatments, Med_drug.unsuitable_population FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_drug.brand_name = 'REQUIP XL' AND Med_drug.activation_conditions != 'before meals';

-- Query 4: filter2_join2 (disease, drug, institution)
SELECT Med_drug.side_effects, Med_disease.treatments, Med_institution.key_technologies, Med_drug.mechanism_of_action FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_disease.treatment_challenges != 'only_one_drug_available' AND Med_disease.drugs != 'diuretics';

-- Query 5: filter3_join1 (disease, drug)
SELECT Med_disease.disease_type, Med_disease.diagnosis_challenges, Med_drug.prescription_status, Med_drug.unsuitable_population FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_drug.activation_conditions != 'no special condition' OR Med_drug.dosage_frequency = 'once or twice a day';

-- Query 6: filter3_join2 (disease, drug, institution)
SELECT Med_institution.institution_country, Med_drug.manufacturer, Med_drug.indication, Med_disease.risk_factors FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_drug.single_dose != 'adults 10 mg' OR Med_drug.active_ingredients = 'Roflumilast';

-- Query 7: filter4_join1 (disease, drug)
SELECT Med_disease.drugs, Med_drug.disease_name, Med_drug.indication, Med_disease.prognosis FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_drug.brand_name = 'Nucala' AND Med_disease.diagnostic_methods = 'genetic_testing' AND Med_drug.activation_conditions = 'requires co-administration with other drugs';

-- Query 8: filter4_join2 (disease, drug, institution)
SELECT Med_disease.common_symptoms, Med_drug.pharmaceutical_form, Med_institution.funding_sources, Med_drug.dosage_frequency FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_disease.prognosis != 'partial_recovery' AND Med_drug.active_ingredients != 'bendamustine' AND Med_drug.manufacturer = 'Astellas';

-- Query 9: filter5_join1 (disease, drug)
SELECT Med_drug.active_ingredients, Med_disease.disease_name, Med_disease.sequelae, Med_drug.side_effects FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_disease.disease_name = 'Metastatic Colorectal Cancer' OR Med_drug.administration_route != 'injection' OR Med_drug.recommended_usage = 'after meals';

-- Query 10: filter5_join2 (disease, drug, institution)
SELECT Med_disease.sequelae, Med_drug.brand_name, Med_drug.pharmaceutical_form, Med_institution.parent_organization FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE Med_drug.pharmaceutical_form = 'tablet' OR Med_drug.indication != 'Runny nose' OR Med_disease.preventive_measures = 'meditation';

-- Query 11: filter6_join1 (disease, drug)
SELECT Med_drug.recommended_usage, Med_disease.preventive_measures, Med_drug.unsuitable_population, Med_disease.disease_type FROM disease JOIN drug ON disease.disease_name = drug.disease_name WHERE (Med_drug.dosage_frequency != '2 or 3 times daily' AND Med_disease.sequelae != 'disability') OR (Med_disease.epidemiology != 'especially amongst young men and women' AND Med_drug.active_ingredients = 'Clomiphene');

-- Query 12: filter6_join2 (disease, drug, institution)
SELECT Med_drug.indication, Med_institution.international_collaboration, Med_disease.prognosis, Med_drug.recommended_usage FROM disease JOIN institution ON disease.disease_name = institution.research_diseases JOIN drug ON disease.disease_name = drug.disease_name WHERE (Med_drug.indication != 'constipation' AND Med_disease.diagnosis_challenges != 'lack of accurate diagnostic test') OR (Med_drug.single_dose = 'adult patients with EGPA 3x100 mg' AND Med_disease.sequelae != 'gangrene');

