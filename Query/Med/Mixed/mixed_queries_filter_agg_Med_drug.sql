-- Query 1: filter1_agg1 (Med_drug)
SELECT prescription_status, MIN(dosage_frequency) AS min_dosage_frequency FROM Med_drug WHERE side_effects != 'drowsiness' GROUP BY prescription_status;

