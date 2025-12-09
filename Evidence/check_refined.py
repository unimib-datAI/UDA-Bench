#!/usr/bin/env python3
import pandas as pd

df = pd.read_csv('Evidence/Player/manager_refined.csv')
print('前5行的name和name_evidence_refined:')
print('='*100)
for i in range(min(5, len(df))):
    name = df.iloc[i]['name']
    refined = df.iloc[i]['name_evidence_refined']
    print(f'\nRow {i}: {name}')
    print(f'Refined evidence ({len(str(refined))} chars): {refined[:150]}...' if len(str(refined)) > 150 else f'Refined evidence ({len(str(refined))} chars): {refined}')

