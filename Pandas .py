import pandas as pd

dataset1 = pd.read_csv('dataset1.csv')
dataset2 = pd.read_csv('dataset2.csv')

merged_data = pd.merge(dataset1, dataset2[['counter_party', 'tier']], on='counter_party', how='left')

max_rating = merged_data.groupby('counter_party')['rating'].max().reset_index()
max_rating = max_rating.rename(columns={'rating': 'max_rating'})

sum_arap = merged_data[merged_data['status'] == 'ARAP'].groupby('counter_party')['value'].sum().reset_index()
sum_arap = sum_arap.rename(columns={'value': 'sum_value_arap'})

sum_accr = merged_data[merged_data['status'] == 'ACCR'].groupby('counter_party')['value'].sum().reset_index()
sum_accr = sum_accr.rename(columns={'value': 'sum_value_accr'})

merged_data = pd.merge(merged_data, max_rating, on='counter_party', how='left')
merged_data = pd.merge(merged_data, sum_arap, on='counter_party', how='left')
merged_data = pd.merge(merged_data, sum_accr, on='counter_party', how='left')

result = merged_data[['legal_entity', 'counter_party', 'tier', 'max_rating', 'sum_value_arap', 'sum_value_accr']]

result = result.drop_duplicates(subset='counter_party')

totals = result.groupby(['legal_entity', 'counter_party', 'tier']).agg({
    'max_rating': 'max',
    'sum_value_arap': 'sum',
    'sum_value_accr': 'sum'
}).reset_index()

total_record = pd.DataFrame({
    'legal_entity': 'Total',
    'counter_party': 'Total',
    'tier': 'Total',
    'max_rating': totals['max_rating'].sum(),
    'sum_value_arap': totals['sum_value_arap'].sum(),
    'sum_value_accr': totals['sum_value_accr'].sum()
}, index=[0])

result = pd.concat([result, total_record], ignore_index=True)
print(result)
result.to_csv('output.csv', index=False)