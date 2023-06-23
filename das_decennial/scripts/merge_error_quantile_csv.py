import glob
import re
import pandas as pd

for i, fname in enumerate(glob.glob('*.csv')):
    miss_name = re.findall(r'[A-Z]+_[A-Z]+', fname)[0]
    df = pd.read_csv(fname).rename(columns={'Error': miss_name})
    merged = df if i == 0 else pd.merge(merged, df, on=['Geolevel','Query', 'ErrorType', 'Quantile'])

merged.to_csv('query_error_quantiles_comparison.csv', index=False)
