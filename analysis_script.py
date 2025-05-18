import dask.dataframe as dd

# Read parquet directory with processed reads
reads_10x = dd.read_parquet('Data/parquet_output')
# Verify that structure has changed
print(f'Reads DataFrame has {reads_10x.shape[1]} columns and {reads_10x.shape[0].compute()} rows.')

# Summary statistics on read data
gene_total = reads_10x['gene'].nunique().compute()
average_read = reads_10x['read'].mean().compute()
std_read = reads_10x['read'].std().compute()
max_read = reads_10x['read'].max().compute()
min_read = reads_10x['read'].min().compute()