# Import and assign aliases to libraries and modules
import dask.dataframe as dd
import pandas as pd

# Read metadata rows
metadata = pd.read_table(
    '../data/10x_reads.tsv', 
    sep='\t', 
    header=None, 
    nrows=8
)

# Compute column names based on metadata
col_names = ['gene'] + [f'10x_{i}' for i in range(1, metadata.shape[1])]

# Transform metadata
metadata = metadata.T
metadata.columns = metadata.iloc[0]
metadata = metadata[1:]
metadata.drop(columns=['clst', 'TSNE_x', 'TSNE_y'], inplace=True)

# Convert metadata to a Dask DataFrame
metadata = dd.from_pandas(metadata, npartitions=1)

# Read and transform data and merge to metadata before export as parquet
metadata.merge(
    dd.read_csv(
        'Data/10x_reads.tsv',
        sep='\t',
        header=None,
        skiprows=8,
        names=col_names,
        sample=1000000
    ).melt(
        id_vars='gene',
        var_name='Cell_ID',
        value_name='read'
    ),
    on='Cell_ID',
    how='inner'
).to_parquet('../data/parquet_output/')