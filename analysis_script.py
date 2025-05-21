import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns

# Read parquet directory with processed reads
reads_10x = dd.read_parquet('Data/parquet_output')
dtypes_dict = {
    'Cell_ID': 'string',
    '10x_barcode': 'string',
    'patient': 'int64',
    'time': 'int64',
    'sample_ID' : 'string',
    'gene': 'string',
    'read': 'float64'
 }
reads_10x = reads_10x.astype(dtypes_dict)

# Define information function
def get_data_information(reads_df):
    cols = [
        'gene',
        'Cell_ID',
        'patient',
        'time',
        'sample_ID'
    ]
    info = [
        reads_df[col].nunique().compute() for col in cols
    ]
    return info

def check_unique (reads_df):
    reads_df =reads_df.groupby('Cell_ID')['patient'].nunique().compute().reset_index()
    reads_df.columns = ['Cell_ID', 'patient']
    if any(reads_df['patient'] > 1):
        return False
    else:
        return True
    
def plot_cell_contribution(reads_df):
    cell_counts_per_patient = reads_df.groupby('patient')['Cell_ID'].nunique().compute()
    cell_counts_df = cell_counts_per_patient.reset_index()
    cell_counts_df.columns = ['patient', 'unique_cell_count']

    plt.figure()
    fig = sns.barplot(
        x='patient',
        y='unique_cell_count',
        data=cell_counts_df
    )
    fig.bar_label(fig.containers[0])

    plt.xlabel('Patient ID')
    plt.ylabel('Number of Cells')
    plt.title('Patient Cell Contribution')
    plt.show()

def plot_average_read_dist(reads_df):
    mean_read_cells = reads_df.groupby('Cell_ID')['read'].mean().compute()
    mean_read_df = mean_read_cells.reset_index()
    mean_read_df.columns = ['cell_id', 'average_read']

    plt.figure()

    sns.displot(
        x='average_read',
        data=mean_read_df,
        kind='hist'
    )

    plt.xlabel('Average Read')
    plt.title('Distribution of Average Read from Cells')

    plt.show()

