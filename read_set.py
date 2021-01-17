from pathlib import Path
from typing import List

rootpath = Path(r"data\")
states = {
    'HH':'Healthy_',
    'HI':'Inner_',
    'HO':'Outer_',
    'HB':'Ball_'
}
loadpositions = ['LM', 'LR']
motorspeeds = ['S' + str(speed) for speed in range(300, 2820, 120)]
testruns = ['1', '2']
default_features = [
    'BL_X',
    'BL_Y',
    'BL_Z',
    'BR_X',
    'BR_Y',
    'BR_Z',
    'MR_X',
    'MR_Y',
    'MR_Z',
    'BL_AE',
    'BR_AE',
    'BR_Mic',
    'BL_Mic',
    'speed',
    'defect_type'
]

def get_defect_type(state: str):
    '''
    Convert state (str) to defect type (int)
    '''
    switcher = {
        "HH" : 0,
        "HI" : 1,
        "HO" : 2,
        "HB" : 3,
    }
    return switcher.get(state, "Invalid")

def generate_file_name(state: str, loadposition: str, motorspeed: str, testrun: str) -> str:
    return state + '_' + loadposition + '_' + motorspeed + '_' + testrun + '_TD'

def generate_file_path(state: str, loadposition: str, motorspeed: str, testrun: str, rootpath: Path) -> Path:
    '''
    Generate matlab file path, given the parameters.
    '''
    filename: str = generate_file_name(state, loadposition, motorspeed, testrun) + '.mat'
    foldername: str = states[state] + loadposition
    return rootpath / foldername  / filename

def read_file(state: str, loadposition: str, motorspeed: str, testrun: str, rootpath: Path, nrows: int = None):
    '''
    Read matlab file for the given parameters, 
    and return pandas data frame, with rows containing NaNs droped.

    param nrows: if none, will return all rows
    '''
    from scipy.io import loadmat
    import numpy as np

    filepath = generate_file_path(state, loadposition, motorspeed, testrun, rootpath)
    data = loadmat(filepath, struct_as_record=False)['data'][0][0]
    data_for_df = {}
    column_len = None
    indices = None
    for feature in data._fieldnames:
        if feature not in default_features:
            continue
        
        column = getattr(data, feature)
        new_column_len = len(column)
        if column_len is not None and column_len != new_column_len:
            continue 
            
        column_len = new_column_len
        # from column matrix to vector
        data_for_df[feature] = column.reshape(column_len)

        # generate indices from which to take data
        if indices is None:
            if nrows is None:
                # take all rows
                indices = np.arange(column_len)
            else:
                # randomly pick some rows
                indices = np.random.choice(column_len, nrows, replace=False)

        data_for_df[feature] = data_for_df[feature][indices]
    
    final_column_len = nrows if nrows is not None else column_len
    if 'speed' not in data_for_df:
        # generate speed from filename
        data_for_df['speed'] = np.full(final_column_len, float(motorspeed[1:]))

    data_for_df['defect_type'] = np.full(final_column_len, get_defect_type(state))
    
    from pandas import DataFrame
    return DataFrame(data = data_for_df)

def iterate_set(states: List[str], loadpositions: List[str], motorspeeds: List[str], testruns: List[str], rootpath: Path, nrows: int):
    for state in states:
        for loadposition in loadpositions:
            for motorspeed in motorspeeds:
                for testrun in testruns:
                    yield read_file(state, loadposition, motorspeed, testrun, rootpath, nrows)

## mat to pd
def read_set_pd(states: List[str] = list(states.keys()), loadpositions: List[str] = loadpositions, motorspeeds: List[str] = motorspeeds, testruns: List[str] = testruns, rootpath: Path = rootpath, nrows: int = None):
    """
    Read matlab files for the given parameters and return a pandas DataFrame, containing all data.
    """
    from pandas import DataFrame
    dfs = [df for df in iterate_set(states, loadpositions, motorspeeds, testruns, rootpath, nrows)]
    return DataFrame().append(dfs, ignore_index=True, sort=False)

def read_set_sp(spark, states: List[str] = list(states.keys()), loadpositions: List[str] = loadpositions, motorspeeds: List[str] = motorspeeds, testruns: List[str] = testruns, rootpath: Path = rootpath, nrows: int = None):
    """
    Read matlab files for the given parameters and return a spark DataFrame, containing all data.
    Spark session configuration will be changed to use pyarrow if possible.

    param spark: SparkSession
    """
    from pyspark.sql import SparkSession

    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    sp_df = None
    for df in iterate_set(states, loadpositions, motorspeeds, testruns, rootpath, nrows):
        if sp_df is None:
            sp_df = spark.createDataFrame(df)
        else:
            sp_df_temp = spark.createDataFrame(df)
            sp_df = sp_df.union(sp_df_temp)

    return sp_df

def convert_mat_to_parquet(output_folder: Path, rootpath: Path):
    """
    Read all needed matlab files and convert them to parquet files.

    param output_folder: output folder that will contain all parquet files
    param rootpath: input that containts matlab folders and files
    """
    output_folder.mkdir(exist_ok=True)
    from pandas import DataFrame
    for state in states:
        for loadposition in loadpositions:
            for motorspeed in motorspeeds:
                for testrun in testruns:
                    df = read_file(state, loadposition, motorspeed, testrun, rootpath, None)
                    filename: str = generate_file_name(state, loadposition, motorspeed, testrun) + '.parquet'
                    df.to_parquet(output_folder / filename)

def print_dataset_info(rootpath: Path):
    import dask.dataframe as dd
    ddf = dd.read_parquet(rootpath, engine="pyarrow")
    display(ddf)
    display(ddf.info(memory_usage=True))