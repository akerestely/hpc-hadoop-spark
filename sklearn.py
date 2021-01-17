#%%
def convert(nrows: int = None):
    '''
    Read raw data and generate a parquet file.
    '''
    from os import path
    data_path = r"data\data_" + str(nrows) + ".parquet"
    if not path.exists(data_path):
        from read_set import read_set_pd
        df = read_set_pd(nrows=nrows)
        df.info()
        df.to_parquet(data_path)
    return data_path

def run_sklearn(test_size, hidden_layers, data_pathnrow, nrows: int = None):
    # generate a parquet file with the specified subset
    data_path = convert(nrows)

    # read the parquet file
    import pandas as pd
    df = pd.read_parquet(data_path, engine="pyarrow")
    df.info()
    x = df.iloc[:, :-1]
    y = df.iloc[:, -1]

    # split
    from sklearn.model_selection import train_test_split
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_size, shuffle=True, stratify=y)

    # verify that indeed we have stratification
    #import numpy as np
    #display(np.unique(y_train.values, return_counts = True))

    # setup, run, score MLPClassifier
    from sklearn.neural_network import MLPClassifier
    model = MLPClassifier(verbose=True, max_iter=500, hidden_layer_sizes = hidden_layers)
    model.fit(x_train, y_train)

    y_pred = model.predict(x_test)
    from sklearn.metrics import accuracy_score
    accuracy_score(y_test, y_pred)

    # setup, run, score LinearSVC
    from sklearn import svm
    model = svm.LinearSVC(verbose=True)
    model.fit(x_train, y_train)

    y_pred = model.predict(x_test)
    from sklearn.metrics import accuracy_score
    accuracy_score(y_test, y_pred)

#%%
#run_sklearn(test_size = 0.2, hidden_layers = (50, 100, 50), nrows = 100) # randomly take 100 rows from each matlab file
run_sklearn(test_size = 0.2, hidden_layers = (50, 100, 50)) # run on all data if it fits in memory