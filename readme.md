# Running Machine Learning algorithms on Spark

This repository contains the source code for the _A Research Study on Running Machine Learning Algorithms on Big Data with Spark_ research paper written by _Arpad Kerestely_, _Alexandra Baicoianu_ and _Razvan Bocu_ in 2020.

## Preparing the Spark environment

1. Create the **redistributable** child directory which should contain **jre** and **python** folders.
1. Copy the a Java 1.8 installation's jre folder to the above create *redistributable/jre* folder
1. run `conda env create -p .\redistributable\python -f environment_worker.yml` from the main directory, from an anaconda environment
1. create and activate the *spark* environment from `environment_driver.yml`
1. run:
    1. `install.py` to download spark and winutils; unpack and merge the two; update required environment variables; create `run_worker.bat` file; open a cmd with the required environment variables
    1. `install.py master` do all of the above and also lunch a master
    1. `install.py worker -a hostname:port` to run a worker that connects to a specified host at a specified port (note: a master and a worker can run on the same machine if run from different cmd instances or if the worker is run directly from the generated *run_worker.bat* after specifying the required address)

To run the workers on different machines, one can make a small installer running the included bundler with `install.py -b`. This installer can be deployed to other machines, and installed without administrator rights (administrator rights are only required to allow spark to communicate thorough the firewall if not done so beforehand). In order to run `install.py -b` one should create and activate the conda environment from `environment_bundler.yml`. The machines where this installer is run, should be connected to the internet, as the installer will download spark.

## Running tests

Although the data used for the test cannot be published on public domain, the source code for running scikit-learn is available in `sklearn.py` or `sklearn.ipynb` and for running Spark, the code is available in `spark.py` or `spark.ipynb`. Scikit-learn code can be run using the *spark* conda environment.