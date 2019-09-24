# Running Dask on AzureML

This repository shows how to run a Dask cluster on an AzureML Compute cluster. It is designed to run on an AzureML Notebook VM, but it should work on your local computer, too. The changes to nginx, however, are only required on the notebook VM.

Please follow these setup instructions and then start here [StartDask.ipynb](StartDask.ipynb).

## Setting up the Python Environment
The environment you are running should have the latest version of `dask` and `distributed` installed -- run this code in the terminal to make sure:

    conda activate py36
    pip install --upgrade dask distributed


Or, if you want to be on the safe side, create a new conda environment using this [environment.yml](dask/environment.yml) file like so:

    conda env create -f dask/environment.yml  
    conda activate dask
    python -m ipykernel install --user --name dask --display-name "Python (dask)"

![](img/dask-status.gif)