# Running Dask on AzureML

This repository shows how to run a [Dask](https://docs.dask.org/en/latest/) cluster on an [AzureML](https://docs.microsoft.com/en-us/azure/machine-learning/service/) Compute cluster. It is designed to run on an AzureML Notebook VM (created after 8/15/2019), but it should work on your local computer, too. 

Please follow these setup instructions and then start:
 
- here for plain DASK interactive scenarios [interactive/StartDask.ipynb](interactive/StartDask.ipynb).
- here for DASK with NVIDIA RAPIDS interactive scenarios [rapids_interactive/start_cluster.ipynb](rapids_interactive/start_cluster.ipynb).

## Setting up the Python Environment
The environment you are running should have the latest version of `dask` and `distributed` installed -- run this code in the terminal to make sure:

```shell
    conda activate py36  # assuming AzureML Notebook VM
    pip install --upgrade dask distributed
```

Or, if you want to be on the safe side, create a new conda environment using this [environment.yml](interactive/dask/environment.yml) file like so:

```shell
    conda env create -f dask/environment.yml  
    conda activate dask
    python -m ipykernel install --user --name dask --display-name "Python (dask)"
```

![](img/dask-status.gif)

