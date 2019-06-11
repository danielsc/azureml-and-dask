# Running Dask on AzureML

This repository shows how to run a [Dask](https://docs.dask.org/en/latest/) cluster on an [AzureML](https://docs.microsoft.com/en-us/azure/machine-learning/service/) Compute cluster. It is designed to run on an AzureML Notebook VM, but it should work on your local computer, too. The changes to nginx, however, are only required on the notebook VM.

Please follow these setup instructions and then start here [StartDask.ipynb](StartDask.ipynb).

## Patching nginx configuration on the Notebook VM
To make sure you can monitor your dask cluster, you need to make a change to your notebook VM (this will no longer be required once [bug 443670](https://msdata.visualstudio.com/Vienna/_workitems/edit/443670) is fixed).

Open a terminal window on the notebook VM and do the following:

1. open `/etc/nginx/nginx.conf` in an editor
2. find this line  
 `location ~ (/api/kernels/|/terminals/websocket/) {`  
 and add `|ws`, so it looks like this  
 `location ~ (/api/kernels/|/terminals/websocket/|/ws) {`  
3. run `sudo systemctl reload nginx`

Unfortunately, you need to rerun this each time you restart the notebook VM -- again, until we fix [bug 443670](https://msdata.visualstudio.com/Vienna/_workitems/edit/443670).


## Setting up the Python Environment
The environment you are running should have the latest version of `dask` and `distributed` installed -- run this code in the terminal to make sure:

    conda activate py36
    pip install --upgrade dask distributed


Or, if you want to be on the safe side, create a new conda environment using this [environment.yml](dask/environment.yml) file like so:

    conda env create -f dask/environment.yml  
    conda activate dask
    python -m ipykernel install --user --name dask --display-name "Python (dask)"

![](img/dask-status.gif)

