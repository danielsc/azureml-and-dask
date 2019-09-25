from mpi4py import MPI
import os
import argparse
import socket
from azureml.core import Run

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    ip = socket.gethostbyname(socket.gethostname())
    print("- my rank is ", rank)
    print("- my ip is ", ip)
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--data")
    parser.add_argument("--gpus")
    FLAGS, unparsed = parser.parse_known_args()
    
    if rank == 0:
        data = {
            "scheduler"  : ip + ":8786",
            "dashboard"  : ip + ":8787"
            }
        Run.get_context().log("headnode", ip)
        Run.get_context().log("scheduler", data["scheduler"])
        Run.get_context().log("dashboard", data["dashboard"])
        Run.get_context().log("data", FLAGS.data)
    else:
        data = None
        
    data = comm.bcast(data, root=0)
    scheduler = data["scheduler"]
    dashboard = data["dashboard"]
    print("- scheduler is ", scheduler)
    print("- dashboard is ", dashboard)

    
    if rank == 0:
        os.system("dask-scheduler " + "--port " + scheduler.split(":")[1] + " --dashboard-address " + dashboard)
    elif rank == 1:
        os.environ["CUDA_VISIBLE_DEVICES"] = '0,1'  # allow the 1st worker to grab the GPU assigned to the scheduler as well as its own
        os.system("dask-cuda-worker " + scheduler + " --memory-limit 0")
    else:
        os.environ["CUDA_VISIBLE_DEVICES"] = str(rank % int(FLAGS.gpus))  # restrict each worker to their own GPU (assuming one GPU per worker)
        os.system("dask-cuda-worker " + scheduler + " --memory-limit 0")
