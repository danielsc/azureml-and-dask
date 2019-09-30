from notebook.notebookapp import NotebookApp
from azureml.core import Run
import socket

def dask_setup(scheduler):
    app = NotebookApp()
    ip = socket.gethostbyname(socket.gethostname())
    app.ip="0.0.0.0"
    app.initialize([])
    Run.get_context().log("jupyter-server", "http://" + ip + ":" + str(app.port) + "/?token=" + app.token)