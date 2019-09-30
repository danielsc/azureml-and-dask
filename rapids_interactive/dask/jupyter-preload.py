from notebook.notebookapp import NotebookApp
from azureml.core import Run
import socket

def dask_setup(scheduler):
    app = NotebookApp()
    ip = socket.gethostbyname(socket.gethostname())
    app.ip="0.0.0.0"
    app.initialize([])
    Run.get_context().log("jupyter-url", "http://" + ip + ":" + str(app.port) + "/?token=" + app.token)
    Run.get_context().log("jupyter-port", app.port)
    Run.get_context().log("jupyter-token", app.token)
    Run.get_context().log("jupyter-ip", ip)