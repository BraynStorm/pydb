from typing import TYPE_CHECKING
from app import *

if TYPE_CHECKING:
    from gevent.pywsgi import WSGIServer


def pydb_rest_server(bind_ip: str = "0.0.0.0", bind_port: int = 80) -> "WSGIServer":
    import pydb_rest.app
    from gevent.pywsgi import WSGIServer

    return WSGIServer(bind_ip, bind_port, pydb_rest.app.app)
