import atexit
from multiprocessing.util import _exit_function

from pyxxl import ExecutorConfig, PyxxlRunner

#bind = ["0.0.0.0:8000"]
backlog = 512
workers = 1
timeout = 300
graceful_timeout = 2
limit_request_field_size = 8192

admin_host = "20.66.161.21"
executor_host= "20.66.161.21"

def when_ready(server):
    # pylint: disable=import-outside-toplevel,unused-import,no-name-in-module
    from app import xxl_handler

    atexit.unregister(_exit_function)

    config = ExecutorConfig(
        xxl_admin_baseurl=f"http://{admin_host}:6588/xxl-job-admin/api/",
        executor_app_name="xxl-job-executor-sample",
	access_token='default_token',
	executor_host=f"{executor_host}",
	#executor_listen_host="0.0.0.0",
        #executor_url="http://172.17.0.1:9999",
    )

    pyxxl_app = PyxxlRunner(config, handler=xxl_handler)
    server.pyxxl_app = pyxxl_app
    pyxxl_app.run_with_daemon()
