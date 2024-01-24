from flask import Flask

app = Flask(__name__)


@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


if __name__ == "__main__":
    # 如果是多进程部署的flask无法这样使用，请参考gunicorn的example
    # 此案例仅用于本地开发和调试用
    from multiprocessing import freeze_support

    from executor_app import app as pyxxl_app

    freeze_support()

    app.pyxxl_app = pyxxl_app
    pyxxl_app.run_with_daemon()
    app.run()
