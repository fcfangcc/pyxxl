import os
from urllib.parse import urlparse

import pytest

from pyxxl.setting import ExecutorConfig
from pyxxl.utils import get_network_ip

TEST_ADMIN_URL = "http://localhost:8080/xxl-job-admin/api/"


def test_config():
    setting = ExecutorConfig(
        xxl_admin_baseurl=TEST_ADMIN_URL,
        executor_app_name="test",
        dotenv_try=False,
    )
    assert urlparse(setting.executor_url).hostname == get_network_ip()
    assert setting.executor_app_name == "test"

    setting = ExecutorConfig(
        xxl_admin_baseurl=TEST_ADMIN_URL,
        executor_app_name="test",
        executor_url="http://127.0.0.1",
        dotenv_try=False,
    )
    assert setting.executor_listen_port == 80
    assert setting.executor_listen_host == "127.0.0.1"
    assert setting.executor_app_name == "test"

    # from env
    os.environ["executor_app_name"] = "fromenv"
    os.environ["XXL_ADMIN_BASEURL"] = TEST_ADMIN_URL
    os.environ["GRACEFUL_TIMEOUT"] = "500"
    os.environ["GRACEFUL_CLOSE"] = "False"

    setting = ExecutorConfig(
        xxl_admin_baseurl="",
        executor_app_name="",
        dotenv_try=True,
    )
    assert setting.executor_app_name == "fromenv"
    assert setting.xxl_admin_baseurl == TEST_ADMIN_URL
    assert setting.graceful_timeout == 500
    assert setting.graceful_close is False
    os.environ.clear()


@pytest.mark.parametrize(
    "msg,error,kwargs",
    [
        ("admin_url", ValueError, dict(xxl_admin_baseurl="dddd", executor_app_name="test")),
        ("executor_app_name", ValueError, dict(xxl_admin_baseurl=TEST_ADMIN_URL, executor_app_name="")),
        (
            "log_local_dir",
            ValueError,
            dict(
                xxl_admin_baseurl=TEST_ADMIN_URL,
                executor_app_name="test",
                log_target="disk",
                log_local_dir="",
            ),
        ),
        (
            "log_redis_uri",
            ValueError,
            dict(
                xxl_admin_baseurl=TEST_ADMIN_URL,
                executor_app_name="test",
                log_target="redis",
                log_redis_uri="",
            ),
        ),
    ],
)
def test_error(msg, error, kwargs):
    with pytest.raises(error, match=msg):
        ExecutorConfig(**kwargs, dotenv_try=False)
