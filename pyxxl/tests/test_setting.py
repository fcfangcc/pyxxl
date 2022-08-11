import os

import pytest

from pyxxl.setting import ExecutorConfig
from pyxxl.utils import get_network_ip


TEST_ADMIN_URL = "http://localhost:8080/xxl-job-admin/api/"


def test_config():
    # not required
    with pytest.raises(ValueError, match="admin_url"):
        ExecutorConfig(xxl_admin_baseurl="dddd", executor_app_name="test")

    with pytest.raises(ValueError, match="executor_app_name"):
        ExecutorConfig(xxl_admin_baseurl=TEST_ADMIN_URL, executor_app_name="")

    setting = ExecutorConfig(
        xxl_admin_baseurl=TEST_ADMIN_URL,
        executor_app_name="test",
    )
    setting.executor_host == get_network_ip()
    setting.executor_app_name == "test"

    # from env
    os.environ["executor_app_name"] = "fromenv"
    os.environ["XXL_ADMIN_BASEURL"] = TEST_ADMIN_URL

    setting = ExecutorConfig(xxl_admin_baseurl="", executor_app_name="")
    setting.executor_app_name = "fromenv"
    setting.xxl_admin_baseurl = TEST_ADMIN_URL
