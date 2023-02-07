from pyxxl.utils import try_import


def test_import():
    assert try_import("pandas") is None
    assert try_import("aiohttp")
