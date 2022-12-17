import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Any

import pytest

from src.disk_cache.disk_cache import DiskCacheConfig, disk_cache, LazyList


@pytest.fixture(scope="function")
def _temp_dir() -> Path:
    with TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@dataclass
class _DummyConfig(DiskCacheConfig):
    a: int
    b: str

    @staticmethod
    def _from_dict(dict_: Dict[str, Any]) -> "DiskCacheConfig":
        return _DummyConfig(a=dict_["a"], b=dict_["b"])


def test_different_function_parameters(_temp_dir):
    @disk_cache(cache_root_folder=_temp_dir)
    def _long_running_function__no_param():
        time.sleep(0.3)
        return 42

    @disk_cache(cache_root_folder=_temp_dir)
    def _long_running_function__param_but_not_the_right_one(number: int):
        time.sleep(0.3)
        return 42

    @disk_cache(cache_root_folder=_temp_dir)
    def _long_running_function__single_param(config: _DummyConfig):
        time.sleep(0.3)
        return 42

    @disk_cache(cache_root_folder=_temp_dir)
    def _long_running_function__multiple_params(config: _DummyConfig, number: int, string: str = "hi"):
        time.sleep(0.3)
        return 24

    pytest.raises(AssertionError, _long_running_function__no_param)
    pytest.raises(AssertionError, _long_running_function__param_but_not_the_right_one, number=123)
    assert _long_running_function__single_param(_DummyConfig(1, "2")) == 42

    # Make sure a warning is emitted ("decorated function expects too many arguments")
    assert _long_running_function__multiple_params(_DummyConfig(1, "hello"), 22) == 24
    warnings.filterwarnings("error")  # Makes warnings being raised as errors
    pytest.raises(UserWarning, _long_running_function__multiple_params, _DummyConfig(1, "hello"), 23)


def test_using_timings(_temp_dir):
    pass


def test_iterables(_temp_dir):
    pass


def test_helper_functions__cache_info(_temp_dir):
    @disk_cache(cache_root_folder=_temp_dir)
    def _dummy_fn(config: _DummyConfig):
        return config.a

    _dummy_fn(_DummyConfig(1, "1"))
    _cache_root_info = _dummy_fn.cache_root_info()
    assert _cache_root_info["n_cache_instances"] == 1

    _dummy_fn(_DummyConfig(2, "2"))
    _cache_root_info = _dummy_fn.cache_root_info()
    assert _cache_root_info["n_cache_instances"] == 2

    _dummy_fn(_DummyConfig(3, "3"))
    _cache_root_info = _dummy_fn.cache_root_info()
    assert _cache_root_info["n_cache_instances"] == 3


def test_helper_functions__cache_clear(_temp_dir):
    pass


def test_cleanup(_temp_dir):
    pass
