import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Any, Callable

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


def _assert_duration(duration: float, fn: Callable, *args, **kwargs) -> Any:
    _started_at = time.time()
    _value = fn(*args, **kwargs)
    _duration = time.time() - _started_at
    assert (duration-0.3) <= _duration <= (duration+0.3), f"Duration of {_duration:.3f}s was not in the expected range!"
    return _value


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
    assert _long_running_function__multiple_params(_DummyConfig(1, "hello"), 22) == 24

    # Make sure a warning is emitted ("decorated function expects too many arguments")
    warnings.filterwarnings("error")  # Makes warnings being raised as errors
    pytest.raises(UserWarning, _long_running_function__multiple_params, _DummyConfig(1, "hello"), 23)


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
    @disk_cache(cache_root_folder=_temp_dir)
    def _dummy_fn(config: _DummyConfig):
        time.sleep(0.5)
        return config.a

    _config_param = _DummyConfig(1, "1")

    _value = _assert_duration(0.5, _dummy_fn, _config_param)
    assert _value == _config_param.a

    _value = _assert_duration(0.0, _dummy_fn, _config_param)
    assert _value == _config_param.a

    _dummy_fn.cache_root_clear()

    _value = _assert_duration(0.5, _dummy_fn, _config_param)
    assert _value == _config_param.a


def test_cleanup(_temp_dir):
    @disk_cache(cache_root_folder=_temp_dir, max_cache_instances=2)
    def _dummy_fn(config: _DummyConfig):
        time.sleep(0.5)
        return config.a

    # Build up cache of two cache instances
    _value = _assert_duration(0.5, _dummy_fn, _DummyConfig(1, "1"))
    assert _value == 1
    _value = _assert_duration(0.5, _dummy_fn, _DummyConfig(2, "2"))
    assert _value == 2

    # Read cache instance #1 and thus mark it as "recently used"
    _value = _assert_duration(0.0, _dummy_fn, _DummyConfig(1, "1"))
    assert _value == 1

    # Generate new cache instance #3 and override instance #2, since it was not recently used
    _value = _assert_duration(0.5, _dummy_fn, _DummyConfig(3, "3"))
    assert _value == 3

    # Make sure that instance #2 was really deleted
    _value = _assert_duration(0.5, _dummy_fn, _DummyConfig(2, "2"))
    assert _value == 2

    # Verify cache instances #2 and #3 are present
    _value = _assert_duration(0.0, _dummy_fn, _DummyConfig(2, "2"))
    assert _value == 2
    _value = _assert_duration(0.0, _dummy_fn, _DummyConfig(3, "3"))
    assert _value == 3

    # Generate new cache instance #4 and override instance #2, since it was not recently used
    _value = _assert_duration(0.5, _dummy_fn, _DummyConfig(4, "4"))
    assert _value == 4


def test_iterables__list(_temp_dir):
    @disk_cache(cache_root_folder=_temp_dir, iterable_loading_strategy="completely-load-to-memory")
    def _dummy_fn(config: _DummyConfig):
        time.sleep(0.5)
        return [config.b for _ in range(config.a)]

    _values = _assert_duration(0.5, _dummy_fn, _DummyConfig(1000, "text"))
    assert isinstance(_values, list) and len(_values) == 1000 and all(v == _values[0] for v in _values)

    _values = _assert_duration(0.0, _dummy_fn, _DummyConfig(1000, "text"))
    assert isinstance(_values, list) and len(_values) == 1000 and all(v == _values[0] for v in _values)


def test_iterables__lazy_list_and_generator(_temp_dir):
    @disk_cache(cache_root_folder=_temp_dir, iterable_loading_strategy="lazy-load-discard")
    def _dummy_fn(config: _DummyConfig):
        time.sleep(0.5)
        for _ in range(config.a):
            yield config.b

    _values = _assert_duration(0.5, _dummy_fn, _DummyConfig(500, "text"))
    assert isinstance(_values, LazyList) and len(_values) == 500 and all(v == _values[0] for v in _values)

    _values = _assert_duration(0.0, _dummy_fn, _DummyConfig(500, "text"))
    assert isinstance(_values, LazyList) and len(_values) == 500 and all(v == _values[0] for v in _values)

    # Test LazyList's iterator
    for _item in _values:
        assert _item == "text"
