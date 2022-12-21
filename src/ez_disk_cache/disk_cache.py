import inspect
import pickle
import shelve
import shutil
import time
import uuid
import warnings
from datetime import datetime
from pathlib import Path
from typing import Optional, Any, Generator, Dict, Union, Literal, Type, List, Tuple
from abc import ABC
from dataclasses import dataclass
import logging

import yaml


_LOGGER = logging.getLogger(__name__)

ENABLE_WARNINGS = True  # Set this to False, in order to avoid emitting "warnings.warn()" messages

_ITERABLE_TYPES = (list, tuple, Generator)

_CONFIG_YAML_FILENAME = "config.yaml"
_SINGLE_CACHE_VALUE_FILENAME = "single-cache.bin"
_ITERABLE_CACHE_VALUE_FILENAME = "iterable-cache.shelf.bin"
_LAST_USAGE_FILENAME = ".last-usage"


@dataclass  # --> Using dataclass here to provide a reasonable __eq__ function; see _cache_is_compatible() below
class DiskCacheConfig(ABC):
    """This type is the base for parameters passed to the decorated function. It must be sub-typed by the user."""
    def _to_dict(self) -> Dict[str, Any]:
        """
        Converts a config object to a dictionary.

        Must be overridden by the user as soon as the config object contains custom or hierarchical data types. However,
        basic Python data types (int, float, str, bool) work out of the box.
        """
        return self.__dict__

    @classmethod
    def _from_dict(cls, dict_: Dict[str, Any]) -> "DiskCacheConfig":
        """
        Converts a dictionary back into a config object.

        Must be overridden by the user as soon as the config object contains custom or hierarchical data types. However,
        basic Python data types (int, float, str, bool) work out of the box.
        """
        return cls(**dict_)

    @staticmethod
    def _cache_is_compatible(passed_to_decorated_function: "DiskCacheConfig", loaded_from_cache: "DiskCacheConfig") \
            -> bool:
        """
        Checks compatibility between two config objects. Decides if there is a cache hit (True), or a cache
        miss (False). By default, we check both config objects for equality.

        Must be overriden by the user in any of the following cases:
          - the config object contains custom or hierarchical data types,
          - only a part of the enclosed data fields is necessary to determine cache compatibility.
        """
        return passed_to_decorated_function == loaded_from_cache


def disk_cache(
        cache_root_folder: Optional[Union[str, Path]] = None,
        max_cache_root_size_mb: Optional[float] = None, max_cache_instances: Optional[int] = None,
        iterable_loading_strategy: Literal["lazy-load-discard", "lazy-load-keep", "completely-load-to-memory"] =
        "lazy-load-keep",
        cache_name_suffix: Optional[str] = None):
    """
    This decorator provides smart disk-caching for results of long-running functions. In case the decorated function
    returns an Iterable (List/Tuple/Generator), its items are saved to a shelf so that they can be accessed
    individually: This is useful in conjunction with large pieces of data, to optionally lazy-load single items.

    Technically, the decorated function may expect any number of parameters, as long as it receives exactly one
    config object which inherits from DiskCacheConfig. It supplies the decorated function with parameters and offers
    logic to detect cache hits/misses.
    Nevertheless -to prevent potential logical inconsistencies- we recommend that the decorated function expects
    only the config object (and optionally a self reference).

    @param cache_root_folder: The root folder that contains all cache instances. Cache instances are represented as
                            sub-folders within the cache root folder.
    @param max_cache_root_size_mb: Maximum size of the cache root folder in MB. If None, there is no size limit.
    @param max_cache_instances: Maximum number of cache instances within the cache root folder. If None, there is no
                                limit of cache instances.
    @param iterable_loading_strategy: States how Iterable (List/Tuple/Generator) types should be handled when
                                      loading from cache.
                                      - Use "lazy-load-discard" to load each element from disk
                                        and discard afterwards; this potentially saves RAM.
                                      - "lazy-load-keep" loads so-far unused elements from disk and keeps them in RAM.
                                      - "completely-load-to-memory" a priori loads all items to RAM before continuing.
    @param cache_name_suffix: If not None, this suffix will be part of the generated cache instance folder.
    """
    assert max_cache_instances is None or max_cache_instances > 0, "Value greater than 0 expected!"
    assert max_cache_root_size_mb is None or max_cache_root_size_mb > 0, "Value greater than 0 expected!"
    assert iterable_loading_strategy in ("lazy-load-discard", "lazy-load-keep", "completely-load-to-memory"), \
        "Invalid iterable_loading_strategy parameter!"

    if cache_name_suffix is not None:
        assert all(char not in cache_name_suffix for char in " /\n\\, ."), "cache_name_suffix contains invalid chars!"
        cache_name_suffix = cache_name_suffix.strip("_- ")
        if cache_name_suffix == "":
            cache_name_suffix = None

    def decorating_function(user_function):
        _cache_root_folder = cache_root_folder
        if _cache_root_folder is None:
            _cache_root_folder = Path.cwd() / f"{user_function.__name__}_cache_root"
            _LOGGER.info(f"No specific cache root folder given. Using '{_cache_root_folder}'")
        elif isinstance(_cache_root_folder, str):
            _cache_root_folder = Path(_cache_root_folder)
        assert _cache_root_folder.is_dir() or _cache_root_folder.parent.is_dir(), \
            f"Neither provided cache_root_folder {_cache_root_folder} nor its parent {_cache_root_folder.parent} exist!"
        if not _cache_root_folder.exists():
            _cache_root_folder.mkdir(parents=False, exist_ok=True)

        wrapper = _disk_cache_wrapper(
            user_function=user_function, cache_root_folder=_cache_root_folder,
            max_cache_root_size_mb=max_cache_root_size_mb, max_cache_instances=max_cache_instances,
            iterable_loading_strategy=iterable_loading_strategy, cache_name_suffix=cache_name_suffix)
        return wrapper

    return decorating_function


def _get_config_from_params(user_function, args: Tuple, kwargs: Dict[str, Any]) -> DiskCacheConfig:
    _passed_args = [*args] + [a for a in kwargs.values()]
    _config_objects = [arg for arg in _passed_args if isinstance(arg, DiskCacheConfig)]
    assert len(_config_objects) == 1, f"Decorated function '{user_function.__name__}' must be supplied with exactly " \
                                      f"ONE parameter of type {DiskCacheConfig.__name__}!"
    _n_args_by_function_signature = len(inspect.signature(user_function).parameters)
    _n_args = max(len(_passed_args), _n_args_by_function_signature)
    if ENABLE_WARNINGS is True and _n_args > 1:
        _msg = f"Warning: Decorated function '{user_function.__name__}' expects {_n_args} parameters (" \
               f"including the disk_cache config). Though having more than one parameter is fully okay, we strongly " \
               f"recommend passing the config object as the only function parameter, and -if possible- making the " \
               f"function a @staticmethod. Everything else might lead to logical inconsistencies in cache generation."
        _LOGGER.warning(_msg)
        warnings.warn(_msg)
    return _config_objects[0]


def _disk_cache_wrapper(user_function, cache_root_folder: Path, max_cache_root_size_mb: Optional[float],
                        max_cache_instances: Optional[int], iterable_loading_strategy: str,
                        cache_name_suffix: Optional[str]):
    def wrapper(*args, **kwargs) -> Any:
        _config = _get_config_from_params(user_function=user_function, args=args, kwargs=kwargs)

        _cache_instance_path = _lookup_cache(config_provided_to_user_function=_config,
                                             cache_root_folder=cache_root_folder)
        if _cache_instance_path is not None:
            _LOGGER.info(f"Compatible cache instance found: {_cache_instance_path.name}")
            _cached_data = _read_cache_instance(cache_instance_path=_cache_instance_path,
                                                iterable_loading_strategy=iterable_loading_strategy)
            return _cached_data

        # No compatible cache instance found --> Generate new cache instance
        _cache_instance_path = _create_new_cache_path(cache_root_folder=cache_root_folder,
                                                      cache_name_suffix=cache_name_suffix)
        _LOGGER.info(f"No compatible cache instance found. Generating new cache instance '{_cache_instance_path.name}'")
        _started_at = datetime.now()
        try:
            _user_data = user_function(*args, **kwargs)
            _is_iterable = isinstance(_user_data, _ITERABLE_TYPES)
            if _is_iterable is True:
                _LOGGER.debug("Saving iterable user data as shelf")
                with shelve.open(str(_cache_instance_path / _ITERABLE_CACHE_VALUE_FILENAME), flag="n") as _shelf:
                    for idx, v in enumerate(_user_data):
                        _shelf[str(idx)] = v
                if isinstance(_user_data, Generator):
                    _user_data.close()
            elif _is_iterable is False:
                _LOGGER.debug("Saving user data as single blob")
                with open(file=_cache_instance_path / _SINGLE_CACHE_VALUE_FILENAME, mode="wb") as _file:
                    pickle.dump(_user_data, _file)
            with open(_cache_instance_path / _CONFIG_YAML_FILENAME, mode="w") as _file:
                yaml.dump(_config._to_dict(),  # noqa Accessing private member is okay here!
                          stream=_file, encoding="utf-8", sort_keys=False)
        except (SystemExit, KeyboardInterrupt):
            _LOGGER.info("Generation of cache instance was aborted by the user/system. Removing intermediate results.")
            shutil.rmtree(_cache_instance_path)
            raise
        except BaseException:
            _LOGGER.info("Generation of cache instance was aborted due to an error. Removing intermediate results.")
            shutil.rmtree(_cache_instance_path)
            raise
        _duration = datetime.now() - _started_at
        _LOGGER.info(f"Successfully generated cache instance '{_cache_instance_path.name}'. It took {_duration}.")

        # If we don't meet the user-imposed size limits, do some cleanup
        _cleanup(cache_root_folder=cache_root_folder, max_cache_root_size_mb=max_cache_root_size_mb,
                 max_cache_elements=max_cache_instances, youngest_cache_instance_path=_cache_instance_path)

        # Read the cache now and return the contents to the user
        _cached_data = _read_cache_instance(cache_instance_path=_cache_instance_path,
                                            iterable_loading_strategy=iterable_loading_strategy)
        return _cached_data

    def cache_root_info() -> Dict[str, Any]:
        """Returns stats about the cache root folder"""
        return _get_cache_root_folder_stats(cache_root_folder=cache_root_folder)

    def cache_root_clear() -> None:
        """Clears all cache instances. Note that LazyList instances pointing to sub-caches might fail afterwards!"""
        for sub_path in cache_root_folder.iterdir():
            shutil.rmtree(sub_path)

    wrapper.cache_root_info = cache_root_info
    wrapper.cache_root_clear = cache_root_clear
    wrapper.cache_root_folder = cache_root_folder
    return wrapper


def _lookup_cache(config_provided_to_user_function: DiskCacheConfig, cache_root_folder: Path) -> Optional[Path]:
    """Checks all cache instances and looks if any of them matches the given config."""
    _LOGGER.debug("Start looking for a compatible cache instance.")
    _config_subtype: Type[DiskCacheConfig] = type(config_provided_to_user_function)
    _cache_subdirs = [path for path in cache_root_folder.iterdir() if path.is_dir()]
    for _sub_path in _cache_subdirs:
        _config_yaml_path = _sub_path / _CONFIG_YAML_FILENAME
        if _config_yaml_path.is_file():
            try:
                with open(_config_yaml_path, mode="r", encoding="utf-8") as file:
                    _loaded_yaml_config_dict = yaml.safe_load(file)
                _loaded_yaml_config = \
                    _config_subtype._from_dict(_loaded_yaml_config_dict)  # noqa Accessing private member is okay here!
            except BaseException as e:
                _msg = f"While parsing cache instance '{_sub_path.name}', an unexpected error occurred. " \
                       f"Skipping cache instance.  Original error message: {str(e)}"
                _LOGGER.warning(_msg)
            else:  # No exception occurred. Check if cache instance is compatible to what we look for..
                if _config_subtype._cache_is_compatible(  # noqa Accessing private member is okay here!
                        passed_to_decorated_function=config_provided_to_user_function,
                        loaded_from_cache=_loaded_yaml_config):
                    return _sub_path
    _LOGGER.debug("Could not find any compatible cache instance")
    return None


def _read_cache_instance(cache_instance_path: Path, iterable_loading_strategy: str) -> Any:
    """Reads a cache instance. Distinguishes between simple cache "blobs" and iterables (shelves)."""
    _LOGGER.debug(f"Start loading cache instance '{cache_instance_path.name}'")
    # Try writing to the last-usage-file, in order to keep track of most-recently used cache instance
    try:
        with open(file=cache_instance_path / _LAST_USAGE_FILENAME, mode="w") as file:
            file.write(f"{time.time_ns():.3f}")
    except IOError:
        # It's alright if we cannot write to this file!
        _LOGGER.debug(f"Couldn't write to last-usage file '{cache_instance_path.name}/{_LAST_USAGE_FILENAME}'.")

    _is_iterable = any(p.name.startswith(_ITERABLE_CACHE_VALUE_FILENAME) for p in cache_instance_path.iterdir())
    if _is_iterable is True:
        _shelf_filename = cache_instance_path / _ITERABLE_CACHE_VALUE_FILENAME
        if iterable_loading_strategy in ("lazy-load-discard", "lazy-load-keep"):
            _LOGGER.debug("Lazy-loading iterable")
            return LazyList(cache_file_path=_shelf_filename, iterable_loading_strategy=iterable_loading_strategy)
        elif iterable_loading_strategy == "completely-load-to-memory":
            _LOGGER.info("Loading iterable to RAM. This could take a while..")
            with shelve.open(str(_shelf_filename), flag="r") as shelf:
                keys = sorted(shelf.keys())
                return [shelf[k] for k in keys]
        else:
            raise RuntimeError("We should not have ended-up here!")
    elif _is_iterable is False:
        with open(file=cache_instance_path / _SINGLE_CACHE_VALUE_FILENAME, mode="rb") as file:
            return pickle.load(file)


class LazyList:
    """Lazily loads iterables. The interface resembles the one of Python lists."""
    def __init__(self, cache_file_path: Path, iterable_loading_strategy: str):
        self._cache_file_path = cache_file_path
        self._shelf = shelve.open(str(cache_file_path), flag="r")
        self._is_open = True
        self._len = len(self._shelf)
        self._iterable_loading_strategy = iterable_loading_strategy
        # Prepare the storage for lazily loaded object, in case we want to keep them in memory
        if iterable_loading_strategy == "lazy-load-keep":
            self._storage_data_loaded: List[bool] = [False for _ in range(self._len)]
            self._storage: List[Any] = [None for _ in range(self._len)]
            self._storage_n_loaded: int = 0
        # Make sure there are no gaps in the keys range. Note, that there are only numerical keys.
        valid_keys = range(len(self._shelf))
        assert all(int(k) in valid_keys for k in self._shelf.keys()), \
            f"At least one of the key is not in the valid keys range {valid_keys}"
        _LOGGER.debug(f"Successfully opened the shelf for '{cache_file_path.parent.name}' with {self._len} keys")

    def __len__(self):
        return self._len

    def __getitem__(self, idx: int):
        if not -len(self) < idx < len(self):
            raise IndexError(f"Index {idx} out of bounds (len={len(self)})")
        if self._iterable_loading_strategy == "lazy-load-keep" and self._storage_data_loaded[idx] is True:
            return self._storage[idx]
        datum = self._shelf[str(idx)]
        if self._iterable_loading_strategy == "lazy-load-keep":
            self._storage_data_loaded[idx] = True
            self._storage[idx] = datum
            self._storage_n_loaded += 1
            if self._storage_n_loaded == self._len:
                self._close()
        return datum

    def __str__(self) -> str:
        if self._iterable_loading_strategy == "lazy-load-keep":
            return f"[{self._len} lazy objects, of which {self._storage_n_loaded} are held in RAM]"
        return f"[{self._len} lazy objects]"

    def __hash__(self):
        return hash(self._len) + hash(self._cache_file_path)

    def __eq__(self, other: "LazyList") -> bool:
        if not isinstance(other, LazyList) or len(self) != len(other):
            return False
        if any(self[i] != other[i] for i in range(len(self))):
            return False
        return True

    def __iter__(self):
        for idx in range(self._len):
            yield self[idx]

    def _close(self):
        if self._is_open is False:
            return
        _LOGGER.debug(f"Successfully closed the shelf for '{self._cache_file_path.parent.name}'")
        self._shelf.close()
        self._is_open = False

    def __del__(self):
        self._close()


def _create_new_cache_path(cache_root_folder: Path, cache_name_suffix: Optional[str]) -> Path:
    """Creates the sub-folder for a new cache instance."""
    _suffix = "" if cache_name_suffix is None else f"__{cache_name_suffix}"
    _uuid = str(uuid.uuid4())[:8]
    _subdir_name = datetime.now().strftime(f"%Y-%m-%d__%H-%M__{_uuid}__cache{_suffix}")
    _subdir_path = cache_root_folder / _subdir_name
    assert not _subdir_path.exists(), f"Cache instance '{_subdir_path}' already exists, which it shouldn't!"
    _subdir_path.mkdir(parents=False, exist_ok=False)
    return _subdir_path


def _get_cache_root_folder_stats(cache_root_folder: Path) -> Dict[str, Any]:
    n_instances = len([path for path in cache_root_folder.iterdir() if path.is_dir()])
    overall_size_mb = sum(p.stat().st_size for p in cache_root_folder.glob("**/*") if p.is_file()) / (1024 ** 2)
    return {"n_cache_instances": n_instances, "cache_root_size_mb": overall_size_mb}


def _get_cache_instance_last_usage_time(cache_instance_path: Path) -> float:
    """Returns the last usage of a cache instance, as seconds since epoch."""
    _last_usage_file = cache_instance_path / _LAST_USAGE_FILENAME
    if _last_usage_file.is_file():
        try:
            with open(_last_usage_file, mode="r", encoding="utf-8") as _file:
                return float(_file.readline())
        except (IOError, ValueError):
            if ENABLE_WARNINGS is True:
                _LOGGER.warning(f"Contents of last-usage file '{cache_instance_path}/{_LAST_USAGE_FILENAME}' could "
                                f"not be parsed!")
            return _last_usage_file.stat().st_mtime
    return cache_instance_path.stat().st_mtime  # Acts as fallback, if (for whatever reason) no last-usage file exists


def _cleanup(cache_root_folder: Path, max_cache_root_size_mb: Optional[float], max_cache_elements: Optional[int],
             youngest_cache_instance_path: Path) -> None:
    def _get_cache_root_status_quo() -> Tuple[int, float, bool]:
        _stats_dict = _get_cache_root_folder_stats(cache_root_folder=cache_root_folder)
        _cleanup_necessary = \
            max_cache_root_size_mb is not None and _stats_dict["cache_root_size_mb"] > max_cache_root_size_mb or \
            max_cache_elements is not None and _stats_dict["n_cache_instances"] > max_cache_elements
        return _stats_dict["n_cache_instances"], _stats_dict["cache_root_size_mb"], _cleanup_necessary

    _n_instances, _overall_size_mb, _cleanup_necessary = _get_cache_root_status_quo()
    if _cleanup_necessary is True:
        _LOGGER.info(f"Cache root folder exceeds its limits, start doing some cleanup..   "
                     f"(overall_cache_size_mb={_overall_size_mb:,.1f}, n_cache_instances={_n_instances:,})")
        _n_deleted = 0
        _instance_usage_time__paths = \
            {_get_cache_instance_last_usage_time(path): path for path in cache_root_folder.iterdir() if
             path.is_dir() and path != youngest_cache_instance_path}
        _sorted_deletable_paths = [p for _, p in sorted(_instance_usage_time__paths.items(), key=lambda item: item[0])]
        while _n_instances >= 2 and _cleanup_necessary is True:
            shutil.rmtree(_sorted_deletable_paths[0])
            _n_deleted += 1
            _sorted_deletable_paths = _sorted_deletable_paths[1:]
            _n_instances, _overall_size_mb, _cleanup_necessary = _get_cache_root_status_quo()
        if _n_instances == 1 and _cleanup_necessary is True and ENABLE_WARNINGS is True:
            _msg = f"Warning: Removed all cache instances besides of the one that was just created. Yet still, the " \
                   f"cache root folder ends up being larger than allowed " \
                   f"({_overall_size_mb:.1f}>{max_cache_root_size_mb:.1f}). Perhaps the parameter " \
                   f"'max_cache_root_size_mb' is configured too restrictively?"
            warnings.warn(_msg)
            _LOGGER.warning(_msg)
        else:
            _LOGGER.info(f"Successfully removed {_n_deleted} cache instances. There are {_n_instances} remaining.")
        assert _n_instances >= 1  # Makes sure we end up with at least the lastly generated cache instance..
