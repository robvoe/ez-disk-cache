# disk_cache
A decorator that provides smart disk-caching for results of long-running or memory-intensive functions.

It provides the following features:
- Management of multiple parallel cache instances,
- Automatic cleanup in order to keep user-defined quota,
- If the decorated function returns an Iterable (List/Tuple/Generator), the values are automatically stored in a shelf and can be retrieved lazily with optional, subsequent discarding. This enables the application to handle sequences of large data chunks that altogether wouldn't fit into memory.

Cache instances are organized as sub-folders inside a **cache root folder**. The latter can be defined by the user and gets passed to the decorator.

## Installation
```bash
pip install disk_cache
```

## Usage examples


## Managing a cache root folder

