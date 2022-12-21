# ez-disk-cache
A decorator that provides smart disk-caching for results of long-running or memory-intensive functions.

It provides the following features:
- Management of multiple coexisting cache instances,
- Automatic cleanup in order to keep user-defined quota,
- If the decorated function returns an Iterable (List/Tuple/Generator), the values are automatically stored in a shelf and can be retrieved lazily with optional, subsequent discarding. This enables the application to handle sequences of large data chunks that altogether wouldn't fit into memory.

Cache instances are organized as sub-folders inside a **cache root folder**. The latter optionally can be defined by the user and gets passed to the decorator. If not provided by the user, the default cache root location is `main_script_location/<name of decorated function>_cache_root`. Nevertheless, the user is encouraged to choose a **unique cache root folder** for each decorated function, since *ez-disk-cache* might output cryptic warning messages in case two functions share a mutual cache root folder.

```python
import time
from dataclasses import dataclass
from ez_disk_cache import DiskCacheConfig, disk_cache

@dataclass
class Config(DiskCacheConfig):
    number: int
    color: str

@disk_cache()  # <-- Cache root folder goes here
def long_running_function(config: Config):  # <-- Only the config parameter object should be here
    time.sleep(2)  # Do heavy stuff here
    return LargeObjectThatTakesLongToCreate()

long_running_function(config=Config(42, "hello"))  # Takes a long time
long_running_function(config=Config(42, "hello"))  # Returns immediately

print(long_running_function.cache_root_folder)  # Prints the location of cache root folder
```

### Config parameter object
When calling the decorated function, *ez-disk-cache* decides if there is a matching cache instance. This is done via a **config parameter object**, which is passed to the decorated function. It has to be a *dataclass* and inherit from `DiskCacheConfig`.

Please note: It is strongly recommended that the decorated function accepts the config parameter object as its **only parameter**! Nevertheless, the user may feel free to pass as many arguments to the function as desired ‒ as long as they do not influence the to-be-cached data!

## Installation
```bash
pip install ez-disk-cache
```

## Iterables (List/Tuple/Generator)
At cache generation ‒in case an Iterable is returned from a decorated function‒ the Iterable is always saved to a shelf file. This keeps the items individually addressable afterwards.

Loading a cached Iterable can be done in multiple ways, which is defined by providing the `iterable_loading_strategy` parameter to the *ez-disk-cache* decorator:
- `completely-load-to-memory` loads all items to RAM prior to returning them in a `list` to the application,
- `lazy-load-discard` returns a `LazyList` to the application. Each time the user accesses an item, it is loaded from disk and discarded right after using. This option might be preferable when working with sequences of large data items, which altogether barely fit in RAM.
- `lazy-load-keep` returns a `LazyList` to the application. With each access, an item is loaded from disk and cached in RAM. Next accesses to the same item will take place without any delay from accessing disk.

```python
@disk_cache(iterable_loading_strategy="<one of the above values>")
def long_running_function(config: Config):  # <-- Only config parameter object should be here
    objects = []
    for i in range(1000):
        time.sleep(3)  # Do heavy stuff here
        objects += [LargeObjectThatTakesLongToCreate(i)]
    return objects
```

## Usage examples

### Basic example
The following example demonstrates the coexistence of multiple cache instances and their automatic selection.
```python
import time
from dataclasses import dataclass
from ez_disk_cache import DiskCacheConfig, disk_cache

@dataclass
class CarConfig(DiskCacheConfig):
    wheel_diameter: float
    color: str

@disk_cache("/tmp/car_instances")
def construct_car(car_config: CarConfig):  # <-- Only the config parameter object should be here
    time.sleep(5)  # Simulate a long process to construct the car
    return f"A fancy {car_config.color} car with wheels of diameter {car_config.wheel_diameter}"

# Construct the dark blue car for the first time
start = time.time()
car = construct_car(CarConfig(wheel_diameter=35, color="dark blue"))
print(car)
print(f"Construction took {time.time()-start:.2f} seconds\n")

# Construct a red car with the same wheel diameter
start = time.time()
car = construct_car(CarConfig(wheel_diameter=35, color="red"))
print(car)
print(f"Construction took {time.time()-start:.2f} seconds\n")

# Now let's see if there is still the dark blue car
start = time.time()
car = construct_car(CarConfig(wheel_diameter=35, color="dark blue"))
print(car)
print(f"Construction took {time.time()-start:.2f} seconds\n")
```

Expected output:
```
A fancy dark blue car with wheels of diameter 35
Construction took 5.01 seconds

A fancy red car with wheels of diameter 35
Construction took 5.01 seconds

A fancy dark blue car with wheels of diameter 35
Construction took 0.00 seconds
```
Since the caches keep existing after the end of a script, the construction of the above cars takes zero time in the second run.
 
### Caching generator results and retrieving as LazyList
The following example shows how *ez-disk-cache* can be used to cache generator function results. This can be particularly helpful when handling huge datasets that won't fit to RAM as a whole.
```python
from dataclasses import dataclass
from typing import List

from ez_disk_cache import DiskCacheConfig, disk_cache, LazyList

@dataclass
class Config(DiskCacheConfig):
    n_items: int

@disk_cache(iterable_loading_strategy="lazy-load-discard")
def long_running_generator_function(config: Config):  # <-- Only the config parameter object should be here
    for _ in range(config.n_items):
        # Heavy workload
        yield DifficultToObtainObject()

objects = long_running_generator_function(config=Config(1000))
assert isinstance(objects, LazyList)
assert len(objects) == 1000

for item in objects:
    process(item)
```

### Usage within class instances
As mentioned above, decorated functions are strongly recommended to expect exactly one parameter: the *config parameter object*. This leads to the fact that decorated class member function are better to be declared a `staticmethod` ‒ in order to avoid the `self` parameter. The short example below shows how to do that.

```python
import time
from dataclasses import dataclass
from ez_disk_cache import DiskCacheConfig, disk_cache

@dataclass
class Config(DiskCacheConfig):
    color: str

class CarDealer:
    def __init__(self):
        self.cars = []
        for color in ("red", "yellow", "blue"):
            self.cars += [self._order_car(config=Config(color))]

    @staticmethod  # <-- This lets us avoid the self parameter in the decorated function
    @disk_cache(cache_root_folder="my/favorite/cache/root/folder")
    def _order_car(config: Config):  # <-- Only the config parameter object should be here
        time.sleep(2)  # Delivery of a car takes some time
        return f"A fancy {config.color} car"

car_dealer = CarDealer()  # First instantiation takes a while
car_dealer = CarDealer()  # Second instantiation returns immediately
print(car_dealer.cars)
```

## Advanced usage

### Quota for the cache root folder
The cache root folders of the above examples were all unbounded. If, however, one wishes the cache root folder not to exceed certain limits, one might apply the following parameters to the decorator:
- `max_cache_root_size_mb` defines a space limit (in MB) for the cache root folder,
- `max_cache_instances` restricts the cache root folder to a maximum number of cache instances.

As soon as a given cache root folder exceeds one of these limits, old cache instances are being deleted. Old instances are those, that were least-recently used (read).

```python
from dataclasses import dataclass
from ez_disk_cache import DiskCacheConfig, disk_cache

@dataclass
class Config(DiskCacheConfig):
    number: int

@disk_cache("my/second/favorite/cache/root/folder", max_cache_instances=2) 
def long_running_function(config: Config):  # <-- Only the config parameter object should be here
    # Do heavy stuff here
    return LargeObjectThatTakesLongToCreate()

long_running_function(config=Config(1))  # Takes a long time
long_running_function(config=Config(2))  # Takes a long time

long_running_function(config=Config(1))  # Finishes quickly. Marks instance 1 as last recently used

long_running_function(config=Config(3))  # Takes a long time. Instance 2 will be deleted accordingly
long_running_function(config=Config(1))  # Finishes quickly
```

### Managing cache root folders 

A decorated function itself offers a few methods that may be used to manage the underlying cache root folder.

```python
from dataclasses import dataclass
from ez_disk_cache import DiskCacheConfig, disk_cache

@dataclass
class Config(DiskCacheConfig):
    number: int

@disk_cache("my/third/favorite/cache/root/folder", max_cache_instances=2) 
def long_running_function(config: Config):  # <-- Only the config parameter object should be here
    # Do heavy stuff here
    return LargeObjectThatTakesLongToCreate()

long_running_function(config=Config(1))  # Takes a long time
long_running_function(config=Config(2))  # Takes a long time

print(long_running_function.cache_root_folder)  # Prints the location of the underlying cache root folder
print(long_running_function.cache_root_info())  # Prints some stats (number of cache instances, space consumption)
long_running_function.cache_root_clear()  # Clears all cache instances from the cache root folder

long_running_function(config=Config(1))  # Takes a long time
long_running_function(config=Config(2))  # Takes a long time
```

### More complex tasks with config objects
A *cache instance* is a sub-folder to the cache root folder; it contains the to-be-cached function results along with a **serialized YAML file** of the respective parameter config object. Each time a decorated function gets called by the user, *ez-disk-cache* walks the pool of available cache instances, deserializes their YAML files and checks if one of them is compatible to the given parameter config object. In the default case, *compatible* means equality of all parameter fields.

To modify *ez-disk-cache's* behavior of how it (de)serializes YAML files and performs compatibility checks, one can override the following config object functions: `_to_dict()`, `_from_dict()` and `_cache_is_compatible()`. 

#### Selectively matching cache configs
The following example shows how to alter the cache-compatibility behaviour of *ez-disk-cache*.

```python
import time
from dataclasses import dataclass

from ez_disk_cache import DiskCacheConfig, disk_cache

@dataclass
class CarConfig(DiskCacheConfig):
    model: str
    color: str  # In this example, we neglect 'color' when searching for compatible cache instances

    @staticmethod
    def _cache_is_compatible(passed_to_decorated_function: "CarConfig", loaded_from_cache: "CarConfig") -> bool:
        """Return True, if a cache instance is compatible. False if not."""
        if passed_to_decorated_function.model == loaded_from_cache.model:
            return True
        return False  # At this point, we don't care about 'color'. Everything that matters is 'model'.

@disk_cache("/tmp/car_rental")
def rent_a_car(car_config: CarConfig):  # <-- Only the config parameter object should be here
    time.sleep(3)  # Renting a car takes some time
    return f"A nice {car_config.color} {car_config.model}, rented for one week!"

rent_a_car(CarConfig(model="Tesla Model X", color="red"))  # Takes a while
rent_a_car(CarConfig(model="Ford Mustang", color="gold"))  # Takes a while

rent_a_car(CarConfig(model="Tesla Model X", color="blue"))  # Returns immediately, since we've already rented a Tesla
```

#### Custom data types within config objects
Config objects were designed in a way that they work out-of-the-box with basic Python data types (int, float, str, bool). If however, the config contains custom or hierarchical data types, the user must provide custom `_to_dict` and `_from_dict` conversion logic.

The following example shows how to *manually* provide support for custom config fields. Since the following involves lots of boilerplate code, users are encouraged to take a look at the [dacite](https://github.com/konradhalas/dacite) package.

```python
import time
from dataclasses import dataclass
from typing import Dict, Any

from ez_disk_cache import DiskCacheConfig, disk_cache

class CustomSubType:
    def __init__(self, a, b):
        self.a, self.b = a, b

@dataclass
class Config(DiskCacheConfig):
    some_number: int
    custom_parameter: CustomSubType

    def _to_dict(self) -> Dict[str, Any]:
        """Converts an object to a dict, such that it can be saved to YAML."""
        dict_ = {
            "some_number": self.some_number,
            "custom_parameter": {"a": self.custom_parameter.a, "b": self.custom_parameter.b}
        }
        return dict_

    @classmethod
    def _from_dict(cls, dict_: Dict[str, Any]) -> "Config":
        """Converts a YAML dict to back an object again."""
        obj = Config(some_number=dict_["some_number"],
                     custom_parameter=CustomSubType(a=dict_["custom_parameter"]["a"], b=dict_["custom_parameter"]["b"]))
        return obj

    @staticmethod
    def _cache_is_compatible(passed_to_decorated_function: "Config", loaded_from_cache: "Config") -> bool:
        """Return True, if a cache instance is compatible. False if not."""
        if passed_to_decorated_function.some_number != loaded_from_cache.some_number:
            return False
        if passed_to_decorated_function.custom_parameter.a != loaded_from_cache.custom_parameter.a:
            return False
        if passed_to_decorated_function.custom_parameter.b != loaded_from_cache.custom_parameter.b:
            return False
        return True

@disk_cache("/tmp/complex_config_subtypes_example")
def long_running_function(car_config: Config):  # <-- Only the config parameter object should be here
    time.sleep(3)  # Do heavy stuff here
    return LargeObjectThatTakesLongToCreate()

long_running_function(Config(some_number=1, custom_parameter=CustomSubType(2, 3)))  # Takes long
long_running_function(Config(some_number=1, custom_parameter=CustomSubType(2, 3)))  # Returns immediately

```
