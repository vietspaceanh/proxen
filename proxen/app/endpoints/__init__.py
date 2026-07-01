import importlib
import pkgutil

from blacksheep import RoutesRegistry

registry = RoutesRegistry()

get = registry.get
post = registry.post
put = registry.put
delete = registry.delete
patch = registry.patch
head = registry.head
ws = registry.ws
route = registry.route

# Import every sibling endpoint module so its decorators populate `registry`.
for _m in pkgutil.iter_modules(__path__):
    importlib.import_module(f"{__name__}.{_m.name}")
