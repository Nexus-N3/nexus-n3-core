"""Gateway discovery utilities."""

import importlib
import inspect
import pkgutil

import nexus_n3.gateway.gateways as gateways_pkg


# simple dictionary of gateways that are implemented. 
def discover_gateways() -> dict[str, type]:
    """
    Discover gateway classes in the gateways package.

    Returns:
        Mapping of gateway key -> gateway class.
    """
    gateways: dict[str, type] = {}
    prefix = gateways_pkg.__name__ + "."

    for _, module_name, _ in pkgutil.walk_packages(
        gateways_pkg.__path__, prefix
    ):
        module = importlib.import_module(module_name)
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if inspect.isclass(attr) and attr_name.lower().endswith("gateway"):
                key = module_name.split(".")[-1]
                if key == "gateway_interface" or key == "gateway_registry":
                    continue
                if getattr(attr, "transport_role", "gateway") != "gateway":
                    continue
                gateways[key.lower()] = attr

    return gateways
