"""
Nexus N3 Core Gateway Server Entry Point
==================================

This module provides a command-line entry point for running the Nexus N3
gateway server. It supports multiple gateway implementations, which
are automatically discovered from the `nexus_n3.gateway.gateways` package.

env variables are now located in /config/runtime.env

"""

import asyncio
import time
import argparse
from pathlib import Path
import os
import sys
import json
from importlib import metadata
from threading import Thread
from threading import Event
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

from nexus_n3.bridge.bridge_registry import create_bridge, discover_bridges
from nexus_n3.core.pipeline_diagnostics import pipeline_diagnostics
from nexus_n3.core.runtime_env import load_runtime_env
from nexus_n3.gateway.server import Server
from nexus_n3.gateway.gateways.gateway_registry import discover_gateways
from nexus_n3.data_file_offload.sinks.usb import USBDiskManager
from nexus_n3.robots.config.loader import RobotConfigError, load_robot_config
from nexus_n3.robots.runtime.factory import build_robot
from nexus_n3.robots.runtime.service import RobotService
from nexus_n3.plugins.dev.bootstrap import prepare_dev_plugins
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig


def _env_flag(name: str, default: bool = False) -> bool:
    value = str(os.environ.get(name, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _env_csv_list(name: str) -> list[str]:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _runtime_site_name(cli_site: str | None = None) -> str:
    """Return the effective site label for runtime startup."""
    candidate = str(cli_site or os.environ.get("AZURE_IOT_SITE") or "").strip()
    return candidate or "local"


def _neia_apps_catalog_url() -> str:
    # gets the app framework catalog endpoint from the runtime env or defaults to localhost
    load_runtime_env()
    return os.getenv("NEXUS_N3_NEIA_APPS_CATALOG_URL", "http://127.0.0.1:8050/api/v1/apps/catalog")


def _output_root() -> str:
    """Return the configured local fallback output root."""
    load_runtime_env()
    return os.getenv("NEXUS_N3_OUTPUT_ROOT", "nexus_n3_outputs")


def _build_robot_service() -> RobotService | None:
    """Create the optional robot runtime for nodes configured as robots."""
    try:
        config = load_robot_config()
    except RobotConfigError as exc:
        print(f"[ROBOT] Robot service disabled: {exc}")
        return None

    robot = build_robot(config)
    if robot is None:
        print("[ROBOT] Robot service disabled: is_robot=false")
        return None

    return RobotService(robot)


def _fetch_neia_app_inventory(timeout_seconds: float = 1.0) -> dict[str, list[dict[str, object]]]:
    """Fetch NEIA app catalog from the local NEIA API when available."""
    try:
        with urlopen(_neia_apps_catalog_url(), timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError):
        return {"apps": [], "workflows": []}

    apps_payload = payload.get("apps")
    if not isinstance(apps_payload, list):
        return {"apps": [], "workflows": []}

    apps: list[dict[str, object]] = []
    workflows: list[dict[str, object]] = []
    for item in apps_payload:
        if not isinstance(item, dict):
            continue
        normalized = {
            "id": item.get("id"),
            "name": item.get("name"),
            "version": item.get("version"),
            "developer": item.get("developer"),
            "description": item.get("description"),
            "app_type": item.get("app_type"),
            "installed": item.get("installed"),
            "supports_online": item.get("supports_online"),
            "supports_offline": item.get("supports_offline"),
            "compatible_with_subject_delivery": item.get("compatible_with_subject_delivery"),
        }
        if str(item.get("app_type") or "").strip().lower() == "workflow":
            workflows.append(normalized)
        else:
            apps.append(normalized)
    return {"apps": apps, "workflows": workflows}

# bridges are servies that hook into the nexus server and provide remote connectivity to cloud services. 
# They are started in a background thread when enabled.
# the main one is the azure bridge which connects to the azure iot hub.
def start_bridge(
    site: str,
    bridge_name: str | None,
    role: str,
    *,
    customer_id: str | None = None,
    site_id: str | None = None,
    site_name: str | None = None,
    remote_control_enabled: bool = False,
):
    """Start a configured remote bridge in a background thread when enabled."""
    if not bridge_name:
        return None
    if role not in {"standalone", "master"}:
        raise ValueError("--bridge is only supported for standalone or master roles")

    bridge = create_bridge(
        bridge_name,
        site=site,
        customer_id=customer_id,
        site_id=site_id or site,
        site_name=site_name or site,
        remote_control_enabled=remote_control_enabled,
    )
    thread = Thread(
        target=bridge.start,
        kwargs={"install_signal_handlers": False},
        daemon=True,
        name=f"nexus-n3-{bridge_name}",
    )
    thread.start()
    print(f"[BRIDGE] {bridge_name} started for site '{site}'")
    return bridge

def safe_stop(*objs):
    for obj in objs:
        if not obj:
            continue
        stop = getattr(obj, "stop", None)
        if callable(stop):
            try:
                stop()
            except Exception as e:
                print(f"[WARN] Error stopping {obj}: {e}")


GATEWAYS = discover_gateways()
GATEWAY_SCOPES = {
    name: getattr(cls, "scope", "unknown") for name, cls in GATEWAYS.items()
}
BRIDGES = discover_bridges()
BRIDGE_SCOPES = {
    name: meta.get("scope", "unknown") for name, meta in BRIDGES.items()
}

# read the release version from the package
def _release_version() -> str:
    """Return the installed nexus-n3-core version or 'unknown'."""
    for name in ("nexus-n3-core", "nexus_n3_core"):
        try:
            return metadata.version(name)
        except metadata.PackageNotFoundError:
            continue
    return "unknown"


def _format_uptime(seconds: int) -> str:
    """Render a compact uptime label for admin and device-info payloads."""
    if seconds < 60:
        return f"{seconds}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


# return a server status based on the core components of the system
def _server_status_snapshot(server_start_time, usb_disk_manager, bridge_name, remote_bridge, ble_runtime_config):
    """Build a runtime status snapshot used by both the admin UI and device-info command."""
    uptime_seconds = int(time.monotonic() - server_start_time)
    usb_disk = {
        "present": bool(usb_disk_manager and usb_disk_manager.usb_path),
        "path": str(usb_disk_manager.usb_path) if usb_disk_manager and usb_disk_manager.usb_path else None,
    }
    bridge_status = remote_bridge.status() if bridge_name and remote_bridge else {}
    ble_backend_status = {"status": "ready", "detail": "Internal Bleak backend ready"}
    if ble_runtime_config.backend == "gateway":
        port = ble_runtime_config.gateway_serial_port
        if not port:
            ble_backend_status = {
                "status": "unavailable",
                "detail": "Gateway serial port is not configured",
            }
        else:
            port_path = Path(port).expanduser()
            if port_path.exists():
                ble_backend_status = {
                    "status": "ready",
                    "detail": f"Gateway detected on {port}",
                }
            else:
                ble_backend_status = {
                    "status": "unavailable",
                    "detail": f"Gateway not detected on {port}",
                }
    return {
        "status": "running",
        "last_error": None,
        "uptime": uptime_seconds,
        "ble_backend": ble_runtime_config.backend,
        "ble_backend_label": ble_runtime_config.backend_label,
        "ble_backend_status": ble_backend_status,
        "usb_disk": usb_disk,
        "azure_bridge": bridge_status if bridge_name == "azure_bridge" else {"enabled": False},
        "bridges": {
            bridge_name: bridge_status,
        } if bridge_name and remote_bridge else {},
    }

# another more general snapshot helper.
def _device_info_runtime_snapshot(
    *,
    site: str,
    customer_id: str | None,
    site_id: str | None,
    site_name: str | None,
    role: str,
    gateway_name: str,
    server_start_time,
    usb_disk_manager,
    bridge_name,
    remote_bridge,
    ble_runtime_config,
):
    """Build the runtime portion of the device-info payload for control-center mapping."""
    server_status = _server_status_snapshot(
        server_start_time,
        usb_disk_manager,
        bridge_name,
        remote_bridge,
        ble_runtime_config,
    )
    bridge_status = server_status["bridges"].get(bridge_name, {}) if bridge_name else {}
    neia_inventory = _fetch_neia_app_inventory()
    return {
        "customer_id": customer_id,
        "site_id": site_id or site,
        "site_name": site_name or site,
        "display_name": site,
        "role": role,
        "status": "online" if server_status["status"] == "running" else "offline",
        "gateway_name": gateway_name,
        "active_bridge": bridge_name or False,
        "iot_hub_device_id": bridge_status.get("device_id"),
        "serial_number": None,
        "device_type": role,
        "software_version": f"nexus-n3-core {_release_version()}",
        "server_status": server_status["status"],
        "uptime_seconds": server_status["uptime"],
        "uptime": _format_uptime(server_status["uptime"]),
        "remote_control_enabled": bool(bridge_status.get("remote_control_enabled", False)),
        "ble_backend": ble_runtime_config.backend,
        "ble_backend_label": ble_runtime_config.backend_label,
        "usb_disk": server_status["usb_disk"],
        "last_heartbeat_at": None,
        "neia_apps": neia_inventory["apps"],
        "neia_workflows": neia_inventory["workflows"],
    }

async def run_async_server(
    site: str,
    role: str,
    customer_id: str | None = None,
    site_id: str | None = None,
    site_name: str | None = None,
    node_id: str = None,
    mdns_hostname: str | None = None,
    admin_enabled: bool = False,
    admin_host: str = "127.0.0.1",
    admin_port: int = 9000,
    admin_display_profile: str | None = None,
    bridge_name: str | None = None,
    azure_bridge_remote_control_enabled: bool = False,
):
    """
    Run the Nexus N3 Core server in asyncio mode with the selected gateway.

    Args:
        site (str): Site name where the nexus server is deployed.
        gateway_name (str): Name of the gateway to use.
        role (str): Role of this node in the distributed system
    """

    server_start_time = time.monotonic()
    gateway = None
    server = None
    master_node = None
    worker_node = None
    ai_node = None
    #ai_node = None  # why is there two of these?
    usb_disk_manager = None
    remote_bridge = None
    ble_runtime_config = BLERuntimeConfig.from_env()

    # we only support one internal gateway now
    gateway_name = "zeromq_gateway"
    gateway_class = GATEWAYS.get(gateway_name.lower())
    if not gateway_class:
        raise ValueError(f"Unknown gateway '{gateway_name}'. Available: {list(GATEWAYS.keys())}")

    if role == "master":
        gateway = gateway_class(site) # instantiate gateway with a site
        
        usb_disk_manager = USBDiskManager(fallback_dir=_output_root())
        robot_service = _build_robot_service()
        server = Server(
            gateway,
            usb_disk_manager,
            deployment_context={
                "customer_id": customer_id,
                "site_id": site_id or site,
                "site_name": site_name or site,
            },
            robot_service=robot_service,
            ble_runtime_config=ble_runtime_config,
        )

        from nexus_n3.distributed.registry import NodeRegistry
        from nexus_n3.distributed.master_node import MasterNode

        registry = NodeRegistry()
        master_node = MasterNode(
            registry=registry,
            usb_disk_manager=usb_disk_manager,
            mdns_hostname=mdns_hostname,
        )
        master_node.system_event_bus = server.system_event_bus
        if usb_disk_manager.supports_hotdisk:
            master_node.set_after_all_streams_drained(server.finalize_usb_after_stream)
        master_node.start()
        await asyncio.sleep(0.1)
        server.start()
        server.handler.registry = registry
        if usb_disk_manager.supports_hotdisk:
            server.handler.set_stream_lifecycle_hooks(
                before_stream_start=server.prepare_usb_for_stream,
                after_stream_stop=None,
            )
        
        server.handler.set_dispatcher(lambda msg: master_node.dispatch_command(msg, message_handler=server.handler))
        print(f"[MASTER] Node registry initialized and command router started for site '{site}'.")
        
    
    elif role == "worker":
        from nexus_n3.distributed.worker_node import WorkerNode
        if not node_id:
            raise ValueError("Worker nodes must be started with a --node-id argument")
        # Worker discovers master internally in its class
        worker_node = WorkerNode(
            node_id=node_id,
            site=site,
            customer_id=customer_id,
            site_id=site_id or site,
            site_name=site_name or site,
        )
        worker_node.start()
    elif role == "ai":
        from nexus_n3.distributed.ai_compute_node import AiComputeNode
        if not node_id:
            raise ValueError("AI nodes must be started with a --node-id argument")
        compute_port = getattr(run_async_server, "compute_port", 7001)
        ai_node = AiComputeNode(node_id=node_id, compute_port=compute_port)
        ai_node.start()

    # this is the defaul standalone mode - its very similar to a master node
    else:
        usb_disk_manager = USBDiskManager(fallback_dir=_output_root())
        gateway = gateway_class(site)
        robot_service = _build_robot_service()
        server = Server(
            gateway,
            usb_disk_manager,
            deployment_context={
                "customer_id": customer_id,
                "site_id": site_id or site,
                "site_name": site_name or site,
            },
            robot_service=robot_service,
            ble_runtime_config=ble_runtime_config,
        )
        server.start()

        print(f"Server running (async) with '{gateway_name}' gateway as '{role}' mode.")

    remote_bridge = start_bridge(
        site,
        bridge_name,
        role,
        customer_id=customer_id,
        site_id=site_id or site,
        site_name=site_name or site,
        remote_control_enabled=azure_bridge_remote_control_enabled,
    )

    # why is this limited to standalone - shouldnt it be master also?
    if (
        server
        and role == "standalone"
        and bridge_name == "azure_bridge"
        and remote_bridge
        and usb_disk_manager
        and usb_disk_manager.supports_hotdisk
    ):
        server.handler.set_stream_lifecycle_hooks(
            before_stream_start=server.prepare_usb_for_stream,
            after_stream_stop=None,
        )
    if server:
        server.handler.set_device_info_provider(
            lambda: _device_info_runtime_snapshot(
                site=site,
                customer_id=customer_id,
                site_id=site_id or site,
                site_name=site_name or site,
                role=role,
                gateway_name=gateway_name,
                server_start_time=server_start_time,
                usb_disk_manager=usb_disk_manager,
                bridge_name=bridge_name,
                remote_bridge=remote_bridge,
                ble_runtime_config=ble_runtime_config,
            )
        )

    restart_event = Event()
    restart_argv = None

    if admin_enabled and role in {"standalone", "master"}:
        # i think we can rid of this also as it doesnt really help things
        if admin_display_profile:
            os.environ["NEXUS_ADMIN_DISPLAY_PROFILE"] = admin_display_profile
        else:
            os.environ.pop("NEXUS_ADMIN_DISPLAY_PROFILE", None)
        from nexus_n3.admin.app import AdminState, create_app
        import uvicorn

        def _restart(bridge_name: str | None = None):
            nonlocal restart_argv
            args = []
            skip_next = False
            for arg in sys.argv[1:]:
                if skip_next:
                    skip_next = False
                    continue
                if arg in {"--gateway", "--bridge"}:
                    skip_next = True
                    continue
                if arg == "--azure-bridge":
                    continue
                args.append(arg)
            args.extend(["--gateway", gateway_name])
            if bridge_name:
                args.extend(["--bridge", bridge_name])
            restart_argv = [sys.executable, sys.argv[0], *args]
            restart_event.set()

        def _node_status():
            nodes = []
            for node_id, meta in master_node.registry.get_nodes().items():
                node = {"node_id": node_id}
                node.update(meta)
                nodes.append(node)
            return nodes

        state = AdminState(
            project_root=Path(__file__).resolve().parent,
            role=role,
            site=site,
            gateway_name=gateway_name,
            bridge_name=bridge_name,
            available_bridges=sorted(BRIDGES.keys()),
            bridge_scopes=BRIDGE_SCOPES,
            node_status_provider=_node_status if master_node else None,
            server_status_provider=lambda: _server_status_snapshot(
                server_start_time,
                usb_disk_manager,
                bridge_name,
                remote_bridge,
                ble_runtime_config,
            ),
            restart_handler=_restart,
            usb_disk_action_handler=(
                server.admin_usb_disk_action
                if server and usb_disk_manager and usb_disk_manager.supports_hotdisk
                else None
            ),
            azure_bridge_control_handler=remote_bridge.set_remote_control_enabled if bridge_name == "azure_bridge" and remote_bridge else None,
        )
        app = create_app(state)
        # starts the server in a thread
        Thread(
            target=uvicorn.run,
            kwargs={
                "app": app,
                "host": admin_host,
                "port": admin_port,
                "log_level": "info",
            },
            daemon=True,
        ).start()
    
    do_restart = False
    try:
        while True:
            if restart_event.is_set():
                do_restart = True
                break
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        print("[SHUTDOWN] Async safe stop")

        safe_stop(
            remote_bridge,
            ai_node,
            worker_node,
            master_node,
            server,
            usb_disk_manager,
        )

        print("[SHUTDOWN] Async shutdown complete")
        if do_restart:
            if restart_argv:
                os.execv(restart_argv[0], restart_argv)
            os.execv(sys.executable, [sys.executable] + sys.argv)


def main():

    """
    Command-line entry point for the Nexus N3 Core server.
    """
    parser = argparse.ArgumentParser(description="Run Nexus N3 Core Gateway Server")
    parser.add_argument(
        "--site",
        type=str,
        help="Site name where the nexus server is deployed"
    )
    parser.add_argument(
        "--customer-id",
        type=str,
        help="Owning customer identifier stamped onto all emitted events"
    )
    parser.add_argument(
        "--site-id",
        type=str,
        help="Site or facility identifier stamped onto all emitted events"
    )
    parser.add_argument(
        "--site-name",
        type=str,
        help="Optional human-readable site or facility name"
    )
    # isnt really needed as we only support zeromq now
    parser.add_argument(
        "--gateway",
        type=str,
        default="zeromq_gateway",
        help="Local gateway type to use. Only zeromq_gateway is supported."
    )
    # allows user selection of either bleak or the nexus gateway.
    parser.add_argument(
        "--ble-backend",
        choices=["bleak", "nexus_ble_gateway"],
        default="nexus_ble_gateway",
        help="BLE backend to use. This overrides BLE_BACKEND from the startup env file."
    )
    parser.add_argument(
        "--bridge",
        type=str,
        help=f"Optional remote bridge to use. Available: {', '.join(BRIDGES.keys())}"
    )
    # we can remove this as we dont need it anymore.
    parser.add_argument(
        "--use-async",           # changed from --async
        action="store_true",
        help="Run server in asyncio mode"
    )
    parser.add_argument(
        "--role",
        choices=["standalone", "master", "worker", "ai"],
        default="standalone",
        help="Role of this node in the distributed system"
    )
    parser.add_argument(
        "--node-id",
        type=str,
        help="Node ID (required for worker nodes)"
    )
    parser.add_argument(
        "--compute-port",
        type=int,
        default=7001,
        help="Compute port for AI nodes (default: 7001)"
    )
    parser.add_argument(
        "--mdns-hostname",
        type=str,
        help="Optional mDNS hostname to advertise (e.g. nexus-n3-master)"
    )
    parser.add_argument(
        "--admin",
        action="store_true",
        help="Start the admin UI server (standalone/master only)"
    )
    parser.add_argument(
        "--admin-port",
        type=int,
        default=9000,
        help="Admin UI port (default: 9000)"
    )
    parser.add_argument(
        "--admin-host",
        type=str,
        default="0.0.0.0",
        help="Admin UI bind host (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--admin-display-profile",
        type=str,
        default=None,
        help="Admin UI display profile (example: 1920x1080)"
    )
    parser.add_argument(
        "--azure-bridge",
        action="store_true",
        help="Start the Azure IoT bridge in-process (standalone/master only)"
    )
    parser.add_argument(
        "--azure-bridge-remote-control",
        action="store_true",
        help="Start the Azure bridge with remote control enabled"
    )
    # store_true creates a boolean flag. its false by default unless the arg is present
    parser.add_argument(
        "--diagnostics",
        action="store_true",
        help="Enable low-overhead pipeline diagnostics capture"
    )
    # overrides env variable. probably should not be used here
    parser.add_argument(
        "--plugin-root",
        type=str,
        help="Installed plugin root to use instead of the default or environment value",
    )
    # same for this.
    parser.add_argument(
        "--plugin-use-system-site-packages",
        action="store_true",
        help="Allow plugin runtime .venv environments to inherit base site-packages for developer runs",
    )
    # this allows specified plugins to be built and installed on starting the server
    # useful for development environments that need to validate a plugin against the core software
    parser.add_argument(
        "--prepare-plugin-catalog-root",
        type=str,
        help="Build and install plugins from a nexus-n3-plugin-catalog tree before server startup",
    )
    parser.add_argument(
        "--prepare-dev-plugin",
        action="append",
        default=[],
        help="Specific dev plugin directory name or plugin_id to prepare; repeatable",
    )
    parser.add_argument(
        "--plugin-tooling-root",
        type=str,
        help="Path to the nexus-n3-plugin-tooling repository used for dev bundle builds",
    )
    parser.add_argument(
        "--plugin-build-root",
        type=str,
        help="Directory where temporary dev-plugin bundles should be built",
    )
    args = parser.parse_args()
    
    load_runtime_env()  # loads the source of truth environment

    if args.site:
        os.environ["AZURE_IOT_SITE"] = args.site
    if args.site_id:
        os.environ["AZURE_IOT_SITE_ID"] = args.site_id
    if args.site_name:
        os.environ["AZURE_IOT_SITE_NAME"] = args.site_name  

    pipeline_diagnostics.set_enabled(args.diagnostics)
    if args.plugin_root:
        os.environ["NEXUS_N3_PLUGIN_ROOT"] = args.plugin_root
    if args.plugin_use_system_site_packages:
        os.environ["NEXUS_N3_PLUGIN_USE_SYSTEM_SITE_PACKAGES"] = "1"

    explicit_bootstrap_requested = bool(args.prepare_plugin_catalog_root or args.prepare_dev_plugin)
    bootstrap_plugins_enabled = explicit_bootstrap_requested or _env_flag(
        "NEXUS_N3_BOOTSTRAP_PLUGINS",
        default=False,
    )
    bootstrap_plugins_root = (
        args.prepare_plugin_catalog_root
        or os.environ.get("NEXUS_N3_PLUGIN_CATALOG_ROOT")
    )
    bootstrap_plugin_list = args.prepare_dev_plugin or _env_csv_list("NEXUS_N3_BOOTSTRAP_PLUGIN_LIST")

    # this is for dev work
    if bootstrap_plugins_enabled:
        if not bootstrap_plugins_root:
            raise ValueError(
                "NEXUS_N3_BOOTSTRAP_PLUGINS is enabled but no dev plugin root is configured. "
                "Set NEXUS_N3_PLUGIN_CATALOG_ROOT or pass --prepare-plugin-catalog-root."
            )
        if not bootstrap_plugin_list and not explicit_bootstrap_requested:
            raise ValueError(
                "NEXUS_N3_BOOTSTRAP_PLUGINS is enabled but no plugin list is configured. "
                "Set NEXUS_N3_BOOTSTRAP_PLUGIN_LIST or pass --prepare-dev-plugin."
            )
        prepared = prepare_dev_plugins(
            plugin_catalog_root=bootstrap_plugins_root,
            plugin_root=args.plugin_root,
            plugin_tooling_root=args.plugin_tooling_root,
            build_root=args.plugin_build_root,
            selected_plugins=bootstrap_plugin_list,
            system_site_packages=args.plugin_use_system_site_packages,
        )
        for item in prepared:
            action = "installed" if item.installed else "reused"
            print(f"[PLUGINS] {action} {item.plugin_id} from {item.plugin_root}")
    if args.ble_backend:
        os.environ["BLE_BACKEND"] = args.ble_backend
    if args.gateway != "zeromq_gateway":
        raise ValueError("Only zeromq_gateway is supported as --gateway. Use --bridge for remote transport.")
    if args.azure_bridge and not args.bridge:
        args.bridge = "azure_bridge"

    # start the server 
    try:
        run_async_server.compute_port = args.compute_port  # the default is 7001 but why does it need it?
        asyncio.run(
            run_async_server(
                site=_runtime_site_name(args.site),
                customer_id=os.environ.get("AZURE_IOT_CUSTOMER_ID"),
                site_id=os.environ.get("AZURE_IOT_SITE_ID"),
                site_name=os.environ.get("AZURE_IOT_SITE_NAME"),
                role=args.role, # these are default anyway but should really be picked up from env
                node_id=args.node_id,
                mdns_hostname=args.mdns_hostname,
                admin_enabled=args.admin,
                admin_host=args.admin_host,
                admin_port=args.admin_port,
                admin_display_profile=args.admin_display_profile,
                bridge_name=args.bridge,
                azure_bridge_remote_control_enabled=args.azure_bridge_remote_control,
        )
    )
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Async server interrupted")

        
if __name__ == "__main__":
    main()
