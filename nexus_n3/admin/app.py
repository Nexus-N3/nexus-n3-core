"""FastAPI admin app for Nexus N3 (local, offline UI)."""

from __future__ import annotations

import os
import shutil
import time
import zipfile
from importlib import metadata
from tempfile import NamedTemporaryFile
from pathlib import Path
from typing import Callable
from urllib.parse import urlencode, urlsplit

from fastapi import FastAPI, HTTPException, Request, Form, File, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from nexus_n3.bridge.bridge_registry import discover_bridges
from nexus_n3.core.runtime_env import load_runtime_env
from nexus_n3.gateway.gateways.gateway_registry import discover_gateways
from nexus_n3.plugins.install.installer import PluginInstallError, PluginInstaller
from nexus_n3.plugins.runtime.discovery import (
    get_installed_plugin_inventory,
    get_supported_algorithms,
    get_supported_sensors,
)


def _get_release_version() -> str:
    """Return the installed nexus-n3-core version or 'unknown'."""
    for name in ("nexus-n3-core", "nexus_n3_core"):
        try:
            return metadata.version(name)
        except metadata.PackageNotFoundError:
            continue
    return "unknown"


def _profile_to_css_class(profile: str | None) -> str:
    """Convert a profile label into a safe CSS class suffix."""
    if not profile:
        return ""
    normalized = "".join(ch.lower() if ch.isalnum() else "-" for ch in profile).strip("-")
    if not normalized:
        return ""
    return f"display-profile-{normalized}"


class AdminState:
    """Runtime state and callbacks for the admin UI."""

    def __init__(
        self,
        project_root: Path,
        role: str,
        site: str | None,
        gateway_name: str | None,
        bridge_name: str | None = None,
        available_bridges: list[str] | None = None,
        bridge_scopes: dict[str, str] | None = None,
        node_status_provider: Callable[[], list[dict]] | None = None,
        server_status_provider: Callable[[], dict] | None = None,
        restart_handler: Callable[[str | None], None] | None = None,
        usb_disk_action_handler: Callable[[str], bool] | None = None,
        azure_bridge_control_handler: Callable[[bool], None] | None = None,
    ):
        self.project_root = project_root
        self.role = role
        self.site = site
        self.gateway_name = gateway_name
        self.bridge_name = bridge_name
        self.available_bridges = available_bridges or []
        self.bridge_scopes = bridge_scopes or {}
        self.node_status_provider = node_status_provider
        self.server_status_provider = server_status_provider
        self.restart_handler = restart_handler
        self.usb_disk_action_handler = usb_disk_action_handler
        self.azure_bridge_control_handler = azure_bridge_control_handler
        self.start_time = time.monotonic()

    def uptime_seconds(self) -> int:
        """Return app uptime in seconds."""
        return int(time.monotonic() - self.start_time)

    def server_status(self) -> dict:
        """Return server status information for the dashboard."""
        if self.server_status_provider:
            return self.server_status_provider()
        return {
            "status": "unknown",
            "last_error": None,
            "uptime": self.uptime_seconds(),
            "usb_disk": {
                "present": False,
                "path": None,
            },
            "azure_bridge": {
                "enabled": False,
            },
            "bridges": {},
        }

    def node_status(self) -> list[dict]:
        """Return node status list for master mode."""
        if self.node_status_provider:
            nodes = self.node_status_provider()
            now = time.time()
            normalized = []
            for node in nodes:
                last_seen = node.get("last_seen")
                age = int(now - last_seen) if last_seen else None
                if last_seen is None:
                    state = "unknown"
                elif age <= 10:
                    state = "active"
                elif age <= 60:
                    state = "idle"
                else:
                    state = "offline"
                normalized.append({
                    **node,
                    "last_seen_age": age,
                    "state": state,
                })
            return normalized
        return []

    def can_restart(self) -> bool:
        """Return True if restart is available."""
        return self.restart_handler is not None

    def can_manage_usb_disk(self) -> bool:
        """Return True if manual USB mount/unmount actions are available."""
        return self.usb_disk_action_handler is not None

    def can_manage_azure_bridge(self) -> bool:
        """Return True if Azure bridge runtime controls are available."""
        return self.azure_bridge_control_handler is not None



def _safe_path(base_dir: Path, requested: str) -> Path:
    """Resolve a requested path and ensure it is within base_dir."""
    resolved = (base_dir / requested).resolve()
    if not str(resolved).startswith(str(base_dir.resolve())):
        raise HTTPException(status_code=400, detail="Invalid path")
    return resolved


def _safe_admin_redirect_target(next_path: str | None) -> str:
    """Normalize a post action redirect target back into the admin app."""
    if not next_path:
        return "/"
    parts = urlsplit(next_path)
    if parts.scheme or parts.netloc:
        return "/"
    target = parts.path or "/"
    if not target.startswith("/"):
        target = f"/{target}"
    if parts.query:
        return f"{target}?{parts.query}"
    return target


def _append_query_params(base_url: str, **params: str) -> str:
    """Append query params to a local URL."""
    parts = urlsplit(base_url)
    existing = []
    if parts.query:
        existing.append(parts.query)
    extra = urlencode({key: value for key, value in params.items() if value is not None})
    if extra:
        existing.append(extra)
    query = "&".join(item for item in existing if item)
    return f"{parts.path}{'?' + query if query else ''}"


def create_app(state: AdminState) -> FastAPI:
    """Create and configure the FastAPI admin application."""
    load_runtime_env()
    app = FastAPI(title="Nexus N3 Admin", docs_url=None, redoc_url=None)
    release_version = _get_release_version()

    admin_root = state.project_root / "nexus_n3" / "admin"
    templates = Jinja2Templates(directory=str(admin_root / "templates"))
    display_profile = os.getenv("NEXUS_ADMIN_DISPLAY_PROFILE", "").strip()
    templates.env.globals["display_profile"] = display_profile
    templates.env.globals["display_profile_class"] = _profile_to_css_class(display_profile)
    static_dir = admin_root / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    docs_build = Path(__file__).resolve().parent / "docs" / "html"
    if not docs_build.exists():
        docs_build = state.project_root / "docs" / "_build" / "html"
    if docs_build.exists():
        app.mount("/docs-static", StaticFiles(directory=str(docs_build), html=True), name="docs")

    logs_dir = state.project_root / "nexus_n3_logs"
    outputs_dir = state.project_root / "nexus_n3_outputs"

    def _render_template(request: Request, template_name: str, context: dict | None = None) -> HTMLResponse:
        """Render templates with common header context."""
        payload = {
            "request": request,
            "role": state.role,
            "site": state.site,
            "release_version": release_version,
        }
        if context:
            payload.update(context)
        return templates.TemplateResponse(request, template_name, payload)

    def _resolve_outputs_dir() -> Path:
        status = state.server_status()
        usb_disk = status.get("usb_disk", {}) if isinstance(status, dict) else {}
        usb_path = usb_disk.get("path") if isinstance(usb_disk, dict) else None
        if usb_disk.get("present") and usb_path:
            candidate = Path(usb_path)
            if candidate.exists() and candidate.is_dir():
                return candidate
        return outputs_dir

    def _resolve_usb_outputs_dir() -> Path | None:
        status = state.server_status()
        usb_disk = status.get("usb_disk", {}) if isinstance(status, dict) else {}
        usb_path = usb_disk.get("path") if isinstance(usb_disk, dict) else None
        if usb_disk.get("present") and usb_path:
            candidate = Path(usb_path)
            if candidate.exists() and candidate.is_dir():
                return candidate
        return None

    def _copy_outputs_to_usb(source: Path, destination: Path) -> dict:
        copied = 0
        skipped = 0
        errors = 0

        for root, _, files in os.walk(source):
            rel_path = Path(root).relative_to(source)
            dest_dir = destination / rel_path
            dest_dir.mkdir(parents=True, exist_ok=True)
            for filename in files:
                src_file = Path(root) / filename
                dst_file = dest_dir / filename
                try:
                    if dst_file.exists():
                        src_stat = src_file.stat()
                        dst_stat = dst_file.stat()
                        if (
                            src_stat.st_size == dst_stat.st_size
                            and int(src_stat.st_mtime) <= int(dst_stat.st_mtime)
                        ):
                            skipped += 1
                            continue
                    shutil.copy2(src_file, dst_file)
                    copied += 1
                except Exception:
                    errors += 1
        return {"copied": copied, "skipped": skipped, "errors": errors}

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request, usb_action: str | None = None):
        """Render the dashboard page."""
        return _render_template(
            request,
            "index.html",
            {
                "gateway": state.gateway_name,
                "bridge": state.bridge_name,
                "bridges": state.available_bridges,
                "bridge_scopes": state.bridge_scopes,
                "bridge_scope": state.bridge_scopes.get(state.bridge_name, "disabled") if state.bridge_name else "disabled",
                "uptime": state.uptime_seconds(),
                "server_status": state.server_status(),
                "nodes": state.node_status(),
                "can_restart": state.can_restart(),
                "can_manage_usb_disk": state.can_manage_usb_disk(),
                "can_manage_azure_bridge": state.can_manage_azure_bridge(),
                "usb_action_status": usb_action,
            },
        )

    @app.get("/logs", response_class=HTMLResponse)
    def list_logs(request: Request):
        """List available log files."""
        files = []
        if logs_dir.exists():
            files = sorted([p.name for p in logs_dir.iterdir() if p.is_file()])
        return _render_template(request, "logs.html", {"files": files})

    @app.get("/logs/{name}", response_class=HTMLResponse)
    def view_log(request: Request, name: str, tail: int = 200):
        """Tail the last N lines of a log file."""
        path = _safe_path(logs_dir, name)
        if not path.exists() or not path.is_file():
            raise HTTPException(status_code=404, detail="Log not found")
        lines = []
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            lines = handle.readlines()[-tail:]
        return _render_template(
            request,
            "log_view.html",
            {
                "name": name,
                "tail": tail,
                "content": "".join(lines),
            },
        )

    @app.get("/outputs", response_class=HTMLResponse)
    def list_outputs(
        request: Request,
        path: str = "",
        transfer: str | None = None,
        copied: int = 0,
        skipped: int = 0,
        errors: int = 0,
        usb_action: str | None = None,
    ):
        """Browse the outputs directory."""
        status = state.server_status()
        usb_disk = status.get("usb_disk", {}) if isinstance(status, dict) else {}
        if usb_disk.get("present"):
            outputs_source = "Reading from disk"
        else:
            outputs_source = "Reading from local directory"
        base_dir = _resolve_outputs_dir()
        target = _safe_path(base_dir, path) if path else base_dir
        if not target.exists() or not target.is_dir():
            raise HTTPException(status_code=404, detail="Path not found")
        entries = sorted(target.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
        listing = []
        for entry in entries:
            rel = entry.relative_to(base_dir)
            listing.append({
                "name": entry.name,
                "path": str(rel),
                "is_dir": entry.is_dir(),
                "size": entry.stat().st_size,
                "mtime": int(entry.stat().st_mtime),
            })
        return _render_template(
            request,
            "outputs.html",
            {
                "path": str(path),
                "entries": listing,
                "outputs_source": outputs_source,
                "server_status": status,
                "transfer_status": transfer,
                "transfer_copied": copied,
                "transfer_skipped": skipped,
                "transfer_errors": errors,
                "usb_action_status": usb_action,
                "can_manage_usb_disk": state.can_manage_usb_disk(),
            },
        )

    @app.get("/outputs/download", response_class=FileResponse)
    def download_output(path: str):
        """Download a file from the outputs directory."""
        base_dir = _resolve_outputs_dir()
        target = _safe_path(base_dir, path)
        if not target.exists() or not target.is_file():
            raise HTTPException(status_code=404, detail="File not found")
        return FileResponse(str(target), filename=target.name)

    @app.get("/outputs/download-zip", response_class=FileResponse)
    def download_outputs_zip(path: str = ""):
        """Download a directory (or root) from outputs as a zip archive."""
        base_dir = _resolve_outputs_dir()
        target = _safe_path(base_dir, path) if path else base_dir
        if not target.exists() or not target.is_dir():
            raise HTTPException(status_code=404, detail="Path not found")

        with NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            zip_path = Path(tmp.name)

        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
            for file_path in target.rglob("*"):
                if file_path.is_file():
                    arcname = file_path.relative_to(target)
                    zipf.write(file_path, arcname)

        filename = f"{target.name or 'outputs'}.zip"
        return FileResponse(str(zip_path), filename=filename)

    @app.post("/outputs/delete")
    def delete_output(path: str = Form(...)):
        """Delete a file from the outputs directory."""
        base_dir = outputs_dir
        target = _safe_path(base_dir, path)
        if not target.exists() or not target.is_file():
            raise HTTPException(status_code=404, detail="File not found")
        target.unlink()
        return RedirectResponse(url="/outputs", status_code=303)

    @app.post("/outputs/transfer-local")
    def transfer_local_outputs():
        """Copy local outputs into the USB outputs directory."""
        usb_dir = _resolve_usb_outputs_dir()
        if usb_dir is None:
            return RedirectResponse(url="/outputs?transfer=no_usb", status_code=303)

        if not outputs_dir.exists() or not outputs_dir.is_dir():
            return RedirectResponse(url="/outputs?transfer=missing_local", status_code=303)

        if outputs_dir.resolve() == usb_dir.resolve():
            return RedirectResponse(url="/outputs?transfer=already_on_usb", status_code=303)

        results = _copy_outputs_to_usb(outputs_dir, usb_dir)
        return RedirectResponse(
            url=(
                "/outputs?transfer=success"
                f"&copied={results['copied']}"
                f"&skipped={results['skipped']}"
                f"&errors={results['errors']}"
            ),
            status_code=303,
        )

    def _run_usb_action(action: str, redirect_path: str) -> RedirectResponse:
        if not state.usb_disk_action_handler:
            raise HTTPException(status_code=400, detail="USB disk actions unavailable")
        if action not in {"mount", "unmount"}:
            raise HTTPException(status_code=400, detail="Unknown USB action")
        success = state.usb_disk_action_handler(action)
        result = f"{action}_{'success' if success else 'failed'}"
        return RedirectResponse(url=f"{redirect_path}{'&' if '?' in redirect_path else '?'}usb_action={result}", status_code=303)

    @app.post("/outputs/usb")
    def manage_usb_outputs(action: str = Form(...)):
        """Manually mount or unmount the USB disk for browsing/removal."""
        return _run_usb_action(action, "/outputs")

    @app.get("/docs", response_class=HTMLResponse)
    def docs_index(request: Request):
        """Render the documentation page wrapper."""
        if not docs_build.exists():
            return _render_template(request, "docs.html", {"available": False})
        return _render_template(request, "docs.html", {"available": True})

    @app.get("/capabilities", response_class=HTMLResponse)
    def capabilities(request: Request):
        """Render supported sensors, algorithms, and gateways."""
        raw_sensors = get_supported_sensors()
        raw_algorithms = get_supported_algorithms()
        inventory = get_installed_plugin_inventory()
        sensor_inventory = {
            str(item.get("sensor_name") or "").strip().lower(): item
            for item in inventory.get("sensor_plugins", [])
        }
        algorithm_inventory = {
            str(item.get("algorithm_name") or "").strip().lower(): item
            for item in inventory.get("algorithm_plugins", [])
        }
        sensors = []
        for item in raw_sensors:
            runtime_name = str(item.get("name") or "")
            installed = sensor_inventory.get(runtime_name.strip().lower(), {})
            sensors.append(
                {
                    "runtime_name": runtime_name,
                    "plugin_display_name": installed.get("display_name") or runtime_name,
                    "plugin_id": installed.get("plugin_id"),
                    "version": installed.get("version"),
                    "locations": list(item.get("locations", []) or []),
                    "computations": list(item.get("computations", []) or []),
                }
            )
        algorithms = []
        for name in raw_algorithms:
            runtime_name = str(name or "")
            installed = algorithm_inventory.get(runtime_name.strip().lower(), {})
            algorithms.append(
                {
                    "runtime_name": runtime_name,
                    "plugin_display_name": installed.get("display_name") or runtime_name,
                    "plugin_id": installed.get("plugin_id"),
                    "version": installed.get("version"),
                }
            )
        gateways = []
        for key, cls in discover_gateways().items():
            gateways.append({
                "name": key,
                "scope": getattr(cls, "scope", "unknown"),
            })
        gateways = sorted(gateways, key=lambda g: g["name"])
        bridges = []
        for key, meta in discover_bridges().items():
            bridges.append({
                "name": key,
                "scope": meta.get("scope", "unknown"),
            })
        bridges = sorted(bridges, key=lambda g: g["name"])
        return _render_template(
            request,
            "capabilities.html",
            {
                "sensors": sensors,
                "algorithms": algorithms,
                "gateways": gateways,
                "bridges": bridges,
            },
        )

    @app.post("/server/restart")
    def restart_server():
        """Trigger a restart via the registered handler."""
        if not state.restart_handler:
            raise HTTPException(status_code=400, detail="Restart unavailable")
        state.restart_handler(state.bridge_name)
        return RedirectResponse(url="/", status_code=303)

    @app.post("/server/usb")
    def manage_usb_from_dashboard(action: str = Form(...)):
        """Manually mount or unmount the USB disk from the dashboard."""
        return _run_usb_action(action, "/")

    @app.post("/server/azure-bridge/control")
    def manage_azure_bridge_from_dashboard(action: str = Form(...)):
        """Enable or disable Azure bridge remote control policy."""
        if not state.azure_bridge_control_handler:
            raise HTTPException(status_code=400, detail="Azure bridge controls unavailable")
        if action not in {"enable", "disable"}:
            raise HTTPException(status_code=400, detail="Unknown Azure bridge action")
        state.azure_bridge_control_handler(action == "enable")
        return RedirectResponse(url="/", status_code=303)

    @app.post("/server/switch-bridge")
    def switch_bridge(bridge: str = Form(...)):
        """Switch the active remote bridge and restart the server."""
        if not state.restart_handler:
            raise HTTPException(status_code=400, detail="Restart unavailable")
        normalized = None if bridge == "__none__" else bridge
        if normalized is not None and normalized not in state.available_bridges:
            raise HTTPException(status_code=400, detail="Unknown bridge")
        state.restart_handler(normalized)
        return RedirectResponse(url="/", status_code=303)

    @app.post("/plugins/install")
    async def install_plugin_bundle(
        next_path: str = Form("/"),
        bundle: UploadFile = File(...),
    ):
        """Upload and install a built .rsnxplugin bundle into the configured plugin root."""
        redirect_target = _safe_admin_redirect_target(next_path)
        filename = (bundle.filename or "").strip()
        if not filename.lower().endswith(".rsnxplugin"):
            return RedirectResponse(
                url=_append_query_params(
                    redirect_target,
                    plugin_install="error",
                    plugin_install_message="Only .rsnxplugin files are supported.",
                ),
                status_code=303,
            )

        suffix = Path(filename).suffix or ".rsnxplugin"
        temp_path: Path | None = None
        try:
            with NamedTemporaryFile(suffix=suffix, delete=False) as handle:
                temp_path = Path(handle.name)
                while True:
                    chunk = await bundle.read(1024 * 1024)
                    if not chunk:
                        break
                    handle.write(chunk)

            installer = PluginInstaller()
            result = installer.install_bundle(temp_path)
            return RedirectResponse(
                url=_append_query_params(
                    redirect_target,
                    plugin_install="success",
                    plugin_install_message=f"Installed {result.plugin_id} {result.version}.",
                ),
                status_code=303,
            )
        except PluginInstallError as exc:
            return RedirectResponse(
                url=_append_query_params(
                    redirect_target,
                    plugin_install="error",
                    plugin_install_message=str(exc),
                ),
                status_code=303,
            )
        finally:
            await bundle.close()
            if temp_path is not None and temp_path.exists():
                temp_path.unlink(missing_ok=True)

    return app
