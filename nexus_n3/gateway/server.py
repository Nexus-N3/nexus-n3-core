"""Gateway server wrapper for routing commands and events."""

import subprocess
import threading
import time
from pathlib import Path

from nexus_n3.gateway.messaging.message_handler import MessageHandler
from nexus_n3.gateway.event_bus.system_event_bus import SystemEventBus
from nexus_n3.gateway.messaging import message_types as mt
from nexus_n3.logger.logger import get_module_logger
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig

logger = get_module_logger("Gateway Server")

# Servers are created only for standalone and master nodes.
class Server:
    """Bridge between gateways, the message handler, and the event bus."""

    # these are to manage the shared disk that is normally attached to a master or standalone
    # node 
    HOTPLUG_SCRIPT = Path("/usr/local/bin/nexusn3-hotplug.sh")
    SAFE_UNPLUG_SCRIPT = Path("/usr/local/bin/nexusn3-usb-safe-unplug.sh")

    def __init__(
        self,
        gateway,
        usb_disk_manager=None,
        deployment_context=None,  # includes customer id and site id and site name 
        robot_service=None,
        ble_runtime_config: BLERuntimeConfig | None = None,
    ):
        """
        Initialize the server wrapper.

        Args:
            gateway: Gateway implementation instance.
            usb_disk_manager: Optional USB disk manager for path updates.
            deployment_context: Optional customer/site context to stamp on events.
        """
        self.gateway = gateway
        self.ble_runtime_config = ble_runtime_config or BLERuntimeConfig.from_env()
        self.usb_disk_manager = usb_disk_manager
        self._usb_hotdisk_enabled = bool(
            usb_disk_manager and getattr(usb_disk_manager, "supports_hotdisk", False)
        )

        # set up the event bus with a deployment context to enrich event payloads
        self.system_event_bus = SystemEventBus(deployment_context=deployment_context)
        self.system_event_bus.subscribe(self.gateway.publish_event)
        # Handles incoming client messages
        self.handler = MessageHandler(
            self.gateway.site,
            self.system_event_bus,
            ble_runtime_config=self.ble_runtime_config,
        )

        # if a disk manager is provided then we are in standalone or master mode
        if self.usb_disk_manager:
            self.usb_disk_manager.register_callback("inserted", self.usb_disk_inserted)
            self.usb_disk_manager.register_callback("removed", self.usb_disk_removed)
            if self._usb_hotdisk_enabled:
                self.handler.set_stream_lifecycle_hooks(
                    before_stream_start=self.prepare_usb_for_stream,
                    after_stream_stop=self.finalize_usb_after_stream,
                )
                self.handler.set_usb_handlers(
                    mount_handler=self.mount_usb_disk,
                    unmount_handler=self.safe_unmount_usb_disk,
                    status_provider=self.usb_status_payload,
                )

        # robot service if this node is acting as a robot.
        # the robot service is a means to control the robot itself.
        # a node can be mounted on a robot and not control it if desired.
        # in which case there is not need for a robot service
        self.robot_service = robot_service
        self.handler.set_robot_service(self.robot_service)
        self._robot_loop_stop = threading.Event()
        self._robot_loop_thread = None

    # runs scripts associated to usb (mainly for the disk)
    def _run_usb_script(self, script_path: Path, *args: str) -> bool:
        """Run a privileged USB helper script and return True on success."""
        if not self._usb_hotdisk_enabled:
            logger.info("USB hot-disk helper skipped: feature disabled on this host")
            return False
        try:
            completed = subprocess.run(
                ["sudo", "-n", str(script_path), *args],
                check=True,
                capture_output=True,
                text=True,
            )
            stdout = completed.stdout.strip()
            if stdout:
                logger.info(stdout)
            return True
        except FileNotFoundError as exc:
            logger.error(f"USB helper execution failed: {exc}")
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            detail = stderr or stdout or str(exc)
            logger.error(f"USB helper {script_path.name} failed: {detail}")
        return False

    def _sync_core_file_path(self):
        """Refresh USB state and update the core storage path immediately."""
        active_path = self.usb_disk_manager.refresh()
        if self.handler.si:
            self.handler.si.set_file_path(str(active_path))

    # returns a disk status
    def usb_status_payload(self, *, action: str, ok: bool, error: str | None = None) -> dict:
        """Build a normalized USB status payload for gateway events."""
        usb_path = str(self.usb_disk_manager.usb_path) if self.usb_disk_manager.usb_path else None
        return {
            "ok": ok,
            "action": action,
            "present": bool(self.usb_disk_manager.usb_path),
            "path": usb_path,
            "error": error,
        }

    # emits the status on the event bus 
    def emit_usb_status(self, *, action: str, ok: bool, error: str | None = None):
        """Broadcast the current USB status to clients."""
        self.system_event_bus.emit({
            "type": mt.EVT_USB_STATUS,
            "payload": self.usb_status_payload(action=action, ok=ok, error=error),
        })

    def prepare_usb_for_stream(self):
        """Remount the USB disk before starting a local stream when possible."""
        if not self._usb_hotdisk_enabled:
            return
        if self._run_usb_script(self.HOTPLUG_SCRIPT, "add"):
            self._sync_core_file_path()

    def finalize_usb_after_stream(self):
        """Safely unmount the USB disk after a full local stop completes."""
        if not self._usb_hotdisk_enabled:
            return
        if self._run_usb_script(self.SAFE_UNPLUG_SCRIPT):
            self._sync_core_file_path()

    def mount_usb_disk(self) -> bool:
        """Manually mount/remount the USB disk and emit status."""
        if not self._usb_hotdisk_enabled:
            self.emit_usb_status(action="mount", ok=False, error="USB mount unsupported on this host")
            return False
        if self._run_usb_script(self.HOTPLUG_SCRIPT, "add"):
            self._sync_core_file_path()
            ok = bool(self.usb_disk_manager.usb_path)
            self.emit_usb_status(action="mount", ok=ok, error=None if ok else "USB mount did not become active")
            return ok
        self.emit_usb_status(action="mount", ok=False, error="USB mount command failed")
        return False

    def safe_unmount_usb_disk(self) -> bool:
        """Safely unmount the USB disk and emit status."""
        if not self._usb_hotdisk_enabled:
            self.emit_usb_status(action="unmount", ok=False, error="USB unmount unsupported on this host")
            return False
        if self.handler.si and self.handler.si.has_active_streams():
            self.emit_usb_status(action="unmount", ok=False, error="Cannot unmount while streaming is active")
            return False
        if self._run_usb_script(self.SAFE_UNPLUG_SCRIPT):
            self._sync_core_file_path()
            ok = self.usb_disk_manager.usb_path is None
            self.emit_usb_status(action="unmount", ok=ok, error=None if ok else "USB still mounted after unmount")
            return ok
        self.emit_usb_status(action="unmount", ok=False, error="USB unmount command failed")
        return False

    def admin_usb_disk_action(self, action: str) -> bool:
        """Handle manual USB mount/unmount requests from the admin UI."""
        if action == "mount":
            return self.mount_usb_disk()
        if action == "unmount":
            return self.safe_unmount_usb_disk()
        logger.error(f"Unknown admin USB action: {action}")
        return False

    def usb_disk_removed(self):
        """Handle USB removal by updating file path for local/core."""
        print("USB disk removed")
        logger.info("USB disk removed")
        self.emit_usb_status(action="removed", ok=True)
        self.handler.handle({
            "type": mt.CMD_UPDATE_FILE_PATH,
           "payload": {"file_path": str(self.usb_disk_manager.local_path)}
        })
    
    def usb_disk_inserted(self, path):
        """Handle USB insertion and broadcast file path updates."""
        # path = 
        print(f"USB disk inserted at {path}")
        logger.info(f"USB disk inserted at {path}")
        self.emit_usb_status(action="inserted", ok=bool(path))
        self.handler.handle({
            "type": mt.CMD_UPDATE_FILE_PATH,
            "payload": {
                            "file_path": str(path) if path else None,
                            "network_path": str(self.usb_disk_manager.network_path) if self.usb_disk_manager.network_path else None,
                        }
        })

    def start(self):
        """Start the gateway and perform initial system setup."""
        logger.info("Starting the gateway")
        self.gateway.start(self.handler.handle)

        self.handler.handle({
                "type": mt.CMD_SYSTEM_SETUP,
                "payload": {
                                "file_path": str(self.usb_disk_manager.local_path),    
                                "network_path": str(self.usb_disk_manager.network_path) if self.usb_disk_manager.network_path else None, 
                            
                            }
            })
        
        # start the robot service
        if self.robot_service:
            self.robot_service.start()
            self._start_robot_loop()

        self.emit_usb_status(action="status", ok=True)

    def _start_robot_loop(self):
        """Run robot safety ticks and feedback polling in the background."""
        if self._robot_loop_thread and self._robot_loop_thread.is_alive():
            return
        self._robot_loop_stop.clear()
        self._robot_loop_thread = threading.Thread(
            target=self._robot_loop,
            daemon=True,
            name="nexus-n3-robot-loop",
        )
        self._robot_loop_thread.start()

    def _robot_loop(self):
        while not self._robot_loop_stop.is_set():
            try:
                self.robot_service.tick()
                feedback = self.robot_service.poll_feedback()
                if feedback:
                    self.system_event_bus.emit({
                        "type": mt.EVT_ROBOT_STATUS,
                        "payload": feedback,
                    })
            except Exception as exc:
                logger.exception(f"Robot service loop failed: {exc}")
            time.sleep(0.05)

    def stop(self):
        """Stop the gateway and release resources."""
        self._robot_loop_stop.set()
        if not self.gateway:
            return
        
        # stop the gateway.
        try:
            self.gateway.stop()
        except Exception as exc:
            print(f"[WARN] Gateway stop failed: {exc}")

        # stop the robot service
        if self.robot_service:
            try:
                self.robot_service.shutdown()
            except Exception as exc:
                print(f"[WARN] Robot service stop failed: {exc}")

        # decide if we should unmount the disk here also?
        # should any other thread be cleaned up here? plugin hosts / runtimes?
