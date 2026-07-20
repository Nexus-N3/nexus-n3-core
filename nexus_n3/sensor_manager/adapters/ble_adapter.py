"""BLE adapter implementation using Bleak."""

from bleak import BleakScanner, BleakClient
import asyncio
import platform
from nexus_n3.sensor_manager.utils import utils as utils
from nexus_n3.sensor_manager.utils import utils as utils
from nexus_n3.logger.logger import get_module_logger
from nexus_n3.sensor_manager.types.connections import ConnectionStatus
from nexus_n3.core.pipeline_diagnostics import pipeline_diagnostics

logger = get_module_logger("BLE Adapter")


class BLEAdapter:
    """
    Provides an interface for BLE operations using Bleak.

    Responsibilities
    ----------------
    - Discover BLE devices by name.
    - Create BleakClient instances for connecting to devices.
    - Execute BLE operations safely with error handling.
    - Set up GATT notifications and read/write characteristics.

    Adapter interface (generic, shared across transports)
    ----------------------------------------------------
    - connect / disconnect / is_connected (per-client)
    - read / write (transport operations)
    - set_notify_callback (optional capability)
    """
    adapter_type = "BLE"

    @staticmethod
    async def connect(ble_device):
        """Connect a BLE device and return True/False."""
        return await BLEAdapter.execute(ble_device.connect)

    @staticmethod
    async def disconnect(ble_device):
        """Disconnect a BLE device and return True/False."""
        result = await BLEAdapter.execute(ble_device.disconnect)
        if result is None:
            return not bool(getattr(ble_device, "is_connected", False))
        return result

    @staticmethod
    def create_transport_client(address: str, loop=None, disconnected_callback=None):
        """Create a transport client for the given BLE address."""
        return BLEAdapter.create_ble_client(address, loop=loop, disconnected_callback=disconnected_callback)

    @staticmethod
    async def connect_to_device(device, adapter, timeout: float = 10):
        """
        Connect a single BLE device using the given adapter.

        Args:
            device: Pre-instantiated sensor object with transport client and name.
            adapter: BLEAdapter instance.
            timeout: Connection timeout in seconds.

        Returns:
            bool: True if device connected successfully, False otherwise.
        """
        logger = get_module_logger("Sensor Connect")
        msg = f"Connecting to device {device.name} (addr={getattr(device, 'address', None)})"
        print(msg)
        logger.info(msg)
        try:
            connected = await asyncio.wait_for(
                adapter.connect(device.transport_client),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            timeout_msg = (
                f"Connect timeout for {device.name} (addr={getattr(device, 'address', None)})"
            )
            print(timeout_msg)
            logger.warning(timeout_msg)
            return False

        if connected is None:
            connected = bool(getattr(device.transport_client, "is_connected", False))

        if connected:
            device.set_connection_status(ConnectionStatus.CONNECTED)
            ok_msg = f"Connected to {device.name} (addr={getattr(device, 'address', None)})"
            print(ok_msg)
            logger.info(ok_msg)

        return connected
        
    @staticmethod
    async def connect_all(devices, adapter, timeout: float = 10):
        """
        Connect all devices in the given list using the adapter.

        On Linux, connections are made sequentially because concurrent connections
        often fail. The BLE cache is cleared before connecting.

        Args:
            devices: List of sensor objects to connect.
            adapter: BLEAdapter instance.
            timeout: Connection timeout per device.

        Returns:
            bool: True if all devices were connected successfully, False otherwise.
        """
        logger = get_module_logger("Sensor Connect")
        msg = f"Connecting to {len(devices)} device(s)"
        print(msg)
        logger.info(msg)
        result = []
        if platform.system() == "Linux":
            utils.remove_devices(devices)
            for d in devices:
                result.append(await BLEAdapter.connect_to_device(d, adapter, timeout=timeout))
        else:
            for d in devices:
                result.append(await BLEAdapter.connect_to_device(d, adapter, timeout=timeout))

        return all(result)

    @staticmethod
    async def test_discover_devices(timeout: float = 5.0):
        """
        Test method for discovery.

        Returns all devices discovered within the timeout.

        Parameters
        ----------
        timeout : float
            Seconds to scan for devices (default 5.0)

        Returns
        -------
        list[tuple]
            Each tuple contains (device, advertisement_data)
        """
        devices = await BleakScanner.discover(return_adv=True)
        return devices

    @staticmethod
    async def discover_devices(names: list[str], timeout: float = 5.0):
        """
        Discover devices and match by local names.

        Currently returns all discovered devices without filtering.

        Parameters
        ----------
        names : list[str]
            List of device names to match.
        timeout : float
            Seconds to scan for devices (default 5.0)

        Returns
        -------
        list[tuple]
            Each tuple contains (device, advertisement_data)
        """
        devices = await BleakScanner.discover(return_adv=True)
        return devices

    @staticmethod
    async def execute(function, *args, **kwargs):
        """
        Execute any BLE-related async function safely.

        Parameters
        ----------
        function : callable
            Async function to execute.
        *args : tuple
            Positional arguments to pass to the function.
        **kwargs : dict
            Keyword arguments to pass to the function.

        Returns
        -------
        Any or None
            Result of the function if successful, otherwise None.

        Notes
        -----
        Logs any exception encountered.
        """
        try:
            result = await function(*args, **kwargs)
            return result
        except Exception as e:
            print(f"Error during BLE operation: {e}")
            logger.error(f"Error during BLE operation: {e}")

    @staticmethod
    async def set_notify_callback(ble_device, uuid, callback_func):
        """
        Set a notification callback for a given BLE characteristic.
        """
        def wrapped_callback(sender, data):
            address = getattr(ble_device, "address", None)
            pipeline_diagnostics.mark_first_ble_notify(address)
            pipeline_diagnostics.increment(
                address,
                "ble_notify_count",
                1,
            )
            try:
                result = callback_func(sender, data)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
                return result
            except Exception as exc:
                pipeline_diagnostics.increment(address, "ble_notify_callback_error_count", 1)
                pipeline_diagnostics.record_event(
                    "ble_notify_callback_error",
                    address=address,
                    uuid=str(uuid),
                    error=f"{type(exc).__name__}: {exc}",
                )
                raise

        try:
            result = await BLEAdapter.execute(
                ble_device.start_notify, uuid, wrapped_callback
            )
            return result
        except Exception as e:
            print(f"Error during BLE Set Notify Callback: {e}")
            logger.error(f"Error during BLE Set Notify Callback: {e}")

    @staticmethod
    async def write(ble_device, uuid, char):
        """
        Write a GATT characteristic.

        Parameters
        ----------
        ble_device : BleakClient
            BleakClient instance.
        uuid : str
            GATT characteristic UUID.
        char : bytes
            Data to write.

        Returns
        -------
        Any or None
            Result of write_gatt_char call or None if error.
        """
        try:
            result = await BLEAdapter.execute(
                ble_device.write_gatt_char, uuid, char, response=True
            )
            return result
        except Exception as e:
            print(f"Error during BLE Write: {e}")
            logger.error(f"Error during BLE Write: {e}")

    @staticmethod
    async def read(ble_device, uuid):
        """
        Read a GATT characteristic.

        Parameters
        ----------
        ble_device : BleakClient
            BleakClient instance.
        uuid : str
            GATT characteristic UUID.

        Returns
        -------
        Any or None
            Characteristic value or None if error.
        """
        try:
            result = await BLEAdapter.execute(
                ble_device.read_gatt_char, uuid, response=True
            )
            return result
        except Exception as e:
            print(f"Error during BLE Read: {e}")
            logger.error(f"Error during BLE Read: {e}")

    @staticmethod
    def create_ble_client(address: str, loop, disconnected_callback=None):
        """
        Create a BleakClient instance for a given device address and event loop.

        Parameters
        ----------
        address : str
            BLE device address.
        loop : asyncio.AbstractEventLoop
            Event loop for the client.

        Returns
        -------
        BleakClient
            BleakClient instance.
        """
        return BleakClient(address, loop=loop, disconnected_callback=disconnected_callback)
