"""Typed errors for the SensorManager Wi-Fi adapter."""


class WifiError(RuntimeError):
    """Base class for Wi-Fi adapter failures."""


class WifiNotInitialized(WifiError):
    """Raised when an operation requires an initialized adapter."""


class WifiShuttingDown(WifiError):
    """Raised when new work is requested after shutdown started."""


class WifiBackendUnavailable(WifiError):
    """Raised when the selected platform backend is unavailable."""


class WifiSensorDriverUnavailable(WifiError):
    """Raised when no sensor-specific Wi-Fi driver can handle a request."""


class WifiDeviceNotDiscovered(WifiError):
    """Raised when a transport is requested for an unknown identity."""


class WifiDiscoveryResultInvalid(WifiError):
    """Raised when a sensor driver returns an invalid discovery result."""


class WifiConnectionFailed(WifiError):
    """Raised when a sensor-specific connection operation fails."""
