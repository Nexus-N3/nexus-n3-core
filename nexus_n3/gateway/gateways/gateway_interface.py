"""Abstract interface for gateway transports."""

from abc import ABC, abstractmethod

class GatewayInterface(ABC):
    """
    Contract for gateway implementations.

    Concrete gateways must implement start/stop and publish operations so the
    server can be transport-agnostic.
    """
    scope = "unknown"

    @abstractmethod
    def start(self, on_message):
        """
        Start receiving messages from clients.

        This method should begin the gateway’s message loop and call `on_message`
        for each received message.

        Args:
            on_message: Callback to handle incoming messages.
        """
        pass

    @abstractmethod
    def publish_event(self, event: dict):
        """
        Publish a system event to connected clients.

        Args:
            event: Event dictionary containing at least a 'type' key and payload.
        """
        pass

    @abstractmethod
    def publish_command(self, command: dict):
        """
        Send a command to connected clients.

        This provides a client-facing API for triggering client actions.

        Args:
            command: Command dictionary containing at least a 'type' key and payload.
        """
        pass

    @abstractmethod
    def stop(self):
        """
        Stop the gateway and clean up any resources.

        Should terminate message loops and release any network or hardware resources.
        """
        pass
