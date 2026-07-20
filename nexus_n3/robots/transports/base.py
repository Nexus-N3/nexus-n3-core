from __future__ import annotations

from abc import ABC, abstractmethod


# base defines what methods are required to be implemented by any transport type
# transports connect and close, send and recieve.
# possible additions could be status, alive, etc.

class Transport(ABC):
    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def send(self, payload: bytes) -> None:
        raise NotImplementedError

    @abstractmethod
    def recv(self) -> bytes:
        raise NotImplementedError