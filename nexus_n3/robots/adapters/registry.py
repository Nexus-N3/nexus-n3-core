from __future__ import annotations

ROBOT_TYPES: dict[str, str] = {
    "wave_rover": "nexus_n3.robots.adapters.wave_rover:WaveRoverAdapter",
}

TRANSPORT_TYPES: dict[str, str] = {
    "uart": "nexus_n3.robots.transports.uart:UartTransport",
    "mock": "nexus_n3.robots.transports.mock:MockTransport",
}

PROTOCOL_TYPES: dict[str, str] = {
    "waveshare_uart": "nexus_n3.robots.protocols.waveshare_uart:WaveshareUartProtocol",
}