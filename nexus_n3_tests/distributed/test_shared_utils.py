from unittest.mock import Mock

from nexus_n3.distributed import shared_utils


def test_get_local_ip_prefers_configured_control_address(monkeypatch):
    monkeypatch.setenv("NEXUS_N3_CONTROL_ADDRESS", " 192.168.60.2 ")
    socket_factory = Mock()
    monkeypatch.setattr(shared_utils.socket, "socket", socket_factory)

    assert shared_utils.get_local_ip() == "192.168.60.2"
    socket_factory.assert_not_called()


def test_get_local_ip_falls_back_to_routing_when_control_address_is_unset(monkeypatch):
    monkeypatch.delenv("NEXUS_N3_CONTROL_ADDRESS", raising=False)
    sock = Mock()
    sock.getsockname.return_value = ("172.20.1.8", 49152)
    monkeypatch.setattr(shared_utils.socket, "socket", Mock(return_value=sock))

    assert shared_utils.get_local_ip("198.51.100.10") == "172.20.1.8"
    sock.connect.assert_called_once_with(("198.51.100.10", 80))
    sock.close.assert_called_once_with()


def test_get_local_ip_falls_back_when_control_address_is_blank(monkeypatch):
    monkeypatch.setenv("NEXUS_N3_CONTROL_ADDRESS", "   ")
    sock = Mock()
    sock.getsockname.return_value = ("10.0.0.25", 49152)
    monkeypatch.setattr(shared_utils.socket, "socket", Mock(return_value=sock))

    assert shared_utils.get_local_ip() == "10.0.0.25"
    sock.close.assert_called_once_with()
