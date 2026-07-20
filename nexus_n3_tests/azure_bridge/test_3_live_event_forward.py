"""
Live ZeroMQ smoke test for Azure bridge event forwarding.

Run this while:
- `nexus_n3_server.py` is running with the ZeroMQ gateway
- `python -m nexus_n3.azure_bridge` is running

This script sends `CMD_IS_SERVER_READY` and prints the first event received.
That event should also be forwarded by the bridge to Azure IoT Hub.
"""

import time

import zmq

from nexus_n3.gateway.messaging import message_types as mt


def main() -> None:
    ctx = zmq.Context.instance()

    cmd_pub = ctx.socket(zmq.PUB)
    cmd_pub.connect("tcp://127.0.0.1:5555")

    evt_sub = ctx.socket(zmq.SUB)
    evt_sub.connect("tcp://127.0.0.1:5556")
    evt_sub.setsockopt_string(zmq.SUBSCRIBE, "")
    evt_sub.setsockopt(zmq.RCVTIMEO, 10000)

    try:
        # Allow PUB/SUB sockets to connect before sending the first command.
        time.sleep(1.0)

        command = {"type": mt.CMD_IS_SERVER_READY}
        cmd_pub.send_json(command)
        print(f"Sent command: {command['type']}")

        while True:
            event = evt_sub.recv_json()
            print(f"Received event: {event}")
            if event.get("type") == mt.EVT_SERVER_READY:
                print("Success: server_ready observed locally. Check IoT Hub for forwarded telemetry.")
                break
    except zmq.Again:
        print("Timed out waiting for EVT_SERVER_READY")
    finally:
        cmd_pub.close(linger=0)
        evt_sub.close(linger=0)


if __name__ == "__main__":
    main()
