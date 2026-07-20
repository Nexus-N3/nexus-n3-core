"""
Live ZeroMQ smoke test for Azure bridge reported-properties capture.

Run this while:
- `nexus_n3_server.py` is running with the ZeroMQ gateway
- `python -m nexus_n3.azure_bridge` is running

This script sends `CMD_IS_SERVER_READY` and prints the resulting payload so you
can confirm the bridge has the capability data it reports to Azure twin state.
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
        time.sleep(1.0)

        command = {"type": mt.CMD_IS_SERVER_READY}
        cmd_pub.send_json(command)
        print(f"Sent command: {command['type']}")

        while True:
            event = evt_sub.recv_json()
            if event.get("type") != mt.EVT_SERVER_READY:
                continue

            payload = event.get("payload", {})
            print("Observed EVT_SERVER_READY payload:")
            print(payload)
            print("supported_sensors:")
            print(payload.get("supported_sensors", []))
            print("supported_gateways:")
            print(payload.get("supported_gateways", []))
            print("Success: bridge should now have published updated reported properties to IoT Hub.")
            break
    except zmq.Again:
        print("Timed out waiting for EVT_SERVER_READY")
    finally:
        cmd_pub.close(linger=0)
        evt_sub.close(linger=0)


if __name__ == "__main__":
    main()
