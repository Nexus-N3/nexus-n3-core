import time
from nexus_n3.data_file_offload.sinks.usb import USBDiskManager

def on_usb_inserted(path):
    print(f"[TEST] USB inserted at {path}")

def on_usb_removed():
    print("[TEST] USB removed, falling back to local folder")

def main():
    manager = USBDiskManager()
    manager.register_callback("inserted", on_usb_inserted)
    manager.register_callback("removed", on_usb_removed)

    print(f"[TEST] Initial output path: {manager.path}")

    try:
        while True:
            time.sleep(1)
            # You can also poll the current path:
            # print(f"[TEST] Current path: {manager.path}")
    except KeyboardInterrupt:
        print("[TEST] Stopping USB manager...")
        manager.stop()

if __name__ == "__main__":
    main()
