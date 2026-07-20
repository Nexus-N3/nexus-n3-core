"""CLI entrypoint for the Nexus N3 Core Admin app."""

import argparse
from pathlib import Path

import uvicorn

from nexus_n3.admin.app import AdminState, create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Nexus N3 Core Admin")
    # update this to 0.0.0.0 to access via AP
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=9000, help="Bind port")
    parser.add_argument("--role", default="standalone", help="Node role")
    parser.add_argument("--site", default=None, help="Site name")
    parser.add_argument("--gateway", default=None, help="Gateway name")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    state = AdminState(
        project_root=project_root,
        role=args.role,
        site=args.site,
        gateway_name=args.gateway,
    )
    app = create_app(state)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
