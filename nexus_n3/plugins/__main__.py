"""Minimal developer-facing CLI for plugin catalog bootstrap and install work."""

from __future__ import annotations

import argparse
import json

from .dev import load_dev_bootstrap_config, prepare_dev_plugins, prepare_dev_plugins_from_env
from .install.config import resolve_plugin_root
from .install.installer import PluginInstaller


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m nexus_n3.plugins")
    subparsers = parser.add_subparsers(dest="command", required=True)

    show_root = subparsers.add_parser("show-root")
    show_root.add_argument("--plugin-root")

    install = subparsers.add_parser("install")
    install.add_argument("bundle")
    install.add_argument("--plugin-root")
    install.add_argument("--no-activate", action="store_true")
    install.add_argument("--system-site-packages", action="store_true")

    install_dev = subparsers.add_parser("install-dev")
    install_dev.add_argument(
        "--plugin-catalog-root",
        dest="plugin_catalog_root",
        help="Path to the nexus-n3-plugin-catalog workspace.",
    )
    install_dev.add_argument("--plugin-root")
    install_dev.add_argument("--plugin", action="append", default=[])
    install_dev.add_argument("--plugin-tooling-root")
    install_dev.add_argument("--plugin-build-root")
    install_dev.add_argument("--system-site-packages", action="store_true")

    install_dev_from_env = subparsers.add_parser("install-dev-list")
    install_dev_from_env.add_argument("--plugin-root")
    install_dev_from_env.add_argument("--plugin-tooling-root")
    install_dev_from_env.add_argument("--plugin-build-root")
    install_dev_from_env.add_argument("--system-site-packages", action="store_true")

    show_dev_list = subparsers.add_parser("show-dev-list")
    show_dev_list.add_argument("--json", action="store_true")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "show-root":
        print(resolve_plugin_root(args.plugin_root))
        return 0

    if args.command == "install":
        installer = PluginInstaller(
            args.plugin_root,
            system_site_packages=args.system_site_packages,
        )
        result = installer.install_bundle(args.bundle, activate=not args.no_activate)
        print(
            json.dumps(
                {
                    "plugin_id": result.plugin_id,
                    "version": result.version,
                    "plugin_root": str(result.plugin_root),
                    "install_path": str(result.install_path),
                    "activated": result.activated,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    if args.command == "install-dev":
        if not args.plugin_catalog_root:
            parser.error("--plugin-catalog-root is required for install-dev")
        if not args.plugin:
            parser.error("at least one --plugin value is required for install-dev")
        prepared = prepare_dev_plugins(
            plugin_catalog_root=args.plugin_catalog_root,
            plugin_root=args.plugin_root,
            plugin_tooling_root=args.plugin_tooling_root,
            build_root=args.plugin_build_root,
            selected_plugins=args.plugin,
            system_site_packages=args.system_site_packages,
        )
        print(json.dumps(_prepared_plugins_payload(prepared), indent=2, sort_keys=True))
        return 0

    if args.command == "install-dev-list":
        prepared = prepare_dev_plugins_from_env(
            plugin_root=args.plugin_root,
            plugin_tooling_root=args.plugin_tooling_root,
            build_root=args.plugin_build_root,
            system_site_packages=args.system_site_packages,
        )
        print(json.dumps(_prepared_plugins_payload(prepared), indent=2, sort_keys=True))
        return 0

    if args.command == "show-dev-list":
        config = load_dev_bootstrap_config()
        payload = {
            "enabled": config.enabled,
            "plugin_catalog_root": (
                str(config.plugin_catalog_root) if config.plugin_catalog_root else None
            ),
            "selected_plugins": config.selected_plugins,
            "plugin_root": str(resolve_plugin_root()),
        }
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"enabled={payload['enabled']}")
            print(f"plugin_catalog_root={payload['plugin_catalog_root']}")
            print(f"plugin_root={payload['plugin_root']}")
            print("selected_plugins=" + (", ".join(payload["selected_plugins"]) or "<empty>"))
        return 0

    parser.error(f"unsupported command: {args.command}")
    return 2


def _prepared_plugins_payload(prepared) -> list[dict]:
    return [
        {
            "plugin_id": item.plugin_id,
            "plugin_source_root": str(item.plugin_root),
            "bundle_path": str(item.bundle_path),
            "installed": item.installed,
            "install_path": str(item.install_path) if item.install_path else None,
        }
        for item in prepared
    ]


if __name__ == "__main__":
    raise SystemExit(main())
