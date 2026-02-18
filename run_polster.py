"""Script to run Dagster with a stable, portable home directory.

This script provides a reliable way to run Dagster operations with proper
environment setup for absolute imports.

Usage:
  python run_polster.py --ui                    # Launch UI only
  python run_polster.py                         # Materialize all assets
  python run_polster.py --ui --materialize      # Materialize all + launch UI
  python run_polster.py --asset bronze_users    # Materialize specific asset
  python run_polster.py --status                # Show pipeline status
"""

import argparse
import os
import pathlib
import subprocess
import sys

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def find_project_root(start: pathlib.Path) -> pathlib.Path:
    """Walk up directories until a pyproject.toml is found."""
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise FileNotFoundError("pyproject.toml not found above run_dagster.py")


def build_env(root: pathlib.Path) -> dict[str, str]:
    """Build environment with proper Dagster and Python path configuration."""
    dagster_home = root / ".dagster"
    dagster_home.mkdir(exist_ok=True)

    env = os.environ.copy()
    env["DAGSTER_HOME"] = str(dagster_home)

    pythonpath = str(root / "src")
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = pythonpath + os.pathsep + env["PYTHONPATH"]
    else:
        env["PYTHONPATH"] = pythonpath

    return env


def materialize_assets(root: pathlib.Path, env: dict[str, str]) -> bool:
    """Materialize all assets and return success status."""
    print("[START] Materializing all assets...")
    cmd = [
        "dagster",
        "asset",
        "materialize",
        "-m",
        "orchestration.definitions",
        "--select",
        "*",
    ]
    result = subprocess.call(cmd, cwd=root, env=env)
    if result == 0:
        print("[OK] Assets materialized successfully!")
        return True
    else:
        print("[ERROR] Asset materialization failed!")
        return False


def launch_ui(root: pathlib.Path, env: dict[str, str]):
    """Launch the Dagster development UI."""
    print("[WEB] Launching Dagster UI...")
    print("   Open http://127.0.0.1:3000 in your browser")
    print("   Press Ctrl+C to stop the server")
    try:
        subprocess.call(["dagster", "dev"], cwd=root, env=env)
    except KeyboardInterrupt:
        print("\n👋 Dagster UI stopped.")


def materialize_specific_assets(
    root: pathlib.Path, env: dict[str, str], asset_names: list[str]
) -> bool:
    """Materialize specific assets and return success status."""
    # Prefix asset names with "run_" to match Dagster asset keys
    dagster_asset_names = [f"run_{name}" for name in asset_names]
    asset_selection = ",".join(dagster_asset_names)
    print(f"[START] Materializing assets: {', '.join(asset_names)}...")
    cmd = [
        "dagster",
        "asset",
        "materialize",
        "-m",
        "orchestration.definitions",
        "--select",
        asset_selection,
    ]
    result = subprocess.call(cmd, cwd=root, env=env)
    if result == 0:
        print(f"[OK] Assets materialized successfully: {', '.join(asset_names)}!")
        return True
    else:
        print(f"[ERROR] Asset materialization failed for: {', '.join(asset_names)}!")
        return False


def show_pipeline_status(root: pathlib.Path, env: dict[str, str]):
    """Show current pipeline status and asset health."""
    print("[STATUS] Checking pipeline health...")
    try:
        # Try to get recent runs using dagster CLI
        cmd = ["dagster", "job", "list", "-m", "orchestration.definitions"]
        result = subprocess.run(cmd, cwd=root, env=env, capture_output=True, text=True)

        if result.returncode == 0:
            print("[OK] Pipeline is accessible")
            print("💡 Tip: Launch the UI with --ui to see detailed asset statuses")
        else:
            print("[WARN] Could not query pipeline status")
            print("💡 Tip: Launch the UI with --ui to check asset statuses")
    except Exception as e:
        print(f"[ERROR] Failed to check pipeline status: {e}")
        print("💡 Tip: Launch the UI with --ui to check asset statuses")


def main():
    SCRIPT_PATH = pathlib.Path(__file__).resolve()
    ROOT = find_project_root(SCRIPT_PATH.parent)
    ENV = build_env(ROOT)

    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Run Dagster operations with proper environment setup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_polster.py --ui                    # Launch UI only
  python run_polster.py                         # Materialize all assets
  python run_polster.py --ui --materialize      # Materialize all + launch UI
  python run_polster.py --asset bronze_users    # Materialize specific asset
  python run_polster.py --status                # Show pipeline status
        """,
    )
    parser.add_argument(
        "--ui", action="store_true", help="Launch Dagster UI after operations"
    )
    parser.add_argument(
        "--materialize",
        action="store_true",
        help="Materialize assets (default when not using --ui)",
    )
    parser.add_argument(
        "--asset",
        action="append",
        help="Materialize specific asset(s) (can be used multiple times)",
    )
    parser.add_argument(
        "--status", action="store_true", help="Show pipeline status and health"
    )
    args = parser.parse_args()

    # Validate arguments
    if args.asset and args.materialize:
        parser.error(
            "--asset already materializes the specified assets, remove --materialize"
        )

    if args.status and args.materialize:
        parser.error("--status only shows status, cannot be used with --materialize")

    if args.asset and args.status:
        parser.error("Cannot use --asset and --status together")

    # Execute operations based on arguments
    if args.status:
        show_pipeline_status(ROOT, ENV)
        if args.ui:
            launch_ui(ROOT, ENV)
    elif args.asset:
        success = materialize_specific_assets(ROOT, ENV, args.asset)
        if not success and not args.ui:
            sys.exit(1)  # Exit if materialization failed and not launching UI
        if args.ui:
            launch_ui(ROOT, ENV)
    else:
        # Default behavior: materialize unless explicitly UI-only
        should_materialize = args.materialize or not args.ui
        if should_materialize:
            success = materialize_assets(ROOT, ENV)
            if not success and not args.ui:
                sys.exit(1)  # Exit if materialization failed and not launching UI

        if args.ui:
            launch_ui(ROOT, ENV)
        elif not should_materialize:
            print(
                "💡 Tip: Use --materialize to run the pipeline, or --ui to launch the dashboard"
            )


if __name__ == "__main__":
    main()
