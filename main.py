"""Project entrypoint for the Critical Minerals replication workflow.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def run_python_script(script_path: Path, project_root: Path) -> None:
    """Run a Python script with the current interpreter and project-root cwd."""
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    env = os.environ.copy()
    env["REPLICATION_ROOT"] = str(project_root)

    print(f"\n[RUN] {script_path.relative_to(project_root)}")
    subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(project_root),
        env=env,
        check=True,
    )


def run_streamlit_map(script_path: Path, project_root: Path) -> None:
    """Launch the interactive Streamlit map app."""
    if not script_path.exists():
        raise FileNotFoundError(f"Streamlit app not found: {script_path}")

    env = os.environ.copy()
    env["REPLICATION_ROOT"] = str(project_root)

    print(f"\n[RUN] Streamlit map: {script_path.relative_to(project_root)}")
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", str(script_path)],
        cwd=str(project_root),
        env=env,
        check=True,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run replication analysis scripts from the project root.",
    )
    parser.add_argument(
        "--only",
        choices=["all", "bgs", "mcs"],
        default="all",
        help="Choose which analysis workflow to run (default: all).",
    )
    parser.add_argument(
        "--with-map",
        action="store_true",
        help="Also launch the Streamlit interactive map after analyses finish.",
    )
    return parser.parse_args()


def main() -> int:
    project_root = Path(__file__).resolve().parent

    bgs_script = project_root / "code" / "data_analysis" / "bgs_descriptive_statistics.py"
    mcs_script = project_root / "code" / "data_analysis" / "mcs2025_reserves_descriptives.py"
    map_script = project_root / "code" / "data_analysis" / "interactive_map.py"

    args = parse_args()

    print("Critical Minerals replication runner")
    print(f"Project root: {project_root}")

    try:
        if args.only in {"all", "bgs"}:
            run_python_script(bgs_script, project_root)

        if args.only in {"all", "mcs"}:
            run_python_script(mcs_script, project_root)

        if args.with_map:
            run_streamlit_map(map_script, project_root)

    except subprocess.CalledProcessError as exc:
        print(f"\n[ERROR] Command failed with exit code {exc.returncode}")
        return exc.returncode
    except FileNotFoundError as exc:
        print(f"\n[ERROR] {exc}")
        return 1

    print("\n[DONE] Workflow completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
