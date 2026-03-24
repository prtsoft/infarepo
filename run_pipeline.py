#!/usr/bin/env python3
"""
Full migration pipeline runner.

Chains the tools in dependency order:
  1. pc-extractor extract
  2. param-translator parse
  3. glue-gen generate-all
  4. databricks-gen generate-all
  5. workflow-gen generate-all  (optional — only if workflow_gen is installed)
  6. validation-harness validate  (optional — only if config exists)

Config is read from migration-intake.json (searched in CWD then repo root).
A checkpoint file (pipeline-state.json) tracks which steps completed so the
pipeline can resume after a partial failure without re-running earlier steps.

Usage:
    python run_pipeline.py [OPTIONS]

Options:
    --intake PATH        Path to migration-intake.json  [default: migration-intake.json]
    --exports DIR        Directory containing PC XML exports  [default: exports/]
    --params DIR         Directory containing .prm parameter files  [default: params/]
    --output DIR         Root output directory  [default: output/]
    --validation PATH    Path to validation YAML config (skip validation if absent)
    --from-step NAME     Resume from a specific step (extract, params, glue, databricks,
                         workflow, validate)
    --dry-run            Print commands without executing them
    --verbose / -v       Debug logging
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone

log = logging.getLogger("run_pipeline")

STEPS = ["extract", "params", "glue", "databricks", "workflow", "validate"]

CHECKPOINT_FILE = "pipeline-state.json"


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def _load_checkpoint(output_dir: Path) -> dict:
    path = output_dir / CHECKPOINT_FILE
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def _save_checkpoint(output_dir: Path, state: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / CHECKPOINT_FILE
    path.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# Step execution
# ---------------------------------------------------------------------------

def _run(cmd: list[str], dry_run: bool) -> bool:
    """Run a subprocess command. Returns True on success."""
    log.info("  $ %s", " ".join(str(c) for c in cmd))
    if dry_run:
        return True
    result = subprocess.run(cmd)
    if result.returncode != 0:
        log.error("  Command failed (exit %d)", result.returncode)
        return False
    return True


def run_pipeline(
    exports_dir: Path,
    params_dir: Path,
    output_dir: Path,
    validation_config: Path | None,
    from_step: str | None,
    dry_run: bool,
) -> bool:
    output_dir.mkdir(parents=True, exist_ok=True)
    state = _load_checkpoint(output_dir)

    manifest_path = output_dir / "migration-manifest.json"
    params_out = output_dir / "params"
    generated_dir = output_dir / "generated"

    start_idx = STEPS.index(from_step) if from_step and from_step in STEPS else 0
    if from_step and from_step in STEPS:
        log.info("Resuming from step: %s", from_step)

    # ── Step 1: extract ────────────────────────────────────────────────────
    if STEPS.index("extract") >= start_idx and state.get("extract") != "done":
        log.info("[1/6] Extracting PC XML exports → manifest")
        xml_files = sorted(exports_dir.glob("*.xml")) if exports_dir.exists() else []
        if not xml_files and not dry_run:
            log.error("No XML files found in %s — aborting", exports_dir)
            return False
        cmd = [
            sys.executable, "-m", "pc_extractor.cli", "extract",
            *[str(f) for f in xml_files],
            "--output-dir", str(output_dir),
        ]
        if not _run(cmd, dry_run):
            return False
        state["extract"] = "done"
        state["extract_ts"] = datetime.now(timezone.utc).isoformat()
        _save_checkpoint(output_dir, state)
    else:
        log.info("[1/6] extract — skipped (already done)")

    # ── Step 2: params ─────────────────────────────────────────────────────
    if STEPS.index("params") >= start_idx and state.get("params") != "done":
        log.info("[2/6] Translating parameter files")
        if params_dir.exists():
            cmd = [
                sys.executable, "-m", "param_translator.cli", "parse",
                str(params_dir),
                "--output-dir", str(params_out),
            ]
            if not _run(cmd, dry_run):
                return False
        else:
            log.info("  No params directory at %s — skipping", params_dir)
        state["params"] = "done"
        _save_checkpoint(output_dir, state)
    else:
        log.info("[2/6] params — skipped")

    # ── Step 3: glue ───────────────────────────────────────────────────────
    if STEPS.index("glue") >= start_idx and state.get("glue") != "done":
        log.info("[3/6] Generating Glue jobs")
        cmd = [
            sys.executable, "-m", "glue_gen.cli", "generate-all",
            str(manifest_path),
            "--output-dir", str(generated_dir),
        ]
        if params_out.exists():
            cmd += ["--params-dir", str(params_out)]
        if not _run(cmd, dry_run):
            return False
        state["glue"] = "done"
        _save_checkpoint(output_dir, state)
    else:
        log.info("[3/6] glue — skipped")

    # ── Step 4: databricks ────────────────────────────────────────────────
    if STEPS.index("databricks") >= start_idx and state.get("databricks") != "done":
        log.info("[4/6] Generating Databricks notebooks")
        cmd = [
            sys.executable, "-m", "databricks_gen.cli", "generate-all",
            str(manifest_path),
            "--output-dir", str(generated_dir),
        ]
        if params_out.exists():
            cmd += ["--params-dir", str(params_out)]
        if not _run(cmd, dry_run):
            return False
        state["databricks"] = "done"
        _save_checkpoint(output_dir, state)
    else:
        log.info("[4/6] databricks — skipped")

    # ── Step 5: workflow ──────────────────────────────────────────────────
    if STEPS.index("workflow") >= start_idx and state.get("workflow") != "done":
        log.info("[5/6] Generating workflow orchestration")
        try:
            import workflow_gen  # noqa: F401 — check if installed
            cmd = [
                sys.executable, "-m", "workflow_gen.cli", "generate-all",
                str(manifest_path),
                "--output-dir", str(generated_dir),
            ]
            if not _run(cmd, dry_run):
                log.warning("  workflow-gen failed — continuing")
        except ImportError:
            log.info("  workflow_gen not installed — skipping")
        state["workflow"] = "done"
        _save_checkpoint(output_dir, state)
    else:
        log.info("[5/6] workflow — skipped")

    # ── Step 6: validate ──────────────────────────────────────────────────
    if STEPS.index("validate") >= start_idx and state.get("validate") != "done":
        log.info("[6/6] Running post-migration validation")
        if validation_config and validation_config.exists():
            validation_out = output_dir / "validation"
            cmd = [
                sys.executable, "-m", "validation_harness.cli", "validate",
                str(validation_config),
                "--output-dir", str(validation_out),
            ]
            if not _run(cmd, dry_run):
                log.warning("  Validation reported failures — check %s", validation_out)
        else:
            log.info(
                "  No validation config provided (use --validation to enable) — skipping"
            )
        state["validate"] = "done"
        _save_checkpoint(output_dir, state)
    else:
        log.info("[6/6] validate — skipped")

    log.info("Pipeline complete. Output: %s", output_dir)
    return True


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--intake",      default="migration-intake.json", help="Path to migration-intake.json")
    p.add_argument("--exports",     default="exports",               help="Directory containing PC XML exports")
    p.add_argument("--params",      default="params",                help="Directory containing .prm files")
    p.add_argument("--output",      default="output",                help="Root output directory")
    p.add_argument("--validation",  default=None,                    help="Path to validation YAML config")
    p.add_argument("--from-step",   choices=STEPS, default=None,    help="Resume from this step")
    p.add_argument("--dry-run",     action="store_true",             help="Print commands without running")
    p.add_argument("--verbose", "-v", action="store_true",           help="Debug logging")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-8s %(message)s",
    )

    validation_config = Path(args.validation) if args.validation else None

    ok = run_pipeline(
        exports_dir=Path(args.exports),
        params_dir=Path(args.params),
        output_dir=Path(args.output),
        validation_config=validation_config,
        from_step=args.from_step,
        dry_run=args.dry_run,
    )
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
