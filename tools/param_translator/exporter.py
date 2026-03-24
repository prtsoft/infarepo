"""
Exporter — writes all output artifacts for the param-translator.

Option C layout (S3 source of truth):

  <output_dir>/
    params/
      <stem>.json                  Full structured param file (all sections, types, notes)

    glue-params/
      <FOLDER>/
        <WORKFLOW>.json            Flat key→value per workflow (merged: global < wf < session)
        <WORKFLOW>.<SESSION>.json  Flat key→value for a specific session (overrides workflow)

    param_loader.py                Utility module dropped into every Glue job's S3 prefix.
                                   Loads the correct JSON at runtime and merges with job args.

    terraform-snippets/
      <stem>_params_args.tf.txt   Terraform default_arguments snippet showing
                                   --PARAMS_S3_PATH arg to add to each aws_glue_job.

    translation-report.json        Summary: param counts, type distribution, warnings.

The JSON format for glue-params files:
  {
    "_metadata": {
      "source_prm": "SALES_MART.prm",
      "section":    "SALES_MART.WF_DAILY:s_M_LOAD",
      "generated":  "2026-03-23T..."
    },
    "START_DATE":  { "value": "2024-01-01", "type": "DATE",      "spark_value": "\"2024-01-01\"" },
    "FILTER_SQL":  { "value": "STATUS != ...", "type": "SQL",    "spark_value": "..." },
    "DATE_MASK":   { "value": "MM/DD/YYYY",  "type": "DATE_MASK","spark_value": "MM/dd/yyyy" },
    "BATCH_SIZE":  { "value": "1000",        "type": "INTEGER",  "spark_value": "1000" }
  }
"""

from __future__ import annotations
import json
import logging
import textwrap
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .models import ParamType, PrmFile, PrmParameter, PrmSection, SectionType

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _param_to_dict(p: PrmParameter) -> Dict[str, Any]:
    return {
        "value":      p.normalized_value if p.normalized_value else p.raw_value,
        "raw_value":  p.raw_value,
        "type":       p.param_type,
        "spark_value": p.spark_value,
        "glue_arg":   f"--{p.glue_arg_name}",
        "notes":      p.notes,
        "source_line": p.source_line,
    }


def _flat_param(p: PrmParameter) -> Dict[str, Any]:
    """Minimal flat representation for glue-params JSON files."""
    return {
        "value":       p.normalized_value if p.normalized_value else p.raw_value,
        "type":        p.param_type,
        "spark_value": p.spark_value,
    }


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    log.info("  Written: %s", path)


# ---------------------------------------------------------------------------
# 1. Full structured params JSON
# ---------------------------------------------------------------------------

def write_full_params_json(prm: PrmFile, output_dir: Path) -> Path:
    stem = Path(prm.source_path).stem
    out  = output_dir / "params" / f"{stem}.json"

    sections_data = {}
    for section in prm.sections:
        sections_data[section.key] = {
            "_header":       section.raw_header,
            "_section_type": section.section_type,
            "_folder":       section.folder,
            "_workflow":     section.workflow,
            "_task":         section.task,
            **{name: _param_to_dict(p) for name, p in section.params.items()},
        }

    merged_data = {
        name: _param_to_dict(p)
        for name, p in prm.merged.items()
    }

    data = {
        "_metadata": {
            "source_file": prm.source_path,
            "generated":   _ts(),
            "total_params": sum(len(s.params) for s in prm.sections),
            "sections":    len(prm.sections),
        },
        "sections": sections_data,
        "merged":   merged_data,
    }
    _write_json(out, data)
    return out


# ---------------------------------------------------------------------------
# 2. Flat glue-params JSON (one per section)
# ---------------------------------------------------------------------------

def _safe_filename(name: str) -> str:
    """Replace characters that are invalid in file/directory names on Windows and POSIX."""
    # Colons are the most common offender (WF:session), but also handle / \ * ? " < > |
    for ch in r':\/|*?"<>':
        name = name.replace(ch, "_")
    return name


def write_glue_params_json(prm: PrmFile, output_dir: Path) -> List[Path]:
    stem   = Path(prm.source_path).stem
    paths  = []

    # Write one flat JSON per section (folder/workflow structure)
    for section in prm.sections:
        if section.section_type == SectionType.GLOBAL:
            folder_dir = output_dir / "glue-params" / "_global"
            filename   = f"{stem}.json"
        else:
            safe_wf    = _safe_filename(section.workflow or "_nowf")
            folder_dir = output_dir / "glue-params" / _safe_filename(section.folder or "_nofolder")
            if section.task:
                filename = f"{safe_wf}.{_safe_filename(section.task)}.json"
            else:
                filename = f"{safe_wf}.json"

        out = folder_dir / filename
        metadata = {
            "source_prm":  prm.source_path,
            "section":     section.key,
            "section_type": section.section_type,
            "generated":   _ts(),
        }
        flat = {"_metadata": metadata}
        flat.update({name: _flat_param(p) for name, p in section.params.items()})
        _write_json(out, flat)
        paths.append(out)

    # Also write the merged view (global < workflow < session precedence)
    merged_dir  = output_dir / "glue-params" / "_merged"
    merged_out  = merged_dir / f"{stem}.json"
    merged_data = {
        "_metadata": {
            "source_prm": prm.source_path,
            "section":    "merged (global < workflow < session)",
            "generated":  _ts(),
        },
    }
    merged_data.update({name: _flat_param(p) for name, p in prm.merged.items()})
    _write_json(merged_out, merged_data)
    paths.append(merged_out)

    return paths


# ---------------------------------------------------------------------------
# 3. param_loader.py  — runtime S3 loader for Glue jobs
# ---------------------------------------------------------------------------

_PARAM_LOADER_PY = textwrap.dedent("""\
    \"\"\"
    param_loader.py — Runtime parameter loader for AWS Glue jobs.

    Drop this file into your Glue job's --extra-py-files S3 location alongside
    the main script.

    Usage in your Glue job script:
        from param_loader import load_params, merge_with_args

        params = load_params(args["PARAMS_S3_PATH"])
        # params is a flat dict: { "PARAM_NAME": "value", ... }

        # Or merge with existing job args (args take precedence over JSON):
        effective = merge_with_args(params, args)

    Option C pattern:
        - JSON config lives on S3 (source of truth)
        - Terraform sets --PARAMS_S3_PATH per environment
        - Runtime args can override any individual param at trigger time
    \"\"\"
    from __future__ import annotations
    import json
    import logging
    from typing import Any, Dict, Optional

    import boto3

    logger = logging.getLogger(__name__)


    def load_params(s3_uri: str) -> Dict[str, Any]:
        \"\"\"
        Load a glue-params JSON file from S3.

        Args:
            s3_uri: Full S3 URI, e.g. s3://my-bucket/glue-params/FOLDER/WORKFLOW.json

        Returns:
            Flat dict of param_name → resolved value string.
            Skips _metadata key.
        \"\"\"
        if not s3_uri or s3_uri.strip() == "":
            logger.warning("PARAMS_S3_PATH is empty — no params loaded from S3")
            return {}

        if not s3_uri.startswith("s3://"):
            raise ValueError(f"PARAMS_S3_PATH must be an s3:// URI, got: {s3_uri!r}")

        bucket, _, key = s3_uri[5:].partition("/")
        logger.info("Loading params from s3://%s/%s", bucket, key)

        s3 = boto3.client("s3")
        try:
            obj  = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(obj["Body"].read().decode("utf-8"))
        except s3.exceptions.NoSuchKey:
            raise FileNotFoundError(f"Params file not found: {s3_uri}")

        result: Dict[str, Any] = {}
        for k, v in data.items():
            if k.startswith("_"):
                continue           # skip _metadata
            if isinstance(v, dict):
                result[k] = v.get("value", "")
            else:
                result[k] = v

        logger.info("Loaded %d param(s) from %s", len(result), s3_uri)
        return result


    def load_spark_values(s3_uri: str) -> Dict[str, Any]:
        \"\"\"
        Like load_params() but returns spark_value instead of value.
        Useful for SQL filters and date masks that need PySpark syntax.
        \"\"\"
        if not s3_uri or s3_uri.strip() == "":
            return {}

        bucket, _, key = s3_uri[5:].partition("/")
        s3   = boto3.client("s3")
        obj  = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))

        result: Dict[str, Any] = {}
        for k, v in data.items():
            if k.startswith("_"):
                continue
            if isinstance(v, dict):
                result[k] = v.get("spark_value", v.get("value", ""))
            else:
                result[k] = v
        return result


    def merge_with_args(params: Dict[str, Any], args: Dict[str, Any]) -> Dict[str, Any]:
        \"\"\"
        Merge S3 params with Glue job args.
        Job args take precedence (allows runtime override at trigger time).
        \"\"\"
        merged = dict(params)          # base: S3 JSON
        for k, v in args.items():
            clean_k = k.lstrip("-")    # strip leading -- from arg name
            if v and v != "":          # only override if the arg is non-empty
                merged[clean_k] = v
        return merged


    def get_param(
        params: Dict[str, Any],
        name: str,
        default: Optional[str] = None,
        required: bool = False,
    ) -> Optional[str]:
        \"\"\"
        Get a single parameter value with optional default and required check.

        Args:
            params:   The merged params dict from merge_with_args()
            name:     Parameter name (without $$)
            default:  Default value if not found
            required: If True, raises ValueError when not found and no default
        \"\"\"
        val = params.get(name, default)
        if val is None and required:
            raise ValueError(
                f"Required parameter '{name}' not found in params or job args. "
                f"Check your glue-params JSON and --PARAMS_S3_PATH setting."
            )
        return val
""")


def write_param_loader(output_dir: Path) -> Path:
    out = output_dir / "param_loader.py"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(_PARAM_LOADER_PY, encoding="utf-8")
    log.info("  Written: %s", out)
    return out


# ---------------------------------------------------------------------------
# 4. Terraform snippet
# ---------------------------------------------------------------------------

def write_terraform_snippet(prm: PrmFile, output_dir: Path) -> Path:
    stem = Path(prm.source_path).stem

    # Collect unique workflow names for snippet generation
    workflows = list(dict.fromkeys(
        s.workflow for s in prm.sections
        if s.section_type != SectionType.GLOBAL
    ))

    snippets = []
    for wf in workflows:
        resource_name = re.sub(r"[^a-z0-9_]", "_", wf.lower())
        snippets.append(textwrap.dedent(f"""\
            # Add to aws_glue_job "{resource_name}" default_arguments:
            #
            #   "--PARAMS_S3_PATH" = "s3://${{var.config_bucket}}/glue-params/${{var.environment}}/{wf}.json"
            #
            # This tells the Glue job where to load its runtime parameters.
            # The JSON file is written by param-translator and uploaded to S3 per environment.
            #
            # Upload command (run once per environment):
            #   aws s3 cp glue-params/{stem}/{wf}.json \\
            #       s3://<config-bucket>/glue-params/<env>/{wf}.json
        """))

    out = output_dir / "terraform-snippets" / f"{stem}_params_args.tf.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        f"# Terraform default_arguments snippets for {stem}.prm\n"
        f"# Generated: {_ts()}\n\n"
        + "\n".join(snippets),
        encoding="utf-8",
    )
    log.info("  Written: %s", out)
    return out


# ---------------------------------------------------------------------------
# 5. Translation report
# ---------------------------------------------------------------------------

def write_translation_report(prm_files: List[PrmFile], output_dir: Path) -> Path:
    type_counts: Dict[str, int] = {}
    warnings: List[Dict] = []
    total_params = 0

    for prm in prm_files:
        for section in prm.sections:
            for name, param in section.params.items():
                total_params += 1
                t = param.param_type
                type_counts[t] = type_counts.get(t, 0) + 1
                for note in param.notes:
                    if any(kw in note.lower() for kw in ("todo", "manually", "warning", "verify", "migrate")):
                        warnings.append({
                            "file":    prm.source_path,
                            "section": section.key,
                            "param":   name,
                            "note":    note,
                        })

    report = {
        "_generated": _ts(),
        "summary": {
            "files_processed": len(prm_files),
            "total_params":    total_params,
            "type_distribution": type_counts,
            "total_warnings":  len(warnings),
        },
        "warnings": warnings,
    }

    out = output_dir / "translation-report.json"
    _write_json(out, report)
    return out


# ---------------------------------------------------------------------------
# Import fix for terraform snippet
# ---------------------------------------------------------------------------

import re


# ---------------------------------------------------------------------------
# Top-level export function
# ---------------------------------------------------------------------------

def export_all(prm_files: List[PrmFile], output_dir: Path) -> Dict[str, List[Path]]:
    """
    Run all exporters for a list of parsed+normalised PrmFile objects.
    Returns a dict of output category → list of written paths.
    """
    output_dir = Path(output_dir)
    written: Dict[str, List[Path]] = {
        "full_params":  [],
        "glue_params":  [],
        "loader":       [],
        "tf_snippets":  [],
        "report":       [],
    }

    for prm in prm_files:
        written["full_params"].append(write_full_params_json(prm, output_dir))
        written["glue_params"].extend(write_glue_params_json(prm, output_dir))
        written["tf_snippets"].append(write_terraform_snippet(prm, output_dir))

    loader_path = write_param_loader(output_dir)
    written["loader"].append(loader_path)

    report_path = write_translation_report(prm_files, output_dir)
    written["report"].append(report_path)

    return written
