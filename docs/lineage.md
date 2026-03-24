# lineage (Python API)

`pc_extractor.lineage` traces field-level data lineage through a PowerCenter mapping by walking the CONNECTOR graph backwards from every target field to its source(s). It produces a structured result showing each field's origin, the transformation chain it passed through, and any lookup annotations along the way.

This is a Python-only module — there is no CLI for it yet (see [TODOS.md](../TODOS.md)). Call it from your own scripts after parsing a manifest with `pc-extractor`.

## When to use this

- **Audit and HIPAA traceability** — demonstrate to your privacy officer that PHI fields originate from specific source tables.
- **Migration validation** — confirm that a generated Glue/Databricks script handles every source field that was wired in the original mapping.
- **S2T documentation** — generate source-to-target mapping spreadsheets for business sign-off.
- **Impact analysis** — find all downstream target fields that depend on a given source column.

## Basic usage

```python
from pc_extractor.xml_parser import parse_xml_files
from pc_extractor.scorer import score_all_mappings
from pc_extractor.lineage import trace_mapping
from pathlib import Path

manifest = parse_xml_files([Path("exports/SALES_MART.xml")])
score_all_mappings(manifest)

folder = manifest.folders["SALES_MART"]
mapping = folder.mappings["M_LOAD_FACT_ORDERS"]

# Pass folder.sources and folder.targets for enriched field type info
lineage = trace_mapping(mapping, folder.sources, folder.targets)

for field in lineage.fields:
    if field.sources:
        src = ", ".join(f"{s.table}.{s.field}" for s in field.sources)
        print(f"  {field.target_field:<25} ← {src}")
    else:
        expr = field.expression or "(unconnected)"
        print(f"  {field.target_field:<25} ← {expr}")
```

Output:

```
  ORDER_ID                  ← SRC_ORDERS.ORDER_ID
  CUSTOMER_ID               ← SRC_ORDERS.CUSTOMER_ID
  ORDER_AMT                 ← SRC_ORDERS.ORDER_AMT
  CUST_NAME                 ← (unconnected)
  PROD_CODE                 ← (unconnected)
  REGION                    ← (unconnected)
  LOAD_TS                   ← SYSDATE
```

---

## Return type: `MappingLineage`

```
MappingLineage
  .mapping_name   str
  .folder         str
  .fields         List[FieldLineage]
```

### `FieldLineage`

```
FieldLineage
  .target_table           str       — target transformation name
  .target_field           str       — target field name
  .sources                List[SourceRef]
  .chain                  List[LineageNode]
  .expression             str       — first non-trivial expression in the chain
  .has_unconnected_lookup bool
  .lookups                List[LookupRef]
  .notes                  List[str]
```

### `SourceRef`

```
SourceRef
  .table      str   — source instance name (matches SourceDef.name in the folder)
  .field      str   — source field name
  .field_type str   — datatype from SourceDef, if folder_sources was provided
```

### `LineageNode`

One hop in the chain. The chain runs from the first transformation downstream of the source to the last transformation upstream of the target.

```
LineageNode
  .instance       str   — transformation instance name
  .field          str   — field name at this transformation
  .transform_type str   — TransformationType value (e.g. "Expression", "Filter")
  .expression     str   — port's expression string (empty for pass-throughs)
  .lookup_ref     Optional[LookupRef]
```

### `LookupRef`

```
LookupRef
  .lookup_name       str
  .lookup_condition  Optional[str]   — condition expression (e.g. "CUSTOMER_ID = CUSTOMER_ID")
  .lookup_table      Optional[str]   — lookup source table name
  .sql_override      Optional[str]   — SQL override if defined
  .is_connected      bool            — False for :LKP.Name() unconnected calls
```

---

## Lookup handling

### Connected lookups

A connected lookup has explicit CONNECTOR wires from its INPUT port and returns values via OUTPUT ports. The traversal:

1. Arrives at a Lookup OUTPUT port (return field).
2. Records a `LookupRef(is_connected=True)` annotated on the `LineageNode`.
3. Follows the Lookup's INPUT port(s) upstream to find the source of the lookup key.

```python
for field in lineage.fields:
    for lkp in field.lookups:
        if lkp.is_connected:
            print(f"  {field.target_field} uses lookup {lkp.lookup_name}")
            print(f"    table:     {lkp.lookup_table}")
            print(f"    condition: {lkp.lookup_condition}")
```

### Unconnected lookups

An unconnected lookup is embedded in an expression string as `:LKP.LookupName(args)`. There is no connector wire — the lookup runs via the expression engine. The traversal detects these via regex and records `LookupRef(is_connected=False)`.

```python
for field in lineage.fields:
    if field.has_unconnected_lookup:
        for lkp in field.lookups:
            if not lkp.is_connected:
                print(f"  {field.target_field}: unconnected lookup {lkp.lookup_name}")
                print(f"    expression: {field.expression}")
```

---

## Transformation chain inspection

```python
lineage = trace_mapping(mapping, folder.sources, folder.targets)

for fl in lineage.fields:
    print(f"\n{fl.target_field}")
    for i, node in enumerate(fl.chain):
        indent = "  " * (i + 1)
        expr = f"  expr={node.expression!r}" if node.expression else ""
        print(f"{indent}← {node.instance}.{node.field} ({node.transform_type}){expr}")
```

Example output for `LOAD_TS` (derived from SYSDATE):

```
LOAD_TS
  ← FIL_STATUS.LOAD_TS (Filter)
    ← EXP_DERIVE.LOAD_TS (Expression)  expr='SYSDATE'
```

Example output for `ORDER_ID` (straight move):

```
ORDER_ID
  ← FIL_STATUS.ORDER_ID (Filter)
    ← EXP_DERIVE.ORDER_ID (Expression)  expr='ORDER_ID'
      ← SQ_ORDERS.ORDER_ID (Source Qualifier)
        ← SRC_ORDERS.ORDER_ID (Source Definition)
```

---

## Generating an S2T document

```python
import csv
from pathlib import Path

def write_s2t_csv(lineage, output_path: Path) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "target_table", "target_field",
            "source_table", "source_field", "source_field_type",
            "expression", "has_lookup", "lookup_names",
        ])
        for fl in lineage.fields:
            for src in fl.sources:
                writer.writerow([
                    fl.target_table, fl.target_field,
                    src.table, src.field, src.field_type,
                    fl.expression,
                    len(fl.lookups) > 0,
                    "|".join(l.lookup_name for l in fl.lookups),
                ])
            if not fl.sources:
                writer.writerow([
                    fl.target_table, fl.target_field,
                    "", "", "",
                    fl.expression or "(unconnected)",
                    len(fl.lookups) > 0,
                    "|".join(l.lookup_name for l in fl.lookups),
                ])

write_s2t_csv(lineage, Path("output/s2t_M_LOAD_FACT_ORDERS.csv"))
```

---

## What the traversal handles

| Scenario | Behaviour |
|---|---|
| Straight move through Filter / SQ / Joiner | Follows pass-through by name matching in expression |
| Expression OUTPUT port (e.g. `SYSDATE`) | Records expression, returns no sources (constant) |
| Expression INPUT/OUTPUT port | Follows the upstream connector for the named input port |
| Source Qualifier with OUTPUT-only ports | Falls back to following the connector INTO the SQ port, which leads to the Source Definition |
| Source Qualifier with INPUT/OUTPUT ports | Resolved via expression/name matching (same path) |
| Connected Lookup return port | Records LookupRef, traces INPUT port(s) upstream |
| Unconnected `:LKP.Name(args)` in expression | Records LookupRef(is_connected=False), no upstream connector to follow |
| Router OUTPUT port with REF_FIELD | Follows REF_FIELD to the input port |
| Normalizer OUTPUT port with REF_SOURCE_FIELD | Finds matching INPUT port via REF_SOURCE_FIELD group |
| Union / Custom FIELDDEPENDENCY | Follows each listed input field |
| Reusable transformation (not inline) | Returns empty — reusable trfs have no inline definition; logged at DEBUG |
| Cycle in connector graph | Stops at the cycle point; logged at DEBUG |

---

## Limitations

- **Mapplets** are not traversed. Mapplet INPUT/OUTPUT boundaries are treated as opaque.
- **Reusable transformations** defined outside the mapping XML (in the folder's shared section) require the full folder XML to be available, and the current implementation only resolves inline definitions.
- **Multiple connectors into the same target field** — only the last connector in declaration order is followed (PC does not allow true multi-input to a target field; duplicate connectors indicate a data model issue).
- The traversal does not evaluate expression *values* — only structure. It cannot tell you that `IIF(STATUS='A', 'ACTIVE', 'INACTIVE')` maps to two logical source values; it identifies `STATUS` as the source field.
