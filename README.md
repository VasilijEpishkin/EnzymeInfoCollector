# EnzymeInfoCollector

EnzymeInfoCollector gathers a reproducible enzyme record from three authoritative resources: ExPASy ENZYME for nomenclature and EC entries, UniProtKB for protein annotations and sequences, and Rhea for curated reactions. It writes local JSON, CSV, or FASTA files and records where each source response came from.

The project is a client-side data collector, not a replacement for BRENDA, ExplorEnz, Open Enzyme Database, or CLEAN DB. It uses their complementary identifiers to assemble a small, traceable dataset for research workflows.

## Requirements

- Python 3.11 or newer
- Network access to ExPASy, UniProt, and Rhea for a fresh collection
- `openpyxl` only when reading Excel workbooks

## Install

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install .
```

For XLSX input, install the optional spreadsheet dependency:

```bash
python -m pip install '.[xlsx]'
```

## Use

Collect by enzyme name (an exact name match is preferred; otherwise the name is matched against official and alternative ENZYME names):

```bash
enzymeinfocollector --enzyme "Alcohol dehydrogenase" --output alcohol-dehydrogenase.json
```

Collect by one or more EC numbers:

```bash
enzymeinfocollector --ec 1.1.1.1 --ec 3.5.1.n3 -o enzymes.json
```

Reduce the UniProt result set or response size with server-side filters. The defaults still include all matching entries and sequences.

```bash
enzymeinfocollector --ec 1.1.1.1 --reviewed-only --taxon-id 4932 --no-sequences -o annotations.json
```

Taxon filters include descendant taxa. `--no-sequences` still returns protein names, accessions, organism IDs, EC annotations, and sequence lengths.

Read a CSV or TSV with a `Protein`, `Enzyme`, or `EC number` column. For spreadsheet input, XLSX is also supported when the optional dependency is installed; the first worksheet is used.

```bash
enzymeinfocollector --file enzyme_list.xlsx -o enzymes.json
enzymeinfocollector --file enzyme_list.csv -o enzymes.csv
```

Save a reusable request manifest with the normalized query list and collection filters, then replay it later without the original input file:

```bash
enzymeinfocollector --file enzyme_list.csv --reviewed-only --taxon-id 4932 \
  --save-manifest run.json -o enzymes.fasta
enzymeinfocollector --replay-manifest run.json -o refreshed-enzymes.fasta
```

Manifest files contain query values in plain text and should be stored accordingly. They preserve the request and collection filters, not upstream database snapshots; later replays can return different results as ExPASy, UniProt, and Rhea change. Output format, cache, timeout, retry, contact email, and refresh behavior remain controls for each invocation. Replay rejects conflicting collection filters.

Export collected protein sequences as FASTA for downstream sequence analysis:

```bash
enzymeinfocollector --ec 1.1.1.1 -o alcohol-dehydrogenase.fasta
```

FASTA records include UniProt accessions, EC annotations, organism names, and source labels. Duplicate accessions are written once. JSON output retains the full reaction and provenance records; FASTA is intended as a sequence-analysis export.

A plain text file may contain one enzyme name or full EC number per line. Prefix an EC number with `EC ` if you want to make its type explicit.

Add a contact email to the User-Agent as recommended by Rhea, and tune the request timeout or retry count when needed:

```bash
enzymeinfocollector --ec 1.1.1.1 --contact-email researcher@example.org --timeout 60 --retries 4 --verbose
```

`ENZYMEINFOCOLLECTOR_EMAIL` can provide the contact email instead. `--refresh` requests fresh responses instead of using unexpired cache entries. HTTP responses are cached under `~/.cache/enzymeinfocollector` (or `$XDG_CACHE_HOME/enzymeinfocollector`); source data and API results have a one to seven day cache lifetime. If a transient upstream failure occurs, a cached response up to 30 days old may be used and is marked `stale` in provenance. The program does not silently replace missing data with empty values when an upstream request fails.

Concurrent processes coordinate cache reads and refreshes per URL, so they cannot mix a response body with another request's metadata. Small `.lock` sidecar files are retained in the cache directory.

For the Python API, use a separate `EnzymeCollector` instance for each concurrent collection. An instance is not thread-safe; the shared HTTP cache itself coordinates concurrent access across processes. Source snapshots are loaded per `collect()` call, and the HTTP cache applies its TTL between calls; with `refresh=True`, each call requests an upstream refresh (a recent cached response may still be used after a transient failure).

Inspect cache size without displaying query URLs, or preview removal of entries older than the 30-day stale-response window:

```bash
enzymeinfocollector cache info
enzymeinfocollector cache prune
enzymeinfocollector cache prune --older-than-days 60 --apply
```

`cache info` also reports the number and logical size of `.lock` sidecars. Pruning is a dry run unless `--apply` is supplied; dry runs can create missing `.lock` sidecars so the cache can be inspected consistently while another process is collecting. Pruning locks and rechecks each eligible entry before deleting its body and metadata. Incomplete or invalid entries are reported by `cache info` and left untouched; `.lock` files and temporary files are never removed. Retention cannot be shorter than 30 days, preserving the existing offline fallback window.

## Output

JSON is the default. When an EC entry has been transferred, the original entry remains in `enzymes` and gains a `resolution` object. The collector follows chained and one-to-many ENZYME transfers, fetches each unique active terminal EC once, and records up to 10,000 complete paths per requested EC. If more paths remain, `resolution.path_limit_reached` is `true`, a `truncated` marker appears in `resolution.paths`, and the result is marked partial even if no active terminal was found among the examined paths. `resolution.status` is `resolved`, `partial`, or `unresolved`; missing entries, deleted entries, cycles, and transfers without a target are listed explicitly. Active terminal records appear under `resolution.resolved_enzymes` with the original requested EC and their resolution paths. Existing active and deleted entry records retain their current shape.

For large JSON exports, opt into lower-memory disk streaming:

```bash
enzymeinfocollector --ec 1.1.1.1 --stream-json --output results.json
```

CSV output uses one row per UniProt protein for each matched EC entry. Its existing columns and their order are preserved; transfer metadata columns are appended. Transfer sources are preserved as `record_kind=enzyme` rows, and their active targets are included as `record_kind=resolved_target` rows with `requested_ec`, `resolution_status`, and JSON-encoded `resolution_paths`; unresolved paths remain on the source row. Its `ec_reactions` column contains the EC-level Rhea reaction list as JSON, and `association_basis` marks that link as EC-level; it does not claim that each protein has an individually curated link to every reaction. List-valued columns are JSON-encoded. FASTA includes proteins collected for resolved active targets. Each result includes:

- ENZYME EC number, accepted and alternative names, catalytic activity, comments, and UniProt cross-references
- UniProt accessions, protein/gene names, organism, reviewed status, sequence length, and amino acid sequence when requested
- Rhea reaction ID, equation, ChEBI participant names and IDs, and reaction SMILES when Rhea provides a matching directed reaction record. Undirected master IDs include available left-to-right/right-to-left SMILES in `reaction_smiles_directions`.
- Per-source request URL, retrieval time, cache status, and stale-cache warning where applicable

CSV and FASTA CLI exports process proteins in bounded batches and use temporary disk runs to preserve accession order within each EC result. Repeated active ECs within one batch reuse their sorted runs and Rhea metadata; runs are removed after their final use. Runs use the system temporary directory and can require more disk space than the cached UniProt response. If a batch repeats many ECs before their final uses, their runs can coexist and raise peak disk use. JSON is eager by default. Add `--stream-json` to spool protein records to temporary disk and insert them into the completed JSON document, preserving transfer resolution and source provenance while reducing memory use. This uses more disk I/O and can take longer; the spool is created beside the output file. All CLI formats replace the destination atomically after collection succeeds. The Python `collect()` API retains its full in-memory result; in callback mode, it sends proteins to the callback and returns metadata-only enzyme records. Callback arguments are borrowed, read-only views valid only during the callback. Query results and enzyme records may be reused, and transfer resolution is updated as target records arrive. Copy values the callback needs to retain or modify.

An earlier three-run cached measurement on 2026-10-23 (CPython 3.12.3, WSL2, EC 1.1.1.1, 40,358 UniProt proteins with sequences) measured eager JSON at a median peak RSS of 196.5 MiB and 2.36 seconds, versus CSV at 109.6 MiB and 8.38 seconds. Output sizes were 35.3 MiB for JSON and 188.1 MiB for CSV. These describe that source snapshot and machine, not a resource guarantee; use `--stream-json` when reducing JSON memory matters more than runtime.

Reaction SMILES and the direction mapping are read from Rhea's published TSV files. They are not reconstructed by assuming participant order from the human-readable equation. An undirected reaction has a null `reaction_smiles` value and a `reaction_smiles_directions` object when directed forms are available. A reaction with no matching SMILES record has a null value and an empty directions object.

Output replacement is atomic: a failed collection does not overwrite an existing result file with a partial dataset. An enzyme-name query with no ExPASy match is included with result status `not_found`. For an explicit valid EC missing from ExPASy, the requested number is retained with enzyme status `not_found_in_expasy`; UniProt and Rhea are still queried independently, so protein or reaction data can be present. The CLI summary reports inputs without an ExPASy match separately from collection failures. Rhea responses at its 10,000-row cap are rejected as potentially incomplete rather than written silently.

## Sources and data use

- [ExPASy ENZYME](https://enzyme.expasy.org/) publishes the official nomenclature file used for name and EC lookup.
- [UniProt REST API](https://rest.uniprot.org/) supplies protein records and sequences.
- [Rhea REST API](https://www.rhea-db.org/help/rest-api) supplies curated reactions; [Rhea TSV downloads](https://www.rhea-db.org/help/download) supply the directed reaction SMILES file.

Collected records remain subject to each upstream provider's terms, license, attribution, and citation requirements. Check those requirements before redistributing a collected dataset. Rhea describes its reaction-SMILES TSV as experimental/beta data; keep that status in mind when using the strings in downstream chemistry workflows.

## Development and checks

Run the offline parser, cache, pipeline, and export tests with:

```bash
python -m unittest discover -s tests -v
```

GitHub Actions runs the test suite on Python 3.11–3.15 on Linux and checks Python 3.11 and 3.15 on Windows. The software is licensed under MIT; the separate license text is in [LICENSE](LICENSE).
