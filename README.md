# EnzymeInfoCollector

EnzymeInfoCollector gathers a reproducible enzyme record from three authoritative resources: ExPASy ENZYME for nomenclature and EC entries, UniProtKB for protein annotations and sequences, and Rhea for curated reactions. It writes a local JSON or CSV file and records where each source response came from.

The project is a client-side data collector, not a replacement for BRENDA, ExplorEnz, Open Enzyme Database, or CLEAN DB. It uses their complementary identifiers to assemble a small, traceable dataset for research workflows.

## Requirements

- Python 3.10 or newer
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

Read a spreadsheet, CSV, or TSV with a `Protein`, `Enzyme`, or `EC number` column. The first worksheet is used for XLSX files.

```bash
enzymeinfocollector --file enzyme_list.xlsx -o enzymes.json
enzymeinfocollector --file enzyme_list.csv -o enzymes.csv
```

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

## Output

JSON is the default. When an EC entry has been transferred, the original entry remains in `enzymes` and gains a `resolution` object. The collector follows chained and one-to-many ENZYME transfers, fetches each unique active terminal EC once, and records up to 10,000 complete paths per requested EC. If more paths remain, `resolution.path_limit_reached` is `true`, a `truncated` marker appears in `resolution.paths`, and the result is marked partial even if no active terminal was found among the examined paths. `resolution.status` is `resolved`, `partial`, or `unresolved`; missing entries, deleted entries, cycles, and transfers without a target are listed explicitly. Active terminal records appear under `resolution.resolved_enzymes` with the original requested EC and their resolution paths. Existing active and deleted entry records retain their current shape.

CSV output uses one row per UniProt protein for each matched EC entry. Its existing columns and their order are preserved; transfer metadata columns are appended. Transfer sources are preserved as `record_kind=enzyme` rows, and their active targets are included as `record_kind=resolved_target` rows with `requested_ec`, `resolution_status`, and JSON-encoded `resolution_paths`; unresolved paths remain on the source row. Its `ec_reactions` column contains the EC-level Rhea reaction list as JSON, and `association_basis` marks that link as EC-level; it does not claim that each protein has an individually curated link to every reaction. List-valued columns are JSON-encoded. FASTA includes proteins collected for resolved active targets. Each result includes:

- ENZYME EC number, accepted and alternative names, catalytic activity, comments, and UniProt cross-references
- UniProt accessions, protein/gene names, organism, reviewed status, sequence length, and amino acid sequence when requested
- Rhea reaction ID, equation, ChEBI participant names and IDs, and reaction SMILES when Rhea provides a matching directed reaction record. Undirected master IDs include available left-to-right/right-to-left SMILES in `reaction_smiles_directions`.
- Per-source request URL, retrieval time, cache status, and stale-cache warning where applicable

Reaction SMILES and the direction mapping are read from Rhea's published TSV files. They are not reconstructed by assuming participant order from the human-readable equation. An undirected reaction has a null `reaction_smiles` value and a `reaction_smiles_directions` object when directed forms are available. A reaction with no matching SMILES record has a null value and an empty directions object.

Output replacement is atomic: a failed collection does not overwrite an existing result file with a partial dataset. A valid query with no match is included in the output with status `not_found`. Rhea responses at its 10,000-row cap are rejected as potentially incomplete rather than written silently.

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

GitHub Actions runs the test suite on Python 3.10 and 3.12. The software is licensed under MIT; the separate license text is in [LICENSE](LICENSE).
