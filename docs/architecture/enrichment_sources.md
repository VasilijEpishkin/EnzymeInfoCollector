# Optional enrichment: ChEBI compounds and Rhea reaction cross-references

Status: **initial opt-in Python API, stacked after PR2**. This work does not
change the legacy CLI or its EC-level JSON/CSV/FASTA results.

## Scientific boundaries

| Record | What it means | What it does NOT mean |
| --- | --- | --- |
| `CompoundSnapshot` | ChEBI's primary chemical identity, optional structure, and response provenance | A protein catalyzes a reaction containing that compound |
| `ChEBIResolution` | ChEBI explicitly identifies a queried primary/secondary ID | Unverified name/synonym matching |
| `RheaCompoundLink` | A ChEBI ID appears in a Rhea query's participant list | Known side, stoichiometry, or direction-specific substrate role |
| `ReactionExternalCrossReference` | Rhea publishes a KEGG or MetaCyc reaction ID mapping | Exact chemical equivalence, EC-based protein join, or independent KEGG/MetaCyc evidence |

**Never** convert these records into `ProteinReactionAssertion`. That record
requires an actual direct source annotation (see
[direct_evidence.md](direct_evidence.md)).

Rhea's v1 TSV query supplies a flat `chebi_ids` list. It does **not** contain
left/right sides or stoichiometric coefficients, so this enrichment API does not
manufacture `ReactionParticipant` objects. A future Rhea participant adapter
must extract those values from authoritative reaction-level data.

## ChEBI 2.0 REST — bounded subset, not a monthly release

ChEBI 2.0 public REST JSON has the fields `chebi_accession`, `name`,
`secondary_ids`, `chemical_data.formula`, `chemical_data.charge`, and
optional `default_structure.smiles`, `standard_inchi`, and
`standard_inchi_key`. The adapter was designed against the official
[ChEBI compound API](https://www.ebi.ac.uk/chebi/backend/api/public/compound/CHEBI:15377/?format=json)
(water, CHEBI:15377). ChEBI API calls do not require a key.

- Accept numeric or prefixed ChEBI IDs; cap a call at 100 **unique** IDs.
- A secondary ID resolves only when the returned compound explicitly lists it.
  A mismatched response is an error, not a guessed alias.
- Missing formula, charge, SMILES, InChI and InChIKey remain null; no structure
  is inferred. Structure strings are not chemically validated.
- The existing HTTP cache is used; stale-cache fallback is **rejected**.
  A missing/malformed/unavailable entry fails the batch; it is **not silently
  dropped** or mislabelled as a negative observation.
- Each compound has a `SourceRelease` containing the HTTP response SHA-256,
  actual `fetched_at` and source URL. The label
  `api-response-sha256:...` is **not an official ChEBI release**.
- Multiple lookups resolving to one primary ID are reconciled; conflicting
  chemical fields fail closed.

### Example

~~~python
from enzymeinfocollector.compound_sources import ChEBICollector
from enzymeinfocollector.sources import HttpCache, default_cache_dir, fetch_rhea

cache = HttpCache(default_cache_dir(), contact_email="your-email@example.org")
rhea_records, _ = fetch_rhea("1.1.1.1", cache)
compounds = ChEBICollector(cache).enrich_rhea_records(rhea_records)
for compound in compounds.compounds:
    print(compound.chebi_id, compound.name, compound.formula)
for link in compounds.rhea_links:
    print(link.rhea_id, link.reported_chebi_id, link.primary_chebi_id)
~~~

This is a **small, on-demand pilot**, not a complete or historical snapshot.
For production/reproducible large-scale use, implement the follow-up in
[issue #8](https://github.com/VasilijEpishkin/EnzymeInfoCollector/issues/8):
pin a monthly [ChEBI bulk release](https://www.ebi.ac.uk/chebi/downloads),
archive raw file hashes and attribution, import only needed IDs, report
unresolved IDs explicitly and align ChEBI/Rhea release dates.

## KEGG / MetaCyc interoperability through Rhea

The official Rhea TSV mapping files use **four** columns:

~~~text
RHEA_ID    DIRECTION    MASTER_ID    ID
10003      BI           10000        R02938
~~~

Directions are `UN`, `LR`, `RL` and `BI`. The parser reconciles
`RHEA_ID`, `MASTER_ID` and `DIRECTION` against the **same retrieved
Rhea directions snapshot**, rejects mismatches, and retains the original
directional ID. A mapping is not assumed to be one-to-one or chemically
identical (protonation and stereochemistry can differ).

~~~python
from enzymeinfocollector.reaction_xrefs import RheaCrossReferenceCollector
from enzymeinfocollector.sources import HttpCache, default_cache_dir

cache = HttpCache(default_cache_dir(), contact_email="your-email@example.org")
xrefs = RheaCrossReferenceCollector(cache).collect(["KEGG", "MetaCyc"])
for record in xrefs.crossrefs:
    print(record.reaction.rhea_id, record.database, record.external_id)
~~~

**Only Rhea's own TSV URLs are requested**:
`rhea2kegg_reaction.tsv`, `rhea2metacyc.tsv`, and
`rhea-directions.tsv`. No KEGG REST/FTP or BioCyc/MetaCyc API is accessed;
no KEGG/BioCyc-derived dump is packaged. Mapping snapshots have their own
SHA-256 and retrieval timestamps, **not** an invented official Rhea release.

Sources: [Rhea downloads](https://www.rhea-db.org/help/download),
[Rhea ID mapping](https://www.rhea-db.org/help/id-mapping),
[KEGG REST](https://www.kegg.jp/kegg/rest/),
[KEGG legal](https://www.kegg.jp/kegg/legal.html),
[BioCyc licensing](https://bioinformatics.ai.sri.com/ptools/licensing/all-reg.shtml).
Free website/API access does **not** imply bulk redistribution rights.
Direct KEGG/MetaCyc adapters remain gated by concrete scientific value and
license review ([issue #11](https://github.com/VasilijEpishkin/EnzymeInfoCollector/issues/11)).

## Next sources and gates

1. [SABIO-RK pilot, issue #9](https://github.com/VasilijEpishkin/EnzymeInfoCollector/issues/9):
   introduce a separate `Measurement`/`Observation` type for kcat, Km,
   pH, temperature, units, organism and literature. Link to a specific
   protein–Rhea pair **only** if both IDs are actually supplied and verified.
   Chemical structures must not override ChEBI.
2. [BRENDA, issue #10](https://github.com/VasilijEpishkin/EnzymeInfoCollector/issues/10):
   optional release-pinned JSON import after the measurement schema, with
   explicit license acceptance, attribution and EC/organism-scoped records.
3. GraphML/CX2 projection: export compounds, mappings, and measurement
   provenance as **typed** nodes/edges; do not increase the evidence level
   of curated UniProt→Rhea assertions based on cross-references.

## Verification and limitations

Offline: `python -m unittest discover -s tests -v`. Fixture tests cover
secondary IDs, missing chemistry, stale cache, malformed/mismatched payloads,
Rhea direction validation and provenance. Default CI never calls external APIs.

This PR does **not** yet implement a pinned ChEBI monthly bulk snapshot,
Rhea reaction participant stoichiometry, SABIO-RK, BRENDA, KEGG direct API,
pathway graphs, or GraphML/CX2 exports. Those are separately scoped follow-ups.

ChEBI and Rhea data are distributed under CC BY 4.0; attribution is required
when distributing their content. Review each source's current terms before
redistribution or commercial integration.
