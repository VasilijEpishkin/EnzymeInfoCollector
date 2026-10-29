# Live-data spike: UniProtKB ↔ Rhea (29 October 2026)

Status: **limited live-data validation**, not a benchmark of biological truth.
Data were retrieved from public APIs/files. This is not a claim that absent
annotations are experimentally negative.

## Method

- UniProtKB REST JSON `/uniprotkb/{accession}.json`: extract only
  `comments[commentType == "CATALYTIC ACTIVITY"].reaction.reactionCrossReferences`
  where `database == "Rhea"` and ID matches `RHEA:<integer>`.
- Read ECO and `source`/`id` from the **same reaction's** `evidences`.
  PubMed IDs are citations; rule/database IDs are non-publication source
  references. Evidence may be missing, and manually reviewed does **not**
  mean experimentally demonstrated.
- Rhea `rhea-directions.tsv`: map UN/LR/RL/BI IDs to one master Rhea ID.
- Rhea `rhea2uniprot_sprot.tsv`: compare only **master IDs**, not raw
  directional identifiers, because this table can include both master and
  directional rows for a protein.
- Explicit direct annotations are kept separate from EC classifications.

## Recorded source versions and checksums

| Source | Version/last-modified | SHA-256 |
| --- | --- | --- |
| UniProtKB REST | `X-UniProt-Release: 2026_03` (`02-September-2026`) | per-entry raw body hash is emitted by the validation script |
| [Rhea directions](https://ftp.expasy.org/databases/rhea/tsv/rhea-directions.tsv) | `Wed, 02 Sep 2026 14:00:00 GMT` | `deae1911da372c70ee8fc97992809c7923e63b288a8702fc4396edcb2633fb04` |
| [Rhea Swiss-Prot mappings](https://ftp.expasy.org/databases/rhea/tsv/rhea2uniprot_sprot.tsv) | `Wed, 02 Sep 2026 14:00:00 GMT` | `ff2e1038df2352d4ce4f3e9ae840c350211d8193e79b6a0a2d29fc1c2f392def` |

Rhea files did not expose a verified semantic release label in this check.
The **file checksums and Last-Modified values**, not an invented release ID,
pin the observed snapshot. The Rhea Swiss-Prot TSV had 400,643 data rows.

## Results

| UniProt accession | EC(s) in UniProtKB | REST direct master reactions | Rhea TSV master reactions | Master-ID mismatches |
| --- | --- | ---: | ---: | --- |
| [P00330](https://rest.uniprot.org/uniprotkb/P00330.json) | 1.1.1.1, 1.1.1.54, 1.1.1.78 | 12 | 12 | 0 |
| [P00331](https://rest.uniprot.org/uniprotkb/P00331.json) | 1.1.1.1 | 5 | 5 | 0 |
| [P04040](https://rest.uniprot.org/uniprotkb/P04040.json) | 1.11.1.6 | 1 | 1 | 0 |
| [P00734](https://rest.uniprot.org/uniprotkb/P00734.json) | 3.4.21.5 | 0 | 0 | 0 |
| [P00519](https://rest.uniprot.org/uniprotkb/P00519.json) | 2.7.10.2 | 1 | 1 | 0 |

For P00330, the **raw** Rhea mapping had 24 rows (master plus directional
variants) while REST exposed 12 master reaction IDs. Comparing raw IDs
would falsely flag 12 missing UniProt annotations. For P00331 the
corresponding counts were 9 mapping rows vs 5 master reactions.

P00734 has a catalytic-activity comment without a Rhea cross-reference.
That comment must not be treated as a negative catalytic observation.
P00519 contains `RHEA-COMP` references alongside `RHEA` IDs; these are
compartment references, not Rhea reaction IDs.

## Reproduce

Run the **opt-in network validation**, not part of CI:

```bash
python -m enzymeinfocollector.validate_sources --refresh
```

The script emits a JSON report including entry/sequence versions, evidence
counts, source checksums and per-accession master-ID differences. It uses
the existing HTTP cache and preserves the legacy CLI.

## Remaining gates

1. Validate UniProt SPARQL RDF statement-level attribution and join
   annotations to REST comment-level evidence without falsely claiming
   that a JSON comment index is a persistent RDF statement URI.
2. Broaden to more organisms and TrEMBL; the Swiss-Prot mapping alone is
   insufficient to characterize unreviewed entries.
3. Document and verify current UniProt/Rhea licensing and redistribution
   terms before bundling bulk snapshots or exposing a hosted service.
4. Test more missing/ambiguous ECO/citation cases and release changes.

The present 5-entry sample establishes **technical feasibility**, not
population-wide completeness or experimental accuracy.
