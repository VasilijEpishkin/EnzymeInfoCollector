# EnzymeInfoCollector: follow-up discovery validation (2026-11-03)

**Scope:** Bounded live checks on main commit b5864d39bde4cba0f2a9cbe11181b5fa1c37b796.
This report measures *consistency with official UniProt/ExPASy/Rhea
annotations and correct CLI failure behavior*. It does NOT establish
experimental enzyme activity, global search recall, or historical
reproducibility. The original 2026-11-02 Phase-3 report remains intact.

## Previously unresolved scenarios

| Case | Result on 2026-11-03 | Observed data |
| --- | --- | --- |
| bacterial-pgk (EC 2.7.2.3, taxon 83333, reviewed) | **pass** | selected P0A799; direct RHEA:14801; one CATALYZES edge, no truncation |
| human-translocase (EC 7.2.2.13, taxon 9606, reviewed) | **selection_drift** | selected Q13733, not original P05023; reviewed human Na/K-transporting ATPase alpha-4; direct RHEA:18353 master and RHEA:18354 direction; 9 chemical participant rows; selection truncated |
| trembl-catalase (EC 1.11.1.6, taxon 61016, include unreviewed) | **pass** | A0A024QG36; direct RHEA:20309; one CATALYZES edge; candidate list truncated |

The Q13733 raw official UniProt JSON had SHA-256
cbe67e28d753c88fb3f5d43545352869babfe1a738f9d0522bd8d2757c2bbe39.
The live reference result carried the same source SHA. Q13733 and
P05023 are separate reviewed subunits of the human Na/K ATPase, and the
alternative candidate's direct source assertion was verified against
the current UniProt record. **Selection drift is not a false catalysis
edge** and must not be silently relabelled as the historical P05023
case. The source search returned truncated=true.

Official records:
- https://rest.uniprot.org/uniprotkb/Q13733.json
- https://rest.uniprot.org/uniprotkb/P05023.json
- https://rest.uniprot.org/uniprotkb/P0A799.json
- https://rest.uniprot.org/uniprotkb/A0A024QG36.json

Combined with the four already passed scenarios in the prior Phase-3
report, **6/7 original case expectations now pass** and the seventh is
a documented, source-verified alternative first candidate. This is
not a 7/7 pass against frozen candidate selections.

## Truncation: actual end-to-end CLI

~~~bash
python -m enzymeinfocollector.discover_cli \
  --ec 1.1.1.1 --max-proteins 1 --format reference-json \
  -o /tmp/enzyme-adh.json
~~~

CLI returned P11766, a reviewed human alcohol dehydrogenase class-3
with **7 directly annotated Rhea masters**. It explicitly printed:

    WARNING: UniProt search truncated to 1 candidate proteins; results do not cover all EC-classified proteins.

The reference discovery selection had truncated=true. Bounded search
TSV response SHA-256:
5c48028cb33ac8889b09cdf2dd0202c0ff2db524c9a9ad8a58edc5c6.
Official record: https://rest.uniprot.org/uniprotkb/P11766.json

The first result is **not** a scientific recommendation or proof of
EC-family coverage. Protein-to-Rhea edges still derive only from
UniProt catalytic-activity annotations.

## Exact taxon versus descendants

Using EC 2.7.2.3, parent taxon E. coli 562, reviewed only:

| Scope | Result |
| --- | --- |
| exact (organism_id:562) | no reviewed candidates, explicit CLI diagnostic; no result file |
| descendants (taxonomy_id:562) | P0A799 of descendant taxon 83333 (E. coli K12), direct RHEA:14801; truncated=true |

A separate bounded descendants query selecting up to two proteins
returned P0A799 (83333) and P0A7A0 (199310), still truncated.
Search response SHA-256:
73ec2d2418a366bbe372729597e55b8e7ceb65c5a8b6ccd8787c0a5d3167a85e.
This demonstrates query behavior, not inferred taxonomy from names.

## ExPASy transferred/deleted EC records

Live official ExPASy ENZYME catalogue digest (text SHA-256):
6e1f553fcaa15ba7018344d4181012002547cf15febeb9d3a03dacdd75963001.
Parsed: 8,472 records = 6,980 active + 1,314 transferred
+ 178 deleted.

- The exact name 'Transferred entry: 1.1.1.303 and 1.1.1.304'
  maps to obsolete EC 1.1.1.5 and the name resolver refuses it,
  reporting transferred status. No output file.
- 'Deleted entry' resolves to 178 *same-name* records, so the
  name resolver rejects the ambiguity instead of selecting an
  arbitrary deleted EC. No output file.
- This does **not** establish that every numeric --ec query is
  checked against ExPASy status: direct numeric EC queries are
  validated syntactically and may be issued without an active
  ExPASy catalogue match. No EC equivalence may create catalysis.

Source: https://ftp.expasy.org/databases/enzyme/enzyme.dat

## Exports and scientific metadata

Using the same successful P11766 reference JSON, ran graph_cli to
Cytoscape JSON and GraphML. Both exports have **25 nodes, 46 edges**;
10 edges represent directly annotated master/directional CATALYZES
assertions. Every graph node and edge attribute matched after
type-aware GraphML decoding (GraphML schema uses string attributes,
and the coefficient '1' must not be parsed into a number). The
reference SHA-256 and full discovery/source provenance are equal
across both formats. A scoped GraphML export for P11766 preserves
scope = {"protein":"P11766"}. A deterministic offline regression
test now checks equivalent full-node and full-edge metadata.

## Fail-closed unsupported and unknown cases

- P00533 (EGFR): live reference_cli returns
  'Rhea RDF chebi binding is missing or not uri'. Exit 1; no file.
  The unavailable/nonstandard compound cannot be silently
  replaced with a guessed ChEBI identifier.
- P00690 (pig alpha-amylase): no direct numeric Rhea link;
  live reference_cli exits 1 with clear no-direct annotation
  diagnostic and no result file. This is **unknown**, not inactive.

## Release decision and limits

No new reproducible defect in the evidence extraction, graph
projection or chemical participant parser was observed in this
bounded follow-up. This is **not** a licence to claim global
completeness, wet-lab precision/recall or immutable upstream
versions. Small new offline regressions cover GraphML/Cytoscape
round-trip parity and the deleted-entry status guard.

The first-result selection bias (Q13733 vs historical P05023)
is a scientifically important UX constraint. Do not create a
custom biological ranking without an explicit, grounded
objective; advanced users can supply accession-first queries,
or bounded multi-candidate discovery with the truncation flag.
