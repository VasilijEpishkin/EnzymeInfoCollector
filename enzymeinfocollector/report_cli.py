"""Offline, source-grounded Markdown view of a verified v2 reference result.

The report reads existing assertions only. An EC classification is never
used to infer a Protein -> Reaction link. No HTTP requests are made.
"""

from __future__ import annotations

import argparse
from html import escape
import json
from pathlib import Path
import re
import sys
from typing import Any, Mapping

from .sources import DataSourceError, _atomic_write

_SCHEMA = "2.0-reference-preview"
_MAX_INPUT_BYTES = 32 * 1024 * 1024
_UNIPROT = re.compile(r"[A-Z0-9]+(?:-[0-9]+)?\Z")
_RHEA = re.compile(r"RHEA:[1-9][0-9]*\Z")
_CHEBI = re.compile(r"CHEBI:[1-9][0-9]*\Z")
_MD = re.compile(r"([\\`*_{}\[\]()|>#!])")


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise DataSourceError(f"{label} must be an object")
    return value


def _list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise DataSourceError(f"{label} must be a list")
    return value


def _id(value: Any, pattern: re.Pattern[str], label: str) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise DataSourceError(f"Invalid {label}: {value!r}")
    return value


def _cell(value: Any) -> str:
    """Escape untrusted source text in Markdown (including raw HTML and links)."""
    if value is None or value == "":
        return "не указано"
    if isinstance(value, (dict, list, tuple)):
        value = json.dumps(value, ensure_ascii=False, sort_keys=True)
    value = " ".join(str(value).split())
    return _MD.sub(r"\\\1", escape(value, quote=False))


def _source(value: Any) -> str:
    data = _object(value, "source_release")
    return " / ".join(_cell(data.get(key)) for key in ("database", "release"))


def _source_rows(reference: Mapping[str, Any]) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    provenance = _object(reference["provenance"], "provenance")
    for key in ("rhea_directions", "rhea_reaction_records"):
        if provenance.get(key) is not None:
            sources.append(dict(_object(provenance[key], key)))
    for key in ("rhea_participant_queries", "chebi_snapshots"):
        for item in _list(provenance.get(key, []), key):
            sources.append(dict(_object(item, key)))
    for protein in _list(reference["proteins"], "proteins"):
        sources.append(dict(_object(protein["source_release"], "protein source")))
        for assertion in _list(protein["direct_assertions"], "direct_assertions"):
            sources.append(dict(_object(assertion["source_release"], "assertion source")))
    for compound in _list(reference["compounds"], "compounds"):
        sources.append(dict(_object(compound["source_release"], "compound source")))
    unique: dict[str, dict[str, Any]] = {}
    for item in sources:
        key = json.dumps(item, ensure_ascii=False, sort_keys=True)
        unique[key] = item
    return [unique[k] for k in sorted(unique)]


def render_report(reference: Mapping[str, Any], *, accession: str | None = None) -> str:
    """Render existing v2 annotations, participants, and provenance deterministically.

    The caller is responsible for having obtained a source-backed reference
    dataset. This function does not certify biochemical truth or completeness.
    """
    if not isinstance(reference, dict) or reference.get("schema_version") != _SCHEMA:
        raise DataSourceError("Expected a 2.0-reference-preview JSON document")
    _object(reference.get("provenance"), "provenance")
    proteins = _list(reference.get("proteins"), "proteins")
    if not proteins:
        raise DataSourceError("The reference has no proteins")
    indexed: dict[str, Mapping[str, Any]] = {}
    for item in proteins:
        protein = _object(item, "protein")
        name = _id(protein.get("accession"), _UNIPROT, "UniProt accession")
        if name in indexed:
            raise DataSourceError(f"Duplicate UniProt accession: {name}")
        _list(protein.get("direct_assertions"), "direct_assertions")
        indexed[name] = protein
    if accession is not None:
        accession = _id(accession, _UNIPROT, "UniProt accession")
        if accession not in indexed:
            raise DataSourceError(f"UniProt accession not in reference: {accession}")
        selected = [accession]
    else:
        selected = sorted(indexed)

    reactions: dict[str, Mapping[str, Any]] = {}
    for item in _list(reference.get("reactions"), "reactions"):
        row = _object(item, "reaction")
        master = _id(row.get("master_id"), _RHEA, "Rhea master")
        if master in reactions:
            raise DataSourceError(f"Duplicate Rhea master: {master}")
        reactions[master] = row
    compounds: dict[str, Mapping[str, Any]] = {}
    for item in _list(reference.get("compounds"), "compounds"):
        row = _object(item, "compound")
        chebi = _id(row.get("chebi_id"), _CHEBI, "ChEBI")
        if chebi in compounds:
            raise DataSourceError(f"Duplicate ChEBI record: {chebi}")
        compounds[chebi] = row

    lines = [
        "# Аннотации ферментов — проверяемая сводка", "",
        "Отчёт построен **офлайн** по существующему reference JSON. "
        "Приведены только прямые UniProtKB–Rhea аннотации. "
        "EC — классификация, **не доказательство** связи конкретного белка с реакцией.",
        "",
        "**Ограничения:** отсутствие прямой аннотации, ECO или PMID означает "
        "«нет сведений в этом результате», а не доказательство отсутствия активности. "
        "Левая/правая стороны Rhea — стороны уравнения, не обязательно "
        "физиологические субстраты/продукты.",
        "",
    ]

    provenance = _object(reference["provenance"], "provenance")
    discovery = provenance.get("discovery")
    candidates_by_accession: dict[str, Mapping[str, Any]] = {}
    if discovery is not None:
        discovery = _object(discovery, "discovery")
        selection = discovery.get("selection") or {}
        if not isinstance(selection, dict):
            raise DataSourceError("discovery.selection must be an object")
        lines += ["## Условия выбора и полнота", ""]
        raw_candidates = selection.get("candidate_pool") or selection.get("selected", [])
        if isinstance(raw_candidates, list):
            for candidate in raw_candidates:
                if isinstance(candidate, dict):
                    candidate_acc = candidate.get("accession")
                    if isinstance(candidate_acc, str) and _UNIPROT.fullmatch(candidate_acc):
                        candidates_by_accession[candidate_acc] = candidate
        if selection.get("truncated") is True or discovery.get("truncated") is True:
            lines.append("- **Выборка UniProt-кандидатов ограничена лимитом.** Это не полный список ферментов данного EC.")
        selected_candidates = selection.get("selected")
        if isinstance(selected_candidates, list):
            selected_ids = [
                row.get("accession") if isinstance(row, dict) else row
                for row in selected_candidates
            ]
            if selected_ids:
                lines.append("- Выбранные кандидаты UniProt: " +
                             _cell(", ".join(str(item) for item in selected_ids)))
        elif isinstance(selected_candidates, str):
            lines.append("- Выбранный accession: " + _cell(selected_candidates))
        search_source = selection.get("source")
        if isinstance(search_source, dict):
            lines.append("- Снимок поиска UniProt (не архивный релиз): " +
                         "; ".join(label + ": " + _cell(search_source.get(key))
                                   for label, key in (
                                       ("дата", "fetched_at"),
                                       ("SHA-256", "sha256"),
                                       ("URL", "url"),
                                   )))
        missing_links = discovery.get("without_direct_rhea_annotation", [])
        for identifier in sorted(_list(missing_links, "without_direct_rhea_annotation")):
            lines.append("- " + _cell(identifier) + ": прямая Rhea-аннотация не найдена в выборке; активность неизвестна.")
        lines.append("")

    lines += ["## Белки и прямые аннотации", ""]
    for acc in selected:
        protein = indexed[acc]
        lines += ["### UniProt " + _cell(acc), ""]
        candidate = candidates_by_accession.get(acc)
        if candidate is not None:
            if candidate.get("protein_name"):
                lines.append("- Название (предпросмотр UniProt, не доказательство активности): " +
                             _cell(candidate["protein_name"]))
            if candidate.get("gene_names"):
                lines.append("- Гены (предпросмотр UniProt): " + _cell(candidate["gene_names"]))
        for label, key in (
            ("Название", "protein_name"), ("Ген", "gene_name"),
            ("Организм", "organism_name"), ("Таксон NCBI", "taxon_id"),
            ("Reviewed", "reviewed"), ("Версия записи", "entry_version"),
            ("Версия последовательности", "sequence_version"),
            ("Классификация EC (не связь катализа)", "ec_numbers"),
        ):
            if key == "reviewed":
                value = "Swiss-Prot" if protein.get(key) is True else (
                    "TrEMBL" if protein.get(key) is False else None)
            else:
                value = protein.get(key)
            lines.append("- " + label + ": " + _cell(value))
        lines += ["- Источник записи: " + _source(protein["source_release"]), ""]

        assertions = _list(protein["direct_assertions"], "direct_assertions")
        if not assertions:
            lines += [
                "**Прямые UniProt–Rhea аннотации в этом результате отсутствуют.** "
                "Это неизвестный статус активности, не отрицательное доказательство.", ""
            ]
            continue
        def key(item: Any) -> tuple[str, str, str]:
            row = _object(item, "assertion")
            reaction = _object(row.get("reaction"), "assertion.reaction")
            return (str(reaction.get("master_id")), str(reaction.get("rhea_id")),
                    str(row.get("annotation_id") or ""))
        for assertion in sorted(assertions, key=key):
            record = _object(assertion, "assertion")
            if record.get("accession") != acc:
                raise DataSourceError(f"Direct assertion accession disagrees with protein: {acc}")
            variant = _object(record.get("reaction"), "assertion.reaction")
            master = _id(variant.get("master_id"), _RHEA, "master Rhea ID")
            directional = _id(variant.get("rhea_id"), _RHEA, "directional Rhea ID")
            if master not in reactions:
                raise DataSourceError(f"Missing Rhea master for direct assertion: {master}")
            rhea = reactions[master]
            lines += [
                "#### " + _cell(master) + " / " + _cell(directional), "",
                "- Направление из UniProt: " + _cell(variant.get("direction")),
                "- Уравнение Rhea: " + _cell(rhea.get("equation")),
                "- Статус записи Rhea: " + _cell(rhea.get("status")),
                "- Источник утверждения: " + _source(record["source_release"]),
                "- ID аннотации: " + _cell(record.get("annotation_id")), "",
                "**Доказательства именно этой направленной аннотации:**", "",
                "| ECO | Метод | Источник | Публикации / ссылки |",
                "| --- | --- | --- | --- |",
            ]
            evidence = _list(record.get("evidence"), "evidence")
            if not evidence:
                raise DataSourceError(f"Direct assertion lacks evidence: {acc} / {directional}")
            for proof in sorted(evidence, key=lambda p: json.dumps(p, sort_keys=True, ensure_ascii=False)):
                item = _object(proof, "evidence entry")
                citations = _list(item.get("citation_ids", []), "citation_ids")
                lines.append("| " + " | ".join((
                    _cell(item.get("eco_code")), _cell(item.get("assertion_method")),
                    _cell(item.get("source_database")),
                    _cell(", ".join(sorted(str(c) for c in citations)) if citations else None),
                )) + " |")
            lines += ["", "**Участники уравнения Rhea (не автоматически физиологические роли):**", "",
                      "| Сторона | Коэффициент | Исходный ChEBI | Основной ChEBI | Соединение | Статус |",
                      "| --- | --- | --- | --- | --- | --- |"]
            memberships = _list(rhea.get("compound_membership"), "compound_membership")
            for member in sorted(memberships, key=lambda m: (
                str(m.get("side")), str(m.get("reported_chebi_id")),
                str(m.get("coefficient")), str(m.get("primary_chebi_id")))):
                m = _object(member, "reaction participant")
                reported = _id(m.get("reported_chebi_id"), _CHEBI, "reported ChEBI")
                primary = m.get("primary_chebi_id")
                if primary is not None:
                    _id(primary, _CHEBI, "primary ChEBI")
                state = m.get("resolution_status")
                if (primary is None and state != "unresolved") or (
                    primary is not None and state != "resolved"
                ):
                    raise DataSourceError(f"Conflicting ChEBI resolution status: {reported}")
                compound = compounds.get(primary) if primary is not None else None
                if primary and compound is None:
                    raise DataSourceError(f"Resolved ChEBI record absent: {primary}")
                lines.append("| " + " | ".join(_cell(value) for value in (
                    m.get("side"), m.get("coefficient"), reported, primary,
                    compound.get("name") if compound else None,
                    m.get("resolution_status"),
                )) + " |")
            if not memberships:
                lines.append("| не указано | не указано | не указано | не указано | не указано | нет данных |")
            lines.append("")

    unresolved = _object(reference.get("unresolved", {}), "unresolved")
    lines += ["## Неопределённость и ограничения источников", ""]
    for field, label in (
        ("missing_rhea_master_ids", "Отсутствующие записи Rhea"),
        ("unresolved_chebi_ids", "Неразрешённые ChEBI ID"),
    ):
        identifiers = _list(unresolved.get(field, []), field)
        lines.append("- " + label + ": " + _cell(", ".join(sorted(map(str, identifiers))) if identifiers else "нет в результате"))
    lines += [
        "- Неподдерживаемые RDF-участники могут приводить к отказу строгого "
        "сборщика; успешный JSON не подтверждает полноту всех реакций.",
        "- SHA-256 HTTP-ответа и время загрузки **не равнозначны** "
        "архивному официальному релизу всей базы данных.",
        "- Не выполняется дополнительная проверка химического баланса или активности фермента.",
        "",
        "## Происхождение данных", "",
        "| База | Релиз / обозначение | Получено | SHA-256 HTTP/файла | URL источника |",
        "| --- | --- | --- | --- | --- |",
    ]
    for src in _source_rows(reference):
        lines.append("| " + " | ".join(_cell(src.get(field))
                      for field in ("database", "release", "retrieved_at", "sha256", "source_url")) + " |")
    lines += ["", "- UniProtKB release: " + _cell(provenance.get("uniprot_release")),
              "- Семантический SHA-256 reference JSON: " + _cell(reference.get("semantic_sha256")),
              "- Атрибуция: " + _cell(provenance.get("attribution")), ""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Existing 2.0-reference-preview JSON")
    parser.add_argument("--accession", help="Report only one accession present in the JSON")
    parser.add_argument("-o", "--output", type=Path, help="Write Markdown atomically (default: stdout)")
    args = parser.parse_args(argv)
    try:
        if args.input.stat().st_size > _MAX_INPUT_BYTES:
            raise DataSourceError("Reference JSON exceeds the 32 MB input limit")
        reference = json.loads(args.input.read_text(encoding="utf-8"))
        result = render_report(reference, accession=args.accession)
        if args.output is not None:
            _atomic_write(args.output, result.encode("utf-8"))
        else:
            print(result, end="")
    except (DataSourceError, OSError, UnicodeError, ValueError, TypeError, KeyError) as error:
        print(f"Evidence report failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
