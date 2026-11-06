"""Offline human-readable report for existing v2 reference JSON (no HTTP)."""
from __future__ import annotations

import re
from typing import Any
from .sources import DataSourceError

RHEA = re.compile(r"RHEA:[1-9][0-9]*$")
CHEBI = re.compile(r"CHEBI:[1-9][0-9]*$")
UNIPROT = re.compile(r"[A-Z0-9]+(?:-[0-9]+)?$")
MARKDOWN = re.compile(r"([\\*_{}\[\]()#+.!|<>~])")


def obj(value: Any, label: str) -> dict:
    if not isinstance(value, dict):
        raise DataSourceError(f"{label} must be an object")
    return value


def rows(value: Any, label: str) -> list:
    if not isinstance(value, list):
        raise DataSourceError(f"{label} must be a list")
    return value


def ident(value: Any, pattern: re.Pattern, label: str) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise DataSourceError(f"Invalid {label}: {value!r}")
    return value


def safe(value: Any) -> str:
    """Escape untrusted source text in Markdown including inline HTML and pipes."""
    if value is None or value == "":
        return "не указано"
    if isinstance(value, (dict, list, tuple)):
        raise DataSourceError("Expected scalar report value")
    text = " ".join(str(value).split())
    return MARKDOWN.sub(lambda m: chr(92) + m.group(1), text).replace(
        chr(96), chr(92) + chr(96)) or "не указано"


def source_info(value: Any) -> str:
    if value is None:
        return "снимок источника не указан"
    record = obj(value, "source release")
    return "; ".join(f"{key}: {safe(record[key])}" for key in (
        "database", "release", "retrieved_at", "sha256", "source_url"
    ) if record.get(key) is not None) or "нет метаданных источника"


def build_evidence_report(value: dict) -> str:
    document = obj(value, "reference")
    if document.get("schema_version") != "2.0-reference-preview":
        raise DataSourceError("Expected 2.0-reference-preview JSON")
    prov = obj(document.get("provenance"), "provenance")
    proteins = rows(document.get("proteins"), "proteins")
    if not proteins:
        raise DataSourceError("Reference contains no proteins")
    reactions = {}
    for raw in rows(document.get("reactions"), "reactions"):
        reaction = obj(raw, "reaction")
        master = ident(reaction.get("master_id"), RHEA, "master Rhea ID")
        if master in reactions:
            raise DataSourceError(f"Duplicate Rhea master {master}")
        rows(reaction.get("compound_membership"), "compound_membership")
        reactions[master] = reaction
    compounds = {}
    for raw in rows(document.get("compounds"), "compounds"):
        compound = obj(raw, "compound")
        chem_id = ident(compound.get("chebi_id"), CHEBI, "ChEBI ID")
        if chem_id in compounds:
            raise DataSourceError(f"Duplicate ChEBI ID: {chem_id}")
        compounds[chem_id] = compound
    unresolved = obj(document.get("unresolved"), "unresolved")

    records = {}
    for raw in proteins:
        protein = obj(raw, "protein")
        acc = ident(protein.get("accession"), UNIPROT, "UniProt accession")
        if acc in records:
            raise DataSourceError(f"Duplicate accession: {acc}")
        for item in rows(protein.get("direct_assertions"), "direct_assertions"):
            assertion = obj(item, "direct assertion")
            variant = obj(assertion.get("reaction"), "reaction variant")
            master = ident(variant.get("master_id"), RHEA, "asserted Rhea master")
            ident(variant.get("rhea_id"), RHEA, "asserted Rhea direction")
            if master not in reactions:
                raise DataSourceError(f"Direct assertion references missing Rhea record: {master}")
            rows(assertion.get("evidence"), "evidence")
        records[acc] = protein

    discovery = prov.get("discovery")
    if discovery is not None:
        discovery = obj(discovery, "discovery")
    names = {}
    if discovery:
        selection = discovery.get("selection")
        if isinstance(selection, dict):
            candidates = selection.get("candidate_pool", selection.get("selected", []))
            if isinstance(candidates, list):
                names = {c["accession"]: c for c in candidates
                         if isinstance(c, dict) and isinstance(c.get("accession"), str)}

    out = ["# Доказательные аннотации ферментов", "",
           "Сводка из сохранённого JSON; **новых запросов к источникам не выполнялось**.",
           "**EC — классификация, а не доказательство катализа** конкретным белком.",
           "Отсутствие прямой аннотации означает «неизвестно», а не отсутствие активности.", ""]
    if discovery:
        sel = discovery.get("selection", {})
        request = discovery.get("request", {})
        out.extend(["## Выборка и ограничения", ""])
        if isinstance(request, dict):
            out.append(f"- Запрос: {safe(request.get('input'))}; EC: {safe(request.get('ec'))}.")
        if isinstance(sel, dict):
            out.append(f"- Режим выбора: {safe(sel.get('selection_mode'))}.")
            if sel.get("truncated") is True:
                out.append("- **Выдача UniProt усечена**; кандидаты не представляют полный EC-класс.")
            snapshot = sel.get("source")
            if isinstance(snapshot, dict):
                out.append("- Снимок поиска: " + source_info({
                    "retrieved_at": snapshot.get("fetched_at"),
                    "sha256": snapshot.get("sha256"),
                    "source_url": snapshot.get("url"),
                }) + ".")
        unknown = discovery.get("without_direct_rhea_annotation")
        if isinstance(unknown, list) and unknown:
            out.append("- Нет прямой Rhea-аннотации: " +
                       ", ".join(safe(x) for x in sorted(unknown)) +
                       " (не отрицательный результат).")
        out.append("")

    master_ids = set()
    for accession, protein in sorted(records.items()):
        out.extend([f"## UniProt {safe(accession)}", ""])
        candidate = names.get(accession, {})
        for field, label in (("protein_name", "Название"), ("gene_names", "Гены")):
            text = candidate.get(field) or protein.get(field)
            if text:
                out.append(f"- {label} из источника: {safe(text)}.")
        reviewed = protein.get("reviewed")
        status = "Swiss-Prot (reviewed)" if reviewed is True else (
            "unreviewed" if reviewed is False else "неизвестно")
        out.append(f"- UniProt: {status}; таксон: {safe(protein.get('taxon_id'))}; "
                   f"организм: {safe(protein.get('organism_name'))}.")
        ecs = rows(protein.get("ec_numbers", []), "ec_numbers")
        out.append("- EC (классификация): " +
                   (", ".join(safe(x) for x in sorted(ecs)) if ecs else "не указано") + ".")
        out.append(f"- Версия записи: {safe(protein.get('entry_version'))}; "
                   f"версия последовательности: {safe(protein.get('sequence_version'))}.")
        out.append("- Источник: " + source_info(protein.get("source_release")) + ".")
        assertions = rows(protein["direct_assertions"], "direct_assertions")
        out.extend(["", f"### Прямые UniProt–Rhea аннотации ({len(assertions)})", ""])
        if not assertions:
            out.extend(["В этом снимке нет прямых Rhea-аннотаций; "
                        "**это не доказательство отсутствия активности**.", ""])
        for item in sorted(assertions, key=lambda x: (
            x["reaction"]["master_id"], x["reaction"]["rhea_id"],
            str(x.get("annotation_id") or ""),
        )):
            reaction = item["reaction"]
            master = reaction["master_id"]
            master_ids.add(master)
            out.extend([f"#### {safe(master)} · направленный {safe(reaction['rhea_id'])}", "",
                        f"- Направление: {safe(reaction.get('direction'))}.",
                        f"- ID аннотации: {safe(item.get('annotation_id'))}.",
                        "- Источник аннотации: " + source_info(item.get("source_release")) + ".",
                        "- Доказательства **только данного направленного утверждения**:"])
            evidence = sorted(item["evidence"], key=lambda x: (
                str(x.get("eco_code") or ""), str(x.get("citation_ids") or "")
            ))
            if not evidence:
                out.append("  - Не указаны; это не отрицательное доказательство.")
            for proof in evidence:
                proof = obj(proof, "evidence item")
                cites = proof.get("citation_ids") or []
                if not isinstance(cites, (list, tuple)):
                    raise DataSourceError("citation_ids must be an array")
                out.append("  - ECO: " + safe(proof.get("eco_code")) +
                           "; публикации: " +
                           (", ".join(safe(x) for x in sorted(cites)) if cites else "не указаны") +
                           "; источник: " + safe(proof.get("source_database")) +
                           "; метод: " + safe(proof.get("assertion_method")) +
                           "; statement: " + safe(proof.get("source_statement_iri")) + ".")
            out.append("")

    out.extend(["## Участники Rhea master-реакций", "",
                "**Left/right описывают стороны уравнения Rhea и не обязательно "
                "физиологические субстраты/продукты.**", ""])
    for master in sorted(master_ids):
        reaction = reactions[master]
        out.extend([f"### {safe(master)}", "",
                    f"- Уравнение Rhea: {safe(reaction.get('equation'))}.",
                    f"- Статус: {safe(reaction.get('status'))}.", "",
                    "| Сторона | Коэффициент | ChEBI Rhea | ChEBI primary | Соединение / формула | Статус |",
                    "| --- | --- | --- | --- | --- | --- |"])
        members = [obj(m, "Rhea participant") for m in reaction["compound_membership"]]
        members.sort(key=lambda m: (
            {"left": 0, "right": 1}.get(str(m.get("side")), 2),
            str(m.get("reported_chebi_id") or ""), str(m.get("coefficient") or ""),
        ))
        for member in members:
            reported = ident(member.get("reported_chebi_id"), CHEBI, "Rhea participant ChEBI ID")
            primary = member.get("primary_chebi_id")
            if primary is not None:
                ident(primary, CHEBI, "primary ChEBI ID")
                if primary not in compounds:
                    raise DataSourceError(f"Resolved ChEBI missing from compounds: {primary}")
            if member.get("resolution_status") == "resolved" and primary is None:
                raise DataSourceError(f"Resolved participant has no primary ID: {reported}")
            chem = compounds.get(primary, {})
            label = " / ".join(str(x) for x in (chem.get("name"), chem.get("formula")) if x)
            fields = (member.get("side"), member.get("coefficient"),
                      reported, primary, label or None, member.get("resolution_status"))
            out.append("| " + " | ".join(safe(x) for x in fields) + " |")
        if not members:
            out.append("| не указано | не указано | не указано | не указано | не указано | участников нет в снимке |")
        out.append("")

    out.extend(["## Неполные данные и происхождение", ""])
    for key, label in (("missing_rhea_master_ids", "Отсутствующие Rhea master"),
                       ("unresolved_chebi_ids", "Неразрешённые ChEBI")):
        items = rows(unresolved.get(key, []), key)
        out.append(f"- {label}: " +
                   (", ".join(safe(x) for x in sorted(items)) if items else "не обнаружены") + ".")
    out.extend(["- Неподдерживаемые RDF-участники в строгом режиме могут прервать сбор; "
                "отчёт не устанавливает полноту данных вне сохранённого набора.",
                "- Хеш полученного HTTP-ответа не равен архиву официального релиза.", "",
                f"- UniProt release: {safe(prov.get('uniprot_release'))}.",
                "- Rhea directions: " + source_info(prov.get("rhea_directions")) + ".",
                "- Rhea reactions: " + source_info(prov.get("rhea_reaction_records")) + ".",
                f"- SHA-256 reference: {safe(document.get('semantic_sha256'))}.", ""])
    for snap in sorted(prov.get("rhea_participant_queries", []),
                       key=lambda x: str(x.get("sha256") or "")):
        out.append("- Rhea RDF: " + source_info(snap) + ".")
    out.append("")
    return "\n".join(out)
