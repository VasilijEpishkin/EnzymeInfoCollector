"""Evidence-aware scientific data contracts (v2 groundwork).

These immutable records do not change the legacy EC-centric collector or its
JSON/CSV/FASTA outputs. Import adapters and graph projections are separate
follow-up work. An EC association is never evidence of a Protein–Reaction link.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from hashlib import sha256
import re


def _nonempty(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def _identifier(value: str, namespace: str) -> str:
    value = _nonempty(value, f"{namespace} ID")
    match = re.fullmatch(rf"(?:{namespace}:)?([1-9][0-9]*)", value, re.IGNORECASE)
    if match is None:
        raise ValueError(f"Invalid {namespace} ID: {value!r}")
    return f"{namespace}:{match.group(1)}"


def _accession(value: str) -> str:
    accession = _nonempty(value, "UniProt accession")
    if any(character.isspace() for character in accession):
        raise ValueError("UniProt accession must not contain whitespace")
    return accession


class ReactionDirection(str, Enum):
    UNDEFINED = "undefined"
    LEFT_TO_RIGHT = "left_to_right"
    RIGHT_TO_LEFT = "right_to_left"
    BIDIRECTIONAL = "bidirectional"


class ReactionSide(str, Enum):
    LEFT = "left"
    RIGHT = "right"


class AssertionMethod(str, Enum):
    MANUAL = "manual"
    AUTOMATIC = "automatic"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class SourceRelease:
    """Source identity for an annotation/snapshot, not merely a query replay."""

    database: str
    release: str
    retrieved_at: str
    sha256: str | None = None
    source_url: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "database", _nonempty(self.database, "database"))
        object.__setattr__(self, "release", _nonempty(self.release, "release"))
        timestamp = _nonempty(self.retrieved_at, "retrieved_at")
        try:
            parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("retrieved_at must be an ISO-8601 timestamp") from exc
        if parsed.utcoffset() is None:
            raise ValueError("retrieved_at must include a timezone")
        if self.sha256 is not None and re.fullmatch(r"[a-fA-F0-9]{64}", self.sha256) is None:
            raise ValueError("sha256 must contain 64 hexadecimal characters")
        if self.source_url is not None:
            _nonempty(self.source_url, "source_url")


@dataclass(frozen=True, slots=True)
class ProteinSequenceSnapshot:
    """Accession is an entry ID; version and hash identify the sequence state."""

    accession: str
    sequence: str
    sequence_version: int
    source_release: SourceRelease
    entry_version: int | None = None
    isoform: str = "canonical"

    def __post_init__(self) -> None:
        object.__setattr__(self, "accession", _accession(self.accession))
        if not isinstance(self.source_release, SourceRelease):
            raise TypeError("source_release must be a SourceRelease")
        if type(self.sequence_version) is not int or self.sequence_version < 1:
            raise ValueError("sequence_version must be a positive integer")
        if self.entry_version is not None and (type(self.entry_version) is not int or self.entry_version < 1):
            raise ValueError("entry_version must be a positive integer")
        if not isinstance(self.sequence, str) or re.fullmatch(r"[A-Z]+", self.sequence) is None:
            raise ValueError("sequence must contain uppercase amino-acid letters")
        object.__setattr__(self, "isoform", _nonempty(self.isoform, "isoform"))

    @property
    def sequence_sha256(self) -> str:
        return sha256(self.sequence.encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True)
class Reaction:
    """Master Rhea identity; no biological direction is implied."""

    master_id: str
    equation: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "master_id", _identifier(self.master_id, "RHEA"))
        if self.equation is not None:
            _nonempty(self.equation, "equation")


@dataclass(frozen=True, slots=True)
class ReactionVariant:
    """Rhea ID as annotated, retaining direction separately from master ID."""

    rhea_id: str
    master_id: str
    direction: ReactionDirection

    def __post_init__(self) -> None:
        object.__setattr__(self, "rhea_id", _identifier(self.rhea_id, "RHEA"))
        object.__setattr__(self, "master_id", _identifier(self.master_id, "RHEA"))
        object.__setattr__(self, "direction", ReactionDirection(self.direction))


@dataclass(frozen=True, slots=True)
class ReactionParticipant:
    """Left/right are chemical equation sides, not fixed substrate/product roles."""

    master_id: str
    chebi_id: str
    side: ReactionSide
    coefficient: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "master_id", _identifier(self.master_id, "RHEA"))
        object.__setattr__(self, "chebi_id", _identifier(self.chebi_id, "CHEBI"))
        object.__setattr__(self, "side", ReactionSide(self.side))
        try:
            coefficient = Decimal(str(self.coefficient))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("coefficient must be a positive finite number") from exc
        if not coefficient.is_finite() or coefficient <= 0:
            raise ValueError("coefficient must be a positive finite number")
        object.__setattr__(self, "coefficient", coefficient)


@dataclass(frozen=True, slots=True)
class Evidence:
    """Evidence for a specific assertion; PMID/citation and ECO may be absent."""

    source_database: str
    eco_code: str | None = None
    assertion_method: AssertionMethod = AssertionMethod.UNKNOWN
    source_statement_iri: str | None = None
    citation_ids: tuple[str, ...] = ()
    source_references: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_database", _nonempty(self.source_database, "source_database"))
        object.__setattr__(self, "assertion_method", AssertionMethod(self.assertion_method))
        if self.eco_code is not None and re.fullmatch(r"ECO:[0-9]{7}", self.eco_code) is None:
            raise ValueError("eco_code must look like ECO:0000269")
        if self.source_statement_iri is not None:
            _nonempty(self.source_statement_iri, "source_statement_iri")
        citations = tuple(_nonempty(value, "citation ID") for value in self.citation_ids)
        object.__setattr__(self, "citation_ids", citations)
        references = tuple(_nonempty(value, "source reference") for value in self.source_references)
        object.__setattr__(self, "source_references", references)


@dataclass(frozen=True, slots=True)
class ProteinECClassification:
    """A protein belongs to an EC class; not a direct catalytic assertion."""

    accession: str
    ec_number: str
    source_release: SourceRelease

    def __post_init__(self) -> None:
        object.__setattr__(self, "accession", _accession(self.accession))
        object.__setattr__(self, "ec_number", _nonempty(self.ec_number, "ec_number"))
        if not isinstance(self.source_release, SourceRelease):
            raise TypeError("source_release must be a SourceRelease")


@dataclass(frozen=True, slots=True)
class ReactionECClassification:
    """A reaction belongs to an EC class; does not imply protein catalysis."""

    master_id: str
    ec_number: str
    source_release: SourceRelease

    def __post_init__(self) -> None:
        object.__setattr__(self, "master_id", _identifier(self.master_id, "RHEA"))
        object.__setattr__(self, "ec_number", _nonempty(self.ec_number, "ec_number"))
        if not isinstance(self.source_release, SourceRelease):
            raise TypeError("source_release must be a SourceRelease")


@dataclass(frozen=True, slots=True)
class ProteinReactionAssertion:
    """A source-asserted protein↔specific-reaction link with explicit provenance.

    This class must only be populated from an actual direct source annotation,
    never by joining ProteinECClassification with ReactionECClassification.
    Unknown/absent assertions are not negative experimental observations.
    """

    accession: str
    reaction: ReactionVariant
    source_release: SourceRelease
    evidence: tuple[Evidence, ...]
    annotation_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "accession", _accession(self.accession))
        if not isinstance(self.reaction, ReactionVariant):
            raise TypeError("reaction must be a ReactionVariant")
        if not isinstance(self.source_release, SourceRelease):
            raise TypeError("source_release must be a SourceRelease")
        proofs = tuple(self.evidence)
        if not proofs or any(not isinstance(proof, Evidence) for proof in proofs):
            raise ValueError("a direct assertion needs at least one Evidence record")
        object.__setattr__(self, "evidence", proofs)
        if self.annotation_id is not None:
            _nonempty(self.annotation_id, "annotation_id")

    @property
    def snapshot_key(self) -> tuple[str, str, str, str]:
        """Logical pair within a source release, not a globally immutable PK."""
        return (
            self.accession,
            self.reaction.rhea_id,
            self.source_release.database,
            self.source_release.release,
        )
