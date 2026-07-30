# Grammar Storage Database — Design

**Date:** 2026-07-30
**Status:** Approved, not yet implemented

## Overview

A new, standalone SQL Server database to store natural-language grammar rules —
morphology, syntax, and phonology, including exceptions — encoded as individual
Prolog clauses. It lives on `corpus-host` alongside the existing ad hoc
grammar-scraper containers (`grammarwatch`, `glottolog`, `twirpx-scraper`,
`lsp-scraper`, `elp-scraper`), which harvest the descriptive-grammar PDFs the
rules will be extracted from. It is **not** part of `TextCorpuses` or this
repo's Airflow pipeline — same category as those scraper containers, unrelated
to the DAG-driven corpus-building system this repo otherwise documents.

## Goals

- Store Prolog-coded grammar rules per language, normalized enough to query
  by language, category, predicate, or grammatical feature.
- Represent exceptions as first-class rules that override a more general rule,
  matching how Prolog itself resolves overlapping clauses (more specific
  clause wins).
- Track provenance back to the source grammar-description document (file,
  page, excerpt) and an extraction review/confidence status, since rules will
  be extracted from PDFs (manually or LLM-assisted) rather than hand-authored
  from scratch.
- Cover all three grammar domains — morphology, syntax, phonology — under one
  schema rather than three parallel ones.

## Non-goals

- Not a Prolog execution environment — this schema stores clause text and
  structured metadata about it; nothing here runs or validates Prolog.
- Not wired into `TextCorpuses`, the Airflow DAGs, or the ops report.
- No ingestion pipeline / extraction tooling is specified here — this spec
  covers storage only. How PDFs get turned into Prolog clauses (manual
  transcription vs. an LLM-assisted extraction step) is a separate concern.
- No versioning/audit history of rule edits beyond `CreatedAt`/`UpdatedAt` —
  can be added later if it turns out to be needed.

## Schema

### `Language`

Minimal — just enough to identify a language and link rules to it.

```
Id            int IDENTITY PK
Name          nvarchar(200) NOT NULL
Iso639_3      char(3) NULL
CreatedAt     datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
```

### `RuleCategory`

Small closed lookup table, seeded with exactly three rows: `Morphology`,
`Syntax`, `Phonology`.

```
Id            int IDENTITY PK
Name          nvarchar(50) NOT NULL UNIQUE
```

### `SourceDocument`

Describes one harvested grammar-description document. Deliberately
language-agnostic — a single comparative grammar can cover several
languages, and language identity is already carried by `Predicate.LanguageId`,
so tagging it again here would risk the two disagreeing.

```
Id              int IDENTITY PK
ContainerSource nvarchar(20) NOT NULL
                CHECK (ContainerSource IN
                  ('grammarwatch','glottolog','twirpx-scraper',
                   'lsp-scraper','elp-scraper','manual'))
FilePath        nvarchar(500) NOT NULL
Title           nvarchar(500) NULL
HarvestedAt     datetime2 NULL
CreatedAt       datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
```

### `Predicate`

One row per named Prolog predicate, per language. Holds the metadata shared
by every clause of that predicate, so it isn't repeated per-clause.

```
Id              int IDENTITY PK
LanguageId      int NOT NULL FK -> Language.Id
RuleCategoryId  int NOT NULL FK -> RuleCategory.Id
Subcategory     nvarchar(100) NULL   -- free text, e.g. "Noun Declension", "Word Order"
Name            nvarchar(200) NOT NULL   -- Prolog predicate name, e.g. "plural_of"
Arity           tinyint NOT NULL
Description     nvarchar(1000) NULL   -- human-readable gloss
CreatedAt       datetime2 NOT NULL DEFAULT SYSUTCDATETIME()

UNIQUE (LanguageId, Name, Arity)
```

`Subcategory` is intentionally free text rather than another FK'd lookup —
subcategory labels are numerous and vary a lot per language; a controlled
vocabulary here would fight the data more than it would help querying.

### `Rule`

One row per individual Prolog clause. A predicate implemented via several
clauses (a regular pattern plus irregular overrides) becomes several `Rule`
rows sharing one `Predicate`.

```
Id                    int IDENTITY PK
PredicateId           int NOT NULL FK -> Predicate.Id
PrologClause          nvarchar(max) NOT NULL   -- the actual clause text
OverridesRuleId        int NULL FK -> Rule.Id   -- self-reference: general rule this exception overrides
Priority              int NOT NULL DEFAULT 0   -- higher = more specific, wins first in resolution order
SourceDocumentId       int NULL FK -> SourceDocument.Id
SourcePage             nvarchar(50) NULL   -- e.g. "42" or "42-43"
SourceExcerpt          nvarchar(2000) NULL   -- original grammar-description text the rule was extracted from
ExtractionMethod       nvarchar(20) NULL
                       CHECK (ExtractionMethod IN ('manual','llm-assisted','automated'))
ReviewStatus           nvarchar(20) NOT NULL DEFAULT 'extracted'
                       CHECK (ReviewStatus IN ('extracted','verified','rejected'))
ExtractionConfidence   float NULL
ReviewedBy             nvarchar(100) NULL
ReviewedAt             datetime2 NULL
CreatedAt              datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
UpdatedAt              datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
```

**Invariant (not enforced by a plain FK):** `OverridesRuleId` should reference
a rule under the *same* `Predicate`. SQL Server can't express "same parent as
me" as a declarative FK constraint; if cross-predicate overrides turn out to
be a real problem in practice, add a trigger then rather than building one
speculatively now.

### Feature model

Normalized dimension/value tables, attached at the clause (`Rule`) level —
not the `Predicate` level — since an exception clause can produce or require
different feature values than the general rule it overrides (e.g. an
irregular plural that's also gender-specific).

```
FeatureDimension
  Id            int IDENTITY PK
  Name          nvarchar(50) NOT NULL UNIQUE   -- Case, Number, Tense, Gender, Aspect, Mood, Person...

FeatureValue
  Id                  int IDENTITY PK
  FeatureDimensionId  int NOT NULL FK -> FeatureDimension.Id
  Name                nvarchar(100) NOT NULL   -- genitive, plural, past...
  UNIQUE (FeatureDimensionId, Name)

RuleFeatureValue
  RuleId          int NOT NULL FK -> Rule.Id
  FeatureValueId  int NOT NULL FK -> FeatureValue.Id
  Role            nvarchar(10) NOT NULL CHECK (Role IN ('produces','requires'))
  PRIMARY KEY (RuleId, FeatureValueId, Role)
```

## Entity relationships

```
Language 1---* Predicate 1---* Rule ---(self-ref OverridesRuleId)---> Rule
                                    \---* RuleFeatureValue *---1 FeatureValue *---1 FeatureDimension
Rule *---1 SourceDocument (nullable)
RuleCategory 1---* Predicate
```

## Worked example

English noun pluralization, `plural_of/2`:

- `Predicate`: `LanguageId=English, Name=plural_of, Arity=2, RuleCategory=Morphology, Subcategory="Noun Declension"`
- `Rule` A (general): `PrologClause = "plural_of(Stem, Plural) :- regular(Stem, Plural)."`, `Priority=0`
- `Rule` B (exception): `PrologClause = "plural_of(child, children)."`, `OverridesRuleId = A.Id`, `Priority=10`, `SourceDocumentId`/`SourcePage`/`SourceExcerpt` pointing at the descriptive grammar it was pulled from, `ReviewStatus='extracted'`
- `RuleFeatureValue` on Rule B: `(FeatureValue=Number:plural, Role=produces)`

## Deployment

Standalone SQL Server database on `corpus-host`, created and managed the same
ad hoc way as the grammar-scraper containers themselves (not via this repo's
`Database/*.sql` migration sequence, since it isn't part of `TextCorpuses`).
Exact deployment mechanics (database name, login/credentials, migration
script location) are an implementation-plan concern, not a schema-design one.

## Open items / future extensions

- Ingestion/extraction tooling (PDF → Prolog clause) is out of scope for this
  spec and would be its own design.
- No versioning/history table yet — add if rule edits need an audit trail.
- `OverridesRuleId`'s same-predicate invariant is documented, not enforced;
  revisit if it's ever violated in practice.
