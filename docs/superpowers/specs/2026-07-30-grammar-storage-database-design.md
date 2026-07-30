# Grammar Storage Database — Design

**Date:** 2026-07-30 (updated same day: Prolog → ASP)
**Status:** Approved, implemented

## Overview

A new, standalone SQL Server database to store natural-language grammar rules —
morphology, syntax, and phonology, including exceptions — encoded as
**Answer Set Programming (ASP, e.g. Clingo) statements**. It lives on
`corpus-host` alongside the existing ad hoc grammar-scraper containers
(`grammarwatch`, `glottolog`, `twirpx-scraper`, `lsp-scraper`, `elp-scraper`),
which harvest the descriptive-grammar PDFs the rules will be extracted from.
It is **not** part of `TextCorpuses` or this repo's Airflow pipeline — same
category as those scraper containers, unrelated to the DAG-driven
corpus-building system this repo otherwise documents.

## Why ASP instead of Prolog

The schema originally targeted Prolog. ASP was chosen instead because the
central modeling problem here — "a general rule, unless a specific exception
applies" — is ASP's native reason for existing, not an emergent property of
clause ordering the way it is in Prolog:

- In Prolog, an exception "wins" because it's tried first (or via cut )— an
  operational, ordering-dependent mechanism.
- In ASP, defaults-with-exceptions are expressed declaratively via **default
  negation**: the general rule's body includes `not <exception-flag>`, and a
  separate exception fact sets that flag. All statements are evaluated
  together under stable-model semantics; there is no "clause order" to reason
  about at all.
- ASP additionally has **weak constraints** (`:~ Body. [weight@priority]`),
  which map directly onto ranked/soft grammatical constraints (the
  Optimality-Theory style of grammar), something Prolog has no native
  equivalent for.

This changed one real thing in the schema: `Rule.PrologClause` became
`Rule.AspStatement`, and the `OverridesRuleId`/`Priority` pair is now
**structured metadata describing the same general/exception relationship
that the ASP text itself encodes via `not`** — not something ASP's execution
reads directly, except in the specific case where a rule is written as a
weak constraint, where `Priority` is literally that constraint's priority
level. See the worked examples below.

## Goals

- Store ASP-coded grammar rules per language, normalized enough to query by
  language, category, predicate, or grammatical feature.
- Represent exceptions as first-class rules that override a more general
  rule, matching how ASP itself expresses defaults and exceptions (default
  negation over an explicit exception fact, or a ranked weak constraint).
- Track provenance back to the source grammar-description document (file,
  page, excerpt) and an extraction review/confidence status, since rules will
  be extracted from PDFs (manually or LLM-assisted) rather than hand-authored
  from scratch.
- Cover all three grammar domains — morphology, syntax, phonology — under one
  schema rather than three parallel ones.

## Non-goals

- Not an ASP execution environment — this schema stores statement text and
  structured metadata about it; nothing here runs Clingo or validates
  answer sets.
- Not wired into `TextCorpuses`, the Airflow DAGs, or the ops report.
- No ingestion pipeline / extraction tooling is specified here — this spec
  covers storage only. How PDFs get turned into ASP statements (manual
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

One row per named ASP predicate, per language. Holds the metadata shared by
every statement of that predicate, so it isn't repeated per-statement.

```
Id              int IDENTITY PK
LanguageId      int NOT NULL FK -> Language.Id
RuleCategoryId  int NOT NULL FK -> RuleCategory.Id
Subcategory     nvarchar(100) NULL   -- free text, e.g. "Noun Declension", "Word Order"
Name            nvarchar(200) NOT NULL   -- ASP predicate name, e.g. "plural_of"
Arity           tinyint NOT NULL
Description     nvarchar(1000) NULL   -- human-readable gloss
CreatedAt       datetime2 NOT NULL DEFAULT SYSUTCDATETIME()

UNIQUE (LanguageId, Name, Arity)
```

`Subcategory` is intentionally free text rather than another FK'd lookup —
subcategory labels are numerous and vary a lot per language; a controlled
vocabulary here would fight the data more than it would help querying.

### `Rule`

One row per individual ASP statement (fact, normal rule, choice rule, or
constraint). A predicate implemented via several statements (a default rule
plus exception facts) becomes several `Rule` rows sharing one `Predicate`.

```
Id                    int IDENTITY PK
PredicateId           int NOT NULL FK -> Predicate.Id
AspStatement          nvarchar(max) NOT NULL   -- the actual ASP statement text
OverridesRuleId        int NULL FK -> Rule.Id   -- self-reference: general rule this exception overrides
Priority              int NOT NULL DEFAULT 0   -- structured metadata; also the literal weak-constraint priority if encoded as one
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

**How an exception is actually encoded:** a default rule guards itself with
`not <exception-flag>`; the exception is a *separate* fact that both sets the
flag and states the override. That's two ASP statements — two `Rule` rows,
both with `OverridesRuleId` pointing at the general rule and a higher
`Priority` — not one. See the worked example.

### Feature model

Normalized dimension/value tables, attached at the statement (`Rule`) level —
not the `Predicate` level — since an exception statement can produce or
require different feature values than the general rule it overrides (e.g. an
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
- `Rule` A (general): `AspStatement = "plural_of(Noun, Plural) :- noun(Noun), regular_plural(Noun, Plural), not irregular(Noun)."`, `Priority=0`
- `Rule` B1 (exception flag): `AspStatement = "irregular(child)."`, `OverridesRuleId = A.Id`, `Priority=10`
- `Rule` B2 (exception override): `AspStatement = "plural_of(child, children)."`, `OverridesRuleId = A.Id`, `Priority=10`,
  `SourceDocumentId`/`SourcePage`/`SourceExcerpt` pointing at the descriptive grammar it was pulled from, `ReviewStatus='extracted'`
- `RuleFeatureValue` on Rule B2: `(FeatureValue=Number:plural, Role=produces)`

Note the general rule (A) is the only statement that mentions `not` — the
negation-as-failure lives in the default, not in the exception. This is the
opposite of how it reads at first glance and is worth remembering when
authoring new rules.

## Deployment

Standalone SQL Server database (`GrammarDB`) on the existing SQL Server
engine — the `apache-airflow-mssql-1` container on `corpus-host`
(172.21.128.103), the same engine that serves `TextCorpuses` for the main
Airflow pipeline. `GrammarDB` is a separate, independent database on that
engine with no FKs or shared tables back to `TextCorpuses`. Deployed via
`schema.sql` (in a sibling `grammar-db` directory, not this repo's own
`Database/*.sql` sequence, since it isn't part of `TextCorpuses`): copy the
script into the container (`docker cp`) and run it with `sqlcmd`, same
pattern as this repo's own migrations.

## Open items / future extensions

- Ingestion/extraction tooling (PDF → ASP statement) is out of scope for this
  spec and would be its own design.
- No versioning/history table yet — add if rule edits need an audit trail.
- `OverridesRuleId`'s same-predicate invariant is documented, not enforced;
  revisit if it's ever violated in practice.
- Weak constraints (`:~ Body. [weight@Priority]`) for genuinely soft/ranked
  (Optimality-Theory-style) rules are supported by the schema as-is (a `Rule`
  row's `AspStatement` can hold any ASP statement shape) but aren't used in
  the worked examples, which stick to the simpler hard default+exception
  pattern. Revisit if a real rule needs ranked, competing constraints rather
  than a strict override.
