# Verixia
### AI outputs, proven.

Verixia is a B2B legal claim verification API. It checks AI-generated legal claims against a self-expanding primary source knowledge graph and returns structured, auditable verification results with confidence ratings, citation trails, and evidence quality certification.

---

## What it does

Submit a legal claim. Verixia returns:

- **Verdict** — YES / YES_CONTESTED / PARTIALLY_SUPPORTED / NO / UNVERIFIABLE
- **Confidence** — HIGH / MEDIUM / LOW / CONTESTED / UNVERIFIABLE
- **Reasoning** — Conversational explanation citing specific authority
- **Citations** — Supporting sources with document ID, date, and relevance score
- **Contradictions** — Opposing authority with equal transparency
- **Evidence Quality** — Holdings percentage, score stability, verifier certification
- **Audit Trail** — Claim ID, graph version, sources queried, temporal filter

---

## Example

**Request**
```
POST /v1/verify
X-API-Key: your_api_key
Content-Type: application/json

{
  "claim": "The First Amendment prohibits Congress from abridging freedom of speech."
}
```

**Response**
```
{
  "claim_id": "a741c10b9304e498",
  "verdict": "YES",
  "confidence": "HIGH",
  "score": 0.8643,
  "reasoning": "Yes. Supported by 4 authoritative source(s), including cl_106935 (1964). Minor contradicting authority present but outweighed.",
  "citations": [...],
  "evidence_quality": {
    "holdings_percentage": 1.0,
    "verifier_passed": true,
    "score_drop": 0.0
  }
}
```

---

## Architecture

Verixia is built on a novel two-layer quality control architecture.

**Layer 1 — Structural Role Classification**

Every document chunk is classified at ingest time by its structural role in the legal opinion: HOLDING, CONSTITUTIONAL_TEXT, STATUTORY_TEXT, DICTA, QUOTED_ARGUMENT, RECITATION, DISSENT, CONCURRENCE. Role weights are applied during evidence scoring — a court's actual holding carries 5x the weight of a recited opposing argument.

**Layer 2 — Pre-Response Evidence Verifier**

Before any confidence score is returned to the caller, a second-pass verifier audits the assembled citation chain. What percentage of supporting citations come from actual holdings? Does the score remain stable when weak-authority chunks are excluded? If the evidence does not meet the certification threshold, confidence is downgraded and the reason is disclosed.

**Knowledge Graph**

- 75,000+ chunks from federal primary sources
- Self-expanding citation graph — every ingested document extracts citations and queues them for automatic retrieval
- Temporally constrained — claims can be verified against the law as it existed at any point in time
- Sources: CourtListener (federal opinions), Congress.gov (statutes), regulations.gov (federal regulations), founding documents (Constitution, Bill of Rights, Declaration of Independence, Federalist Papers, Magna Carta)

**Confidence Levels**

- HIGH — Strong supporting authority, verifier certified
- MEDIUM — Partial support, limited corpus or mixed signals
- CONTESTED — Substantial authority on both sides
- LOW — Contradicting authority outweighs support
- UNVERIFIABLE — Insufficient corpus coverage for this claim

---

## API Reference

### POST /v1/verify

**Headers**

    X-API-Key: your_api_key
    Content-Type: application/json

**Request body**

    {
      "claim": "string (required)",
      "top_k": 10,
      "as_of_date": "YYYY-MM-DD (optional)",
      "domain": "case_law | statute | regulation (optional)"
    }

**Response**

    {
      "claim_id": "string",
      "verdict": "YES | YES_CONTESTED | PARTIALLY_SUPPORTED | NO | UNVERIFIABLE",
      "confidence": "HIGH | MEDIUM | CONTESTED | LOW | UNVERIFIABLE",
      "score": 0.0,
      "reasoning": "string",
      "citations": [...],
      "contradictions": [...],
      "evidence_quality": {...},
      "audit_trail": {...}
    }

### GET /v1/stats

Returns knowledge graph statistics and registry summary.

---

## Accuracy

Benchmark: 20-claim test set spanning constitutional law, civil rights, federal regulatory, and statutory domains.

| Graph Size | Accuracy |
|---|---|
| 77 chunks | Baseline |
| 2,203 chunks | 44% |
| 72,909 chunks | 85% |
| 75,578 chunks + founding documents | In progress |

Accuracy improves demonstrably with graph size, validating the self-expanding knowledge graph architecture.

---

## Production Status

- Live endpoint on AWS EC2 (us-east-1)
- 75,000+ chunk knowledge graph
- Two-layer quality control architecture (patent pending)
- API key authentication with tier-based rate limiting
- Verdict + reasoning in every response
- Founding documents corpus (Constitution, Bill of Rights, Federalist Papers, Magna Carta)

---

## Built by

[ContinuumCoreDev](https://github.com/ContinuumCoreDev)

---

*Verixia is a B2B API product. For licensing and acquisition inquiries, contact via GitHub.*
