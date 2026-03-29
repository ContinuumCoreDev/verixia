"""
Probatum — Multi-Layer Document Chunker
The core proprietary component.

Three layers working in sequence:
  1. Structural marker detection (hard breaks, confidence=1.0)
  2. Semantic similarity boundary detection (soft breaks)
  3. spaCy sentence segmentation (atomic unit — never break mid-sentence)

Min/max token guards ensure no chunk is too small or too large.
Each chunk is tagged with metadata for confidence-weighted retrieval.
"""

import re
import logging
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

import yaml
import spacy

from chunker.structural import detect_structural_breaks
from chunker.semantic   import detect_semantic_breaks

logger = logging.getLogger(__name__)

_cfg_path = Path(__file__).parent.parent / "config" / "config.yaml"
with open(_cfg_path) as f:
    _cfg = yaml.safe_load(f)

MIN_TOKENS = _cfg["chunker"]["min_tokens"]
MAX_TOKENS = _cfg["chunker"]["max_tokens"]

# Lazy-load spaCy — sentencizer only, no full NLP pipeline
_nlp = None

def _get_nlp():
    global _nlp
    if _nlp is None:
        _nlp = spacy.blank("en")
        _nlp.add_pipe("sentencizer")
        logger.info("spaCy sentencizer loaded.")
    return _nlp


@dataclass
class Chunk:
    chunk_id:       str
    doc_id:         str
    text:           str
    token_count:    int
    position:       int             # index within document
    break_type:     str             # structural | semantic | size_guard | first
    break_marker:   Optional[str]   # which marker triggered the break (if structural)
    similarity:     Optional[float] # similarity score (if semantic)
    source:         str             # document source
    doc_type:       str             # case_law | statute | regulation | unknown
    published_date: Optional[str]   # for temporal constraint layer
    section_label:  str = ""        # detected section heading if any


def _count_tokens(text: str) -> int:
    """Approximate token count — words + punctuation."""
    return len(text.split())


def _extract_section_label(sentence: str) -> str:
    """
    Attempt to extract a section label from a structural break sentence.
    Returns a short label or empty string.
    """
    patterns = [
        r"(ARTICLE\s+[IVXLC\d]+)",
        r"(SECTION\s+[\d\.]+)",
        r"(§\s*[\d\.]+)",
        r"(PART\s+[IVXLC\d]+)",
        r"([A-Z][A-Z\s]{3,20})$",
    ]
    for pattern in patterns:
        match = re.search(pattern, sentence.strip(), re.IGNORECASE)
        if match:
            return match.group(1).strip()[:50]
    return ""


def _get_sentences(text: str) -> list[str]:
    """
    Segment text into sentences using spaCy sentencizer.
    Filters empty sentences and very short fragments.
    """
    nlp  = _get_nlp()
    doc  = nlp(text[:1_000_000])  # spaCy limit guard
    sents = [s.text.strip() for s in doc.sents if len(s.text.strip()) > 10]
    return sents


def _merge_sentences(
    sentences: list[str],
    break_indices: set,
    break_metadata: dict
) -> list[tuple[str, str, Optional[str], Optional[float]]]:
    """
    Merge sentences into chunks at break points.
    Applies min/max token guards.

    Returns list of (text, break_type, break_marker, similarity) tuples.
    """
    chunks      = []
    current     = []
    current_tok = 0
    break_type  = "first"
    break_marker = None
    similarity   = None

    for i, sentence in enumerate(sentences):
        tok = _count_tokens(sentence)

        # ── Size guard: flush if adding would exceed max ──────
        if current and (current_tok + tok) > MAX_TOKENS:
            chunks.append((" ".join(current), break_type, break_marker, similarity))
            current      = []
            current_tok  = 0
            break_type   = "size_guard"
            break_marker = None
            similarity   = None

        # ── Structural or semantic break ──────────────────────
        if i in break_indices and current:
            # Only break if current chunk meets minimum size
            if current_tok >= MIN_TOKENS:
                chunks.append((" ".join(current), break_type, break_marker, similarity))
                current      = []
                current_tok  = 0
                meta         = break_metadata.get(i, {})
                break_type   = meta.get("break_type", "semantic")
                break_marker = meta.get("break_marker")
                similarity   = meta.get("similarity")
            # If too small, absorb the break and continue

        current.append(sentence)
        current_tok += tok

    # ── Flush final chunk ─────────────────────────────────────
    if current:
        chunks.append((" ".join(current), break_type, break_marker, similarity))

    return chunks


def chunk_document(doc: dict) -> list[Chunk]:
    """
    Chunk a Probatum document into semantically coherent pieces.

    Pipeline:
      1. Sentence segmentation (spaCy)
      2. Structural break detection
      3. Semantic break detection (on non-structural sentences)
      4. Merge into chunks with min/max guards
      5. Tag each chunk with metadata

    Args:
        doc     Probatum document dict with raw_text, doc_id, doc_type, etc.

    Returns:
        List of Chunk objects ready for Qdrant ingest.
    """
    doc_id    = doc["doc_id"]
    doc_type  = doc.get("doc_type", "unknown")
    raw_text  = doc.get("raw_text", "")
    source    = doc.get("source", "unknown")
    pub_date  = doc.get("published_date")

    if not raw_text or len(raw_text.strip()) < 50:
        logger.warning(f"{doc_id}: raw_text too short to chunk ({len(raw_text)} chars)")
        return []

    logger.info(f"Chunking {doc_id} ({doc_type}) — {len(raw_text)} chars")

    # ── Step 1: Sentence segmentation ────────────────────────
    sentences = _get_sentences(raw_text)
    if len(sentences) < 2:
        logger.warning(f"{doc_id}: only {len(sentences)} sentences found")
        return []

    # ── Step 2: Structural breaks ─────────────────────────────
    structural = detect_structural_breaks(sentences, doc_type)
    struct_idx = {b.sentence_index for b in structural}
    struct_map = {
        b.sentence_index: {
            "break_type":   "structural",
            "break_marker": b.marker_type,
            "similarity":   None,
        }
        for b in structural
    }

    # ── Step 3: Semantic breaks ───────────────────────────────
    semantic = detect_semantic_breaks(
        sentences,
        exclude_indices=struct_idx
    )
    sem_idx = {b.sentence_index for b in semantic}
    sem_map = {
        b.sentence_index: {
            "break_type":   "semantic",
            "break_marker": None,
            "similarity":   b.similarity,
        }
        for b in semantic
    }

    # ── Step 4: Merge break maps ──────────────────────────────
    all_break_indices = struct_idx | sem_idx
    all_break_metadata = {**sem_map, **struct_map}  # structural wins conflicts

    # ── Step 5: Build chunks ──────────────────────────────────
    raw_chunks = _merge_sentences(sentences, all_break_indices, all_break_metadata)

    # ── Step 6: Tag with metadata ─────────────────────────────
    chunks = []
    for position, (text, break_type, break_marker, similarity) in enumerate(raw_chunks):
        chunk_id = f"{doc_id}_chunk_{position:04d}"

        # Try to extract section label from first sentence
        first_sentence = text.split(".")[0] if "." in text else text[:80]
        section_label  = _extract_section_label(first_sentence) if break_type == "structural" else ""

        chunks.append(Chunk(
            chunk_id       = chunk_id,
            doc_id         = doc_id,
            text           = text,
            token_count    = _count_tokens(text),
            position       = position,
            break_type     = break_type,
            break_marker   = break_marker,
            similarity     = similarity,
            source         = source,
            doc_type       = doc_type,
            published_date = pub_date,
            section_label  = section_label,
        ))

    logger.info(
        f"{doc_id}: {len(sentences)} sentences → "
        f"{len(structural)} structural + {len(semantic)} semantic breaks → "
        f"{len(chunks)} chunks"
    )
    return chunks


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Testing chunker on Marbury v. Madison...")

    from procurement.courtlistener import fetch_opinions_by_query
    from procurement.resolver      import resolve_full_text

    docs = fetch_opinions_by_query(
        query="Marbury v Madison",
        court="scotus",
        max_results=1
    )

    if not docs:
        print("No documents fetched.")
        exit()

    doc = resolve_full_text(docs[0])

    if doc["parse_status"] != "ok":
        print(f"Resolution failed: {doc['error_notes']}")
        exit()

    chunks = chunk_document(doc)

    print(f"\nResults:")
    print(f"  Document: {doc['title']}")
    print(f"  Full text: {len(doc['raw_text'])} chars")
    print(f"  Chunks: {len(chunks)}")

    print(f"\nBreakdown by type:")
    from collections import Counter
    types = Counter(c.break_type for c in chunks)
    for btype, count in types.most_common():
        print(f"  {btype}: {count}")

    print(f"\nToken range:")
    tokens = [c.token_count for c in chunks]
    print(f"  Min: {min(tokens)}")
    print(f"  Max: {max(tokens)}")
    print(f"  Avg: {sum(tokens) // len(tokens)}")

    print(f"\nFirst 3 chunks:")
    for c in chunks[:3]:
        print(f"\n  [{c.position}] {c.break_type}"
              f" | {c.token_count} tokens"
              f" | marker: {c.break_marker or 'none'}")
        print(f"  {c.text[:200]}...")
