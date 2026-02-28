"""
Text Chunking — Split extracted text into overlapping chunks for embedding.

Sentence-aware chunking with configurable size and overlap. Each chunk
becomes a separate vector in the document_chunks table.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class TextChunk:
    """A chunk of text from a document, with position metadata."""
    text: str
    chunk_index: int
    start_char: int
    end_char: int
    token_count: int


# ── Sentence splitting ────────────────────────────────────────────────────

# Primary: split on sentence-ending punctuation followed by whitespace.
# Matches: "Hello. World", "Hello! Next", "end.\nStart", "done. next"
_SENTENCE_RE = re.compile(
    r'(?<=[.!?])\s+'    # lookbehind for sentence-end punctuation, consume whitespace
)

# Fallback: split on double newlines (paragraph boundaries)
_PARAGRAPH_RE = re.compile(r'\n\s*\n')


def _split_sentences(text: str) -> list[str]:
    """Split text into sentences. Falls back to paragraph/newline splitting."""
    sentences = _SENTENCE_RE.split(text)
    # If regex produced only 1 chunk (no sentence boundaries found),
    # try paragraph splitting
    if len(sentences) <= 1:
        sentences = _PARAGRAPH_RE.split(text)
    # If still 1 chunk, split on single newlines
    if len(sentences) <= 1:
        sentences = text.split('\n')
    # Filter empty strings and strip whitespace
    return [s.strip() for s in sentences if s.strip()]


# ── Token estimation ─────────────────────────────────────────────────────

def _estimate_tokens(text: str) -> int:
    """Approximate token count: word_count × 1.3.

    This avoids a tokenizer dependency while being accurate enough
    for chunking decisions. Gemini and most LLMs tokenize at roughly
    1.3 tokens per whitespace-delimited word.
    """
    words = len(text.split())
    return max(1, int(words * 1.3))


# ── Main chunking function ───────────────────────────────────────────────

def chunk_text(
    text: str,
    chunk_size: int = 512,
    chunk_overlap: int = 64,
    min_chunk_size: int = 50,
) -> list[TextChunk]:
    """
    Split text into overlapping chunks suitable for embedding.

    Uses sentence-aware splitting: chunks break at sentence boundaries
    when possible, never mid-word.

    Args:
        text: The full document text.
        chunk_size: Target chunk size in approximate tokens (default 512).
        chunk_overlap: Overlap between consecutive chunks in tokens (default 64).
        min_chunk_size: Minimum chunk size — trailing chunks smaller than this
                        are merged into the previous chunk (default 50).

    Returns:
        List of TextChunk with text, position metadata, and token counts.
        Empty list if text is empty/whitespace.
    """
    if not text or not text.strip():
        return []

    text = text.strip()

    # If the whole text fits in one chunk, return it directly
    total_tokens = _estimate_tokens(text)
    if total_tokens <= chunk_size:
        return [TextChunk(
            text=text,
            chunk_index=0,
            start_char=0,
            end_char=len(text),
            token_count=total_tokens,
        )]

    sentences = _split_sentences(text)
    if not sentences:
        return []

    chunks: list[TextChunk] = []
    current_sentences: list[str] = []
    current_tokens = 0
    chunk_start_char = 0

    # Track character positions in the original text
    # Build a map of sentence → start position in original text
    sentence_positions: list[int] = []
    search_from = 0
    for sent in sentences:
        pos = text.find(sent, search_from)
        if pos == -1:
            pos = search_from  # fallback
        sentence_positions.append(pos)
        search_from = pos + len(sent)

    def _flush_chunk(sent_list: list[str], start_idx: int, end_idx: int) -> None:
        """Create a chunk from accumulated sentences."""
        chunk_text_str = " ".join(sent_list)
        s_char = sentence_positions[start_idx]
        e_char = sentence_positions[end_idx - 1] + len(sentences[end_idx - 1])
        tokens = _estimate_tokens(chunk_text_str)
        chunks.append(TextChunk(
            text=chunk_text_str,
            chunk_index=len(chunks),
            start_char=s_char,
            end_char=e_char,
            token_count=tokens,
        ))

    sent_start_idx = 0  # index of first sentence in current chunk

    for i, sent in enumerate(sentences):
        sent_tokens = _estimate_tokens(sent)

        if current_tokens + sent_tokens > chunk_size and current_sentences:
            # Flush current chunk
            _flush_chunk(current_sentences, sent_start_idx, i)

            # Calculate overlap: walk backward from end of current chunk
            # Always include at least one sentence for continuity
            overlap_sentences: list[str] = []
            overlap_tokens = 0
            overlap_start_idx = i
            for j in range(len(current_sentences) - 1, -1, -1):
                s = current_sentences[j]
                s_tok = _estimate_tokens(s)
                if overlap_tokens + s_tok > chunk_overlap and overlap_sentences:
                    break
                overlap_sentences.insert(0, s)
                overlap_tokens += s_tok
                overlap_start_idx = sent_start_idx + j

            # Start new chunk with overlap + current sentence
            current_sentences = overlap_sentences + [sent]
            current_tokens = overlap_tokens + sent_tokens
            sent_start_idx = overlap_start_idx
        else:
            current_sentences.append(sent)
            current_tokens += sent_tokens

    # Flush remaining sentences
    if current_sentences:
        if chunks and current_tokens < min_chunk_size:
            # Merge tiny trailing chunk into previous
            prev = chunks[-1]
            merged_text = prev.text + " " + " ".join(current_sentences)
            end_idx = len(sentences)
            e_char = sentence_positions[end_idx - 1] + len(sentences[end_idx - 1])
            chunks[-1] = TextChunk(
                text=merged_text,
                chunk_index=prev.chunk_index,
                start_char=prev.start_char,
                end_char=e_char,
                token_count=_estimate_tokens(merged_text),
            )
        else:
            _flush_chunk(current_sentences, sent_start_idx, len(sentences))

    return chunks
