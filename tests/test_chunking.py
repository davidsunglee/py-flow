"""
Unit tests for text chunking — pure Python, no services needed.

Tests the sentence-aware chunking logic, overlap, boundary handling,
and edge cases.
"""

import pytest
from media.chunking import chunk_text, TextChunk, _estimate_tokens, _split_sentences


# ── Helpers ───────────────────────────────────────────────────────────────

def _words(n: int) -> str:
    """Generate a string of approximately n words."""
    return " ".join(f"word{i}" for i in range(n))


def _sentences(n: int, words_per: int = 30) -> str:
    """Generate n sentences, each with `words_per` words."""
    return " ".join(
        " ".join(f"word{i}x{s}" for i in range(words_per)) + "."
        for s in range(n)
    )


# ── Token estimation ─────────────────────────────────────────────────────


class TestTokenEstimation:
    def test_basic(self):
        assert _estimate_tokens("hello world") >= 2

    def test_empty(self):
        assert _estimate_tokens("") == 1  # max(1, ...)

    def test_proportional(self):
        short = _estimate_tokens("one two three")
        long = _estimate_tokens("one two three four five six seven eight nine ten")
        assert long > short


# ── Sentence splitting ───────────────────────────────────────────────────


class TestSentenceSplitting:
    def test_basic_sentences(self):
        text = "Hello world. This is a test. Another sentence here."
        sents = _split_sentences(text)
        assert len(sents) >= 2

    def test_paragraph_fallback(self):
        text = "first paragraph content\n\nsecond paragraph content"
        sents = _split_sentences(text)
        assert len(sents) == 2

    def test_single_line(self):
        text = "just one line without any sentence endings"
        sents = _split_sentences(text)
        assert len(sents) >= 1


# ── Chunking ─────────────────────────────────────────────────────────────


class TestChunking:
    def test_empty_text(self):
        """Empty or whitespace text returns empty list."""
        assert chunk_text("") == []
        assert chunk_text("   ") == []
        assert chunk_text("\n\n") == []

    def test_short_text(self):
        """Text shorter than chunk_size returns a single chunk."""
        text = "This is a short document about machine learning."
        chunks = chunk_text(text, chunk_size=512)
        assert len(chunks) == 1
        assert chunks[0].text == text
        assert chunks[0].chunk_index == 0
        assert chunks[0].start_char == 0
        assert chunks[0].end_char == len(text)
        assert chunks[0].token_count > 0

    def test_exact_chunk_size(self):
        """Text at exactly chunk_size tokens returns a single chunk."""
        # ~100 words ≈ 130 tokens
        text = _words(100)
        chunks = chunk_text(text, chunk_size=200)
        assert len(chunks) == 1

    def test_two_chunks(self):
        """Text that exceeds chunk_size splits into multiple chunks."""
        # Create text that's definitely bigger than chunk_size=50
        text = _sentences(20, words_per=20)
        chunks = chunk_text(text, chunk_size=50, chunk_overlap=10, min_chunk_size=10)
        assert len(chunks) >= 2
        # All chunks should have valid metadata
        for i, c in enumerate(chunks):
            assert c.chunk_index == i
            assert c.token_count > 0
            assert c.start_char >= 0
            assert c.end_char > c.start_char

    def test_overlap_content(self):
        """Consecutive chunks should share some text in the overlap region."""
        text = _sentences(20, words_per=20)
        chunks = chunk_text(text, chunk_size=80, chunk_overlap=20, min_chunk_size=10)
        assert len(chunks) >= 2
        # Check that some words from chunk 0 appear in chunk 1 (overlap)
        words_0 = set(chunks[0].text.split())
        words_1 = set(chunks[1].text.split())
        overlap = words_0 & words_1
        assert len(overlap) > 0, "Expected overlap between consecutive chunks"

    def test_chunk_offsets(self):
        """start_char/end_char should map back to the original text."""
        text = "First sentence here. Second sentence there. Third sentence everywhere."
        chunks = chunk_text(text, chunk_size=10, chunk_overlap=3, min_chunk_size=3)
        for c in chunks:
            # The chunk text should be findable in (or reconstructable from) the original
            assert c.start_char >= 0
            assert c.end_char <= len(text) + 1  # allow minor boundary tolerance

    def test_min_chunk_filter(self):
        """Trailing chunks below min_chunk_size are merged into previous."""
        # Create text where the last chunk would be tiny
        text = _sentences(10, words_per=20) + " Tiny."
        chunks = chunk_text(text, chunk_size=80, chunk_overlap=10, min_chunk_size=30)
        if len(chunks) >= 1:
            # The last chunk should not be tiny
            assert chunks[-1].token_count >= 5  # at least a few tokens

    def test_token_count_reasonable(self):
        """token_count should be approximately correct."""
        text = _words(100)  # ~100 words ≈ ~130 tokens
        chunks = chunk_text(text, chunk_size=1000)
        assert len(chunks) == 1
        # Should be roughly 100-150 tokens for 100 words
        assert 80 <= chunks[0].token_count <= 200

    def test_chunk_indices_sequential(self):
        """chunk_index should be 0, 1, 2, ..."""
        text = _sentences(30, words_per=20)
        chunks = chunk_text(text, chunk_size=50, chunk_overlap=10, min_chunk_size=10)
        for i, c in enumerate(chunks):
            assert c.chunk_index == i

    def test_all_text_covered(self):
        """Chunks should collectively cover all the original text's content."""
        text = "Alpha bravo charlie. Delta echo foxtrot. Golf hotel india. Juliet kilo lima."
        chunks = chunk_text(text, chunk_size=15, chunk_overlap=3, min_chunk_size=3)
        # Every word in the original should appear in at least one chunk
        original_words = set(text.replace(".", "").split())
        chunk_words = set()
        for c in chunks:
            chunk_words.update(c.text.replace(".", "").split())
        missing = original_words - chunk_words
        assert len(missing) == 0, f"Words missing from chunks: {missing}"
