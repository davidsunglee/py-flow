"""
Mirror test for demo_media.py
================================
Verifies the full demo flow:

  1. Upload documents (text, markdown, HTML, binary)
  2. Full-text search with ranking
  3. Download and verify
  4. List and filter
  5. Storable features (find, history)
"""

import textwrap

import pytest

from media import MediaStore
from media.models import Document, bootstrap_search_schema


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def _provision(store_server):
    """Provision user + bootstrap search schema."""
    store_server.provision_user("media_demo_user", "media_demo_pw")
    admin_conn = store_server.admin_conn()
    bootstrap_search_schema(admin_conn)
    admin_conn.close()
    return store_server


@pytest.fixture(scope="module")
def conn(_provision):
    """User connection for media tests."""
    from store import connect
    info = _provision.conn_info()
    c = connect(user="media_demo_user", host=info["host"], port=info["port"],
                dbname=info["dbname"], password="media_demo_pw")
    yield c
    c.close()


@pytest.fixture(scope="module")
def ms(media_server, conn):
    """MediaStore connected to test S3."""
    store = MediaStore(s3_endpoint=media_server.endpoint, s3_bucket=media_server.bucket)
    yield store
    store.close()


@pytest.fixture(scope="module")
def uploaded_docs(ms):
    """Upload all 5 demo documents and return them."""
    doc1 = ms.upload(
        b"This research report covers interest rate swap pricing, "
        b"credit default swap valuation, and yield curve construction. "
        b"The analysis uses Monte Carlo simulation for CVA/DVA calculations.",
        filename="irs_research.txt",
        title="Interest Rate Swap Research",
        tags=["research", "rates", "swaps"],
    )

    doc2 = ms.upload(
        textwrap.dedent("""\
        # Q1 Trading Summary

        ## Equity Derivatives
        The desk generated **$12.5M** in P&L from equity options market-making.
        Volatility skew steepened significantly in March.

        ## Fixed Income
        Interest rate swap volumes increased 30% quarter-over-quarter.

        ## Risk Metrics
        - VaR (99%): $2.1M
        """).encode(),
        filename="q1_summary.md",
        title="Q1 Trading Summary",
        tags=["trading", "quarterly", "risk"],
    )

    doc3 = ms.upload(
        b"""<html><body>
        <h1>Regulatory Filing</h1>
        <p>This filing covers the firm's <b>Basel III</b> capital requirements,
        including risk-weighted assets and leverage ratio.</p>
        </body></html>""",
        filename="regulatory_filing.html",
        title="Basel III Capital Filing",
        tags=["regulatory", "capital"],
    )

    doc4 = ms.upload(
        b"\x89PNG\r\n\x1a\n" + b"\x00" * 200,
        filename="risk_heatmap.png",
        title="Risk Heatmap",
        tags=["chart", "risk"],
    )

    doc5 = ms.upload(
        b"Portfolio optimization using mean-variance analysis. "
        b"The efficient frontier shows optimal risk-return tradeoffs.",
        filename="portfolio_optimization.txt",
        title="Portfolio Optimization Notes",
        tags=["research", "portfolio"],
    )

    return [doc1, doc2, doc3, doc4, doc5]


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoMedia:
    """Mirrors demo_media.py — upload → search → download → list → storable."""

    # ── 1. Upload ────────────────────────────────────────────────────

    def test_upload_text(self, uploaded_docs) -> None:
        doc = uploaded_docs[0]
        assert doc.title == "Interest Rate Swap Research"
        assert doc.content_type == "text/plain"
        assert doc.size > 0
        assert len(doc.extracted_text) > 0

    def test_upload_markdown(self, uploaded_docs) -> None:
        doc = uploaded_docs[1]
        assert doc.title == "Q1 Trading Summary"
        assert doc.content_type == "text/markdown"

    def test_upload_html(self, uploaded_docs) -> None:
        doc = uploaded_docs[2]
        assert doc.title == "Basel III Capital Filing"
        assert doc.content_type == "text/html"

    def test_upload_binary_no_extraction(self, uploaded_docs) -> None:
        doc = uploaded_docs[3]
        assert doc.title == "Risk Heatmap"
        assert doc.content_type == "image/png"
        assert not doc.has_text

    def test_upload_count(self, uploaded_docs) -> None:
        assert len(uploaded_docs) == 5

    # ── 2. Search ────────────────────────────────────────────────────

    def test_search_interest_rate_swap(self, ms, uploaded_docs) -> None:
        results = ms.search("interest rate swap")
        assert len(results) > 0
        titles = [r["title"] for r in results]
        assert any("Interest Rate" in t for t in titles)

    def test_search_equity_derivatives(self, ms, uploaded_docs) -> None:
        results = ms.search("equity derivatives volatility")
        assert len(results) > 0

    def test_search_basel(self, ms, uploaded_docs) -> None:
        results = ms.search("Basel capital")
        assert len(results) > 0

    def test_search_by_content_type(self, ms, uploaded_docs) -> None:
        results = ms.search("swap", content_type="text/plain")
        assert len(results) > 0
        assert all(r["content_type"] == "text/plain" for r in results)

    def test_search_by_tags(self, ms, uploaded_docs) -> None:
        results = ms.search("risk", tags=["research"])
        assert len(results) > 0

    # ── 3. Download ──────────────────────────────────────────────────

    def test_download_and_verify(self, ms, uploaded_docs) -> None:
        data = ms.download(uploaded_docs[0])
        assert b"interest rate swap" in data

    # ── 4. List & filter ─────────────────────────────────────────────

    def test_list_all_documents(self, ms, uploaded_docs) -> None:
        all_docs = ms.list()
        assert len(all_docs) >= 5

    def test_list_by_tags(self, ms, uploaded_docs) -> None:
        research = ms.list(tags=["research"])
        assert len(research) >= 2

    # ── 5. Storable features ─────────────────────────────────────────

    def test_find_by_entity_id(self, uploaded_docs) -> None:
        doc = uploaded_docs[0]
        found = Document.find(doc.entity_id)
        assert found is not None
        assert found.title == doc.title

    def test_version_history(self, uploaded_docs) -> None:
        doc = uploaded_docs[0]
        found = Document.find(doc.entity_id)
        assert found is not None
        history = found.history()
        assert len(history) >= 1
