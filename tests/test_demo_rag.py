"""
Mirror test for demo_rag.py
==============================
Verifies the full demo flow:

  1. Upload financial documents (auto-chunk + embed)
  2. Search three ways: full-text, semantic, hybrid
  3. RAG: ask questions grounded in documents
  4. Structured extraction from text
  5. Direct generation + streaming
  6. Tool calling: LLM searches autonomously
"""

import textwrap

import pytest

from ai import AI
from media import MediaStore
from media.models import bootstrap_chunks_schema, bootstrap_search_schema
from store import connect


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def infra(store_server, media_server):
    """Bootstrap schemas + user — same as demo setup."""
    store_server.provision_user("rag_user", "rag_pw")
    store_server.register_alias("demo-rag")

    admin_conn = store_server.admin_conn()
    bootstrap_search_schema(admin_conn, embedding_dim=768)
    admin_conn.close()

    admin_conn = store_server.admin_conn()
    bootstrap_chunks_schema(admin_conn, embedding_dim=768)
    admin_conn.close()

    media_server.register_alias("demo-rag")

    conn = connect("demo-rag", user="rag_user", password="rag_pw")
    yield {"conn": conn}
    conn.close()


@pytest.fixture(scope="module")
def ai_client():
    """AI client — same as demo's AI()."""
    return AI()


@pytest.fixture(scope="module")
def ms(infra, ai_client, media_server):
    """MediaStore with AI-powered embeddings — same as demo."""
    store = MediaStore("demo-rag", ai=ai_client)
    yield store
    store.close()


@pytest.fixture(scope="module")
def docs(ms):
    """Upload all 4 demo documents — same content as demo_rag.py."""
    uploaded = []

    doc = ms.upload(
        textwrap.dedent("""\
        Credit Default Swaps (CDS) are financial derivatives that allow investors
        to transfer credit risk. The buyer of a CDS makes periodic payments to the
        seller and receives a payoff if a credit event occurs on the reference entity.

        CDS spreads reflect the market's perception of credit risk. A wider spread
        indicates higher perceived default probability. The CDS market grew rapidly
        before the 2008 financial crisis and played a significant role in the
        systemic risk that led to the collapse of major financial institutions.

        Key terms: reference entity, credit event, notional amount, premium leg,
        protection leg, recovery rate, ISDA documentation.
        """).encode(),
        filename="cds_overview.txt",
        title="Credit Default Swaps Overview",
        tags=["derivatives", "credit", "risk"],
    )
    uploaded.append(doc)

    doc = ms.upload(
        textwrap.dedent("""\
        Basel III Capital Requirements

        The Basel III framework strengthens bank capital requirements through:

        1. Common Equity Tier 1 (CET1): Minimum 4.5% of risk-weighted assets
        2. Tier 1 Capital: Minimum 6% of risk-weighted assets
        3. Total Capital: Minimum 8% of risk-weighted assets
        4. Capital Conservation Buffer: Additional 2.5% CET1
        5. Countercyclical Buffer: 0-2.5% at national discretion

        The Leverage Ratio requires Tier 1 capital of at least 3% of total
        exposures. The Liquidity Coverage Ratio (LCR) requires sufficient
        high-quality liquid assets to cover 30-day net cash outflows.

        Banks must also report the Net Stable Funding Ratio (NSFR) to ensure
        long-term funding stability.
        """).encode(),
        filename="basel3_requirements.txt",
        title="Basel III Capital Requirements",
        tags=["regulatory", "capital", "banking"],
    )
    uploaded.append(doc)

    doc = ms.upload(
        textwrap.dedent("""\
        Interest Rate Swap Valuation

        An interest rate swap (IRS) exchanges fixed-rate payments for floating-rate
        payments on a notional principal. Valuation involves:

        1. Discount factors derived from the yield curve
        2. Forward rates for projecting floating leg cash flows
        3. Present value of fixed leg minus present value of floating leg

        The swap rate is the fixed rate that makes the swap NPV equal to zero
        at inception. DV01 (dollar value of a basis point) measures sensitivity
        to a 1bp parallel shift in the yield curve.

        Common day count conventions: ACT/360 for floating, 30/360 for fixed.
        Payment frequencies: quarterly floating, semi-annual fixed.

        Mark-to-market P&L = current NPV minus inception NPV.
        """).encode(),
        filename="irs_valuation.txt",
        title="Interest Rate Swap Valuation",
        tags=["derivatives", "rates", "pricing"],
    )
    uploaded.append(doc)

    doc = ms.upload(
        textwrap.dedent("""\
        Portfolio Risk Metrics

        Value at Risk (VaR) estimates the maximum expected loss over a given
        time horizon at a specified confidence level. A 1-day 99% VaR of $5M
        means there is a 1% chance of losing more than $5M in one day.

        Expected Shortfall (ES), also called CVaR, measures the average loss
        in the worst X% of cases. ES is considered a more coherent risk measure
        than VaR because it satisfies subadditivity.

        Stress testing involves hypothetical or historical scenarios:
        - 2008 Financial Crisis replay
        - +200bp parallel rate shock
        - Equity market crash (-30%)
        - Credit spread widening (+500bp)

        Greeks (Delta, Gamma, Vega, Theta, Rho) measure option sensitivities.
        """).encode(),
        filename="risk_metrics.txt",
        title="Portfolio Risk Metrics",
        tags=["risk", "portfolio", "var"],
    )
    uploaded.append(doc)

    return uploaded


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoRag:
    """Mirrors demo_rag.py — upload → search → RAG → extract → generate → tools."""

    # ── 1. Upload ────────────────────────────────────────────────────

    def test_upload_count(self, docs) -> None:
        assert len(docs) == 4

    def test_cds_document_uploaded(self, docs) -> None:
        assert docs[0].title == "Credit Default Swaps Overview"
        assert docs[0].size > 0

    def test_basel_document_uploaded(self, docs) -> None:
        assert docs[1].title == "Basel III Capital Requirements"

    def test_irs_document_uploaded(self, docs) -> None:
        assert docs[2].title == "Interest Rate Swap Valuation"

    def test_risk_document_uploaded(self, docs) -> None:
        assert docs[3].title == "Portfolio Risk Metrics"

    # ── 2. Search — three modes ──────────────────────────────────────

    def test_fulltext_search(self, ms, docs) -> None:
        """Full-text search (keyword matching) — same as demo."""
        results = ms.search("credit risk derivatives", limit=3)
        assert len(results) > 0

    def test_semantic_search(self, ms, docs) -> None:
        """Semantic search (meaning-based) — same as demo."""
        results = ms.semantic_search("credit risk derivatives", limit=3)
        assert len(results) > 0

    def test_hybrid_search(self, ms, docs) -> None:
        """Hybrid search (RRF fusion) — same as demo."""
        results = ms.hybrid_search("credit risk derivatives", limit=3)
        assert len(results) > 0
        assert "rrf_score" in results[0]

    # ── 3. RAG ───────────────────────────────────────────────────────

    def test_rag_cds_question(self, ai_client, ms, docs) -> None:
        """RAG: What is a credit default swap?"""
        result = ai_client.ask("What is a credit default swap and how does it work?",
                               documents=ms, limit=3)
        assert len(result.answer) > 0
        assert len(result.sources) > 0

    def test_rag_basel_question(self, ai_client, ms, docs) -> None:
        """RAG: What are Basel III minimum capital requirements?"""
        result = ai_client.ask("What are the Basel III minimum capital requirements?",
                               documents=ms, limit=3)
        assert len(result.answer) > 0

    def test_rag_dv01_question(self, ai_client, ms, docs) -> None:
        """RAG: How is DV01 used in IRS valuation?"""
        result = ai_client.ask("How is DV01 used in interest rate swap valuation?",
                               documents=ms, limit=3)
        assert len(result.answer) > 0

    def test_rag_var_question(self, ai_client, ms, docs) -> None:
        """RAG: What is the difference between VaR and Expected Shortfall?"""
        result = ai_client.ask(
            "What is the difference between VaR and Expected Shortfall?",
            documents=ms, limit=3)
        assert len(result.answer) > 0

    # ── 4. Structured extraction ─────────────────────────────────────

    def test_structured_extraction(self, ai_client) -> None:
        """Extract structured data from earnings text — same as demo."""
        text = (
            "Goldman Sachs reported Q3 2024 earnings: revenue of $12.7 billion, "
            "net income of $2.99 billion, and earnings per share of $8.40. "
            "The investment banking division generated $1.87 billion in fees. "
            "Trading revenue was $6.4 billion. ROE was 10.4%."
        )
        result = ai_client.extract(
            text=text,
            schema={
                "type": "object",
                "properties": {
                    "company": {"type": "string"},
                    "quarter": {"type": "string"},
                    "revenue_billions": {"type": "number"},
                    "net_income_billions": {"type": "number"},
                    "eps": {"type": "number"},
                    "ib_fees_billions": {"type": "number"},
                    "trading_revenue_billions": {"type": "number"},
                    "roe_pct": {"type": "number"},
                },
                "required": ["company", "quarter", "revenue_billions"],
            },
        )
        assert result.data is not None
        assert "company" in result.data
        assert result.data["company"].lower().startswith("goldman")

    # ── 5. Direct generation + streaming ─────────────────────────────

    def test_direct_generation(self, ai_client) -> None:
        """Non-streaming generation — same as demo."""
        response = ai_client.generate(
            "Explain the difference between systematic and idiosyncratic risk in one sentence.",
            temperature=0.3,
        )
        assert len(response.content) > 0

    def test_streaming_generation(self, ai_client) -> None:
        """Streaming generation — same as demo."""
        chunks = list(ai_client.stream(
            "What is convexity in fixed income? Answer in one sentence."
        ))
        full_text = "".join(chunks)
        assert len(full_text) > 0

    # ── 6. Tool calling ──────────────────────────────────────────────

    def test_search_tools(self, ai_client, ms, docs) -> None:
        """Tool calling — LLM uses search tools autonomously."""
        tools = ai_client.search_tools(ms)
        assert len(tools) > 0

        response = ai_client.run_tool_loop(
            "Search for documents about Basel capital requirements and summarize what you find.",
            tools=tools,
        )
        # The LLM should either produce content or make tool calls (or both)
        assert len(response.content) > 0 or len(response.tool_calls) > 0
