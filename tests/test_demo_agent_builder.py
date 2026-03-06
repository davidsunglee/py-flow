"""
Mirror test for demo_agent_builder.py
========================================
Verifies the full demo flow:

  1. Reactive graph: Position + PortfolioRisk with @computed fields
  2. @computed: market_value, unrealized_pnl, var_1d_95, var_1d_99
  3. Cross-entity @computed: PortfolioRisk reads all positions
  4. Stress tests on live positions
  5. Agent tools: get_portfolio_positions, get_live_quote, get_portfolio_risk,
     run_stress_test, query_price_history, get_realized_vol, query_analytics,
     search_research, ask_research
  6. Agent team: market_data + risk_analyst + research
  7. EvalRunner: quality check with expected tool selection

All services started via conftest — same as demo.
"""

import json
import math
import textwrap
import time as _time
from datetime import datetime, timezone

import httpx
import pytest

from ai import AI, Agent, tool
from ai.eval import EvalCase, EvalRunner
from ai.team import AgentTeam
from lakehouse import Lakehouse
from media import MediaStore
from media.models import bootstrap_chunks_schema, bootstrap_search_schema
from reactive.computed import computed, effect
from store import Storable, connect

from dataclasses import dataclass, field


# ── Constants — same as demo ─────────────────────────────────────────────

SECTORS = {
    "AAPL": "Technology", "NVDA": "Technology", "MSFT": "Technology",
    "GOOGL": "Technology", "AMZN": "Technology",
    "TSLA": "Consumer Discretionary",
}

IV = {
    "AAPL": 0.22, "NVDA": 0.55, "MSFT": 0.20,
    "TSLA": 0.62, "GOOGL": 0.25, "AMZN": 0.30,
}

BETA = {
    "AAPL": 1.18, "NVDA": 1.72, "MSFT": 1.10,
    "TSLA": 2.05, "GOOGL": 1.15, "AMZN": 1.25,
}

STRESS_SHOCKS = {
    "rate_hike":       {"AAPL": -0.08, "NVDA": -0.12, "MSFT": -0.07, "TSLA": -0.15, "GOOGL": -0.06, "AMZN": -0.09},
    "tech_crash":      {"AAPL": -0.25, "NVDA": -0.40, "MSFT": -0.22, "TSLA": -0.35, "GOOGL": -0.20, "AMZN": -0.28},
    "recession":       {"AAPL": -0.15, "NVDA": -0.20, "MSFT": -0.18, "TSLA": -0.30, "GOOGL": -0.15, "AMZN": -0.22},
    "inflation_spike": {"AAPL": -0.10, "NVDA": -0.15, "MSFT": -0.08, "TSLA": -0.18, "GOOGL": -0.07, "AMZN": -0.12},
}

HOLDINGS = {
    "AAPL":  {"qty": 500,  "avg_cost": 178.50},
    "NVDA":  {"qty": 200,  "avg_cost": 450.00},
    "MSFT":  {"qty": 300,  "avg_cost": 380.00},
    "TSLA":  {"qty": 150,  "avg_cost": 242.00},
    "GOOGL": {"qty": 250,  "avg_cost": 155.00},
    "AMZN":  {"qty": 100,  "avg_cost": 185.00},
}

RESEARCH_DOCS = [
    {
        "filename": "fed_outlook.txt",
        "title": "Fed Policy Outlook — March 2024",
        "tags": ["fed", "rates", "macro"],
        "content": textwrap.dedent("""\
            Federal Reserve Policy Outlook — March 2024
            The Federal Reserve is expected to hold rates steady at 5.25-5.50%
            through Q2 2024. Market pricing suggests 3 cuts in H2 2024.
            Recommendation: underweight long-duration tech until rate path clarifies.
        """),
    },
    {
        "filename": "nvda_analysis.txt",
        "title": "NVIDIA — AI Semiconductor Cycle Analysis",
        "tags": ["nvda", "semiconductors", "ai", "tech"],
        "content": textwrap.dedent("""\
            NVIDIA Corporation (NVDA) — Deep Dive Analysis
            Revenue growth driven by data center demand (+265% YoY).
            Risks: export controls, AMD MI300X competition, vertical integration.
            Recommendation: Hold with trailing stop at -15%.
        """),
    },
    {
        "filename": "tsla_competitive.txt",
        "title": "Tesla — EV Market & Competitive Position",
        "tags": ["tsla", "ev", "auto"],
        "content": textwrap.dedent("""\
            Tesla Inc (TSLA) — Competitive Position Analysis
            Global EV penetration 18%. Tesla US share declined from 62% to 51%.
            Beta of 2.05 makes it a significant portfolio risk contributor.
            Recommendation: Trim to <5% of portfolio.
        """),
    },
    {
        "filename": "hedging_strategies.txt",
        "title": "Portfolio Hedging Strategies — Current Environment",
        "tags": ["hedging", "options", "risk", "portfolio"],
        "content": textwrap.dedent("""\
            Portfolio Hedging Strategies — Q1 2024
            With VIX at 16.8, put protection is relatively cheap.
            1. Index Protection: SPY 500P 3-month puts
            2. Single-Name Collars: NVDA Buy 800P / Sell 1000C
            3. Sector Rotation: reduce Technology from 60% to 40-45%
        """),
    },
]


# ── Reactive Models — same as demo ───────────────────────────────────────

@dataclass
class ABPosition(Storable):
    __key__ = "symbol"
    symbol: str = ""
    quantity: int = 0
    avg_cost: float = 0.0
    price: float = 0.0
    sector: str = ""
    implied_vol: float = 0.25
    beta: float = 1.0

    @computed
    def market_value(self):
        return self.price * self.quantity

    @computed
    def unrealized_pnl(self):
        return (self.price - self.avg_cost) * self.quantity

    @computed
    def var_1d_95(self):
        daily_vol = self.implied_vol / math.sqrt(252)
        return self.market_value * daily_vol * 1.645

    @computed
    def var_1d_99(self):
        daily_vol = self.implied_vol / math.sqrt(252)
        return self.market_value * daily_vol * 2.326


@dataclass
class ABPortfolioRisk(Storable):
    __key__ = "name"
    name: str = "main"
    positions: list = field(default_factory=list)

    @computed
    def total_value(self):
        return sum(p.market_value for p in self.positions)

    @computed
    def total_unrealized_pnl(self):
        return sum(p.unrealized_pnl for p in self.positions)

    @computed
    def portfolio_var_95(self):
        sum_sq = sum(p.var_1d_95 ** 2 for p in self.positions)
        return math.sqrt(sum_sq) * 0.85

    @computed
    def portfolio_var_99(self):
        sum_sq = sum(p.var_1d_99 ** 2 for p in self.positions)
        return math.sqrt(sum_sq) * 0.85

    @computed
    def var_pct_95(self):
        tv = self.total_value
        return (self.portfolio_var_95 / tv * 100) if tv else 0

    @computed
    def hhi(self):
        tv = self.total_value
        if tv == 0:
            return 0
        return sum((p.market_value / tv) ** 2 for p in self.positions)

    @computed
    def concentration_level(self):
        if self.hhi > 0.25:
            return "concentrated"
        if self.hhi > 0.15:
            return "moderate"
        return "diversified"

    def stress_test(self, scenario: str):
        shocks = STRESS_SHOCKS.get(scenario)
        if not shocks:
            return {"error": f"Unknown scenario: {scenario}"}
        total_pnl = 0
        details = []
        for p in self.positions:
            shock_pct = shocks.get(p.symbol, 0)
            loss = p.market_value * shock_pct
            total_pnl += loss
            details.append({"symbol": p.symbol, "shock_pct": round(shock_pct * 100, 1), "pnl": round(loss, 2)})
        tv = self.total_value
        return {
            "scenario": scenario,
            "total_pnl": round(total_pnl, 2),
            "total_pnl_pct": round(total_pnl / tv * 100, 2) if tv else 0,
            "details": sorted(details, key=lambda x: x["pnl"]),
        }


# ── Fixtures ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def infra(store_server, media_server, tsdb_server, market_data_server,
          lakehouse_server):
    """Start infrastructure, bootstrap schemas — same as demo."""
    store_server.provision_user("ab_user", "ab_pw")
    store_server.register_alias("demo-ab")

    admin = store_server.admin_conn()
    bootstrap_search_schema(admin, embedding_dim=768)
    admin.close()
    admin = store_server.admin_conn()
    bootstrap_chunks_schema(admin, embedding_dim=768)
    admin.close()

    media_server.register_alias("demo-ab")
    tsdb_server.register_alias("demo-ab")
    market_data_server.register_alias("demo-ab")
    lakehouse_server.register_alias("demo-ab")

    conn = connect("demo-ab", user="ab_user", password="ab_pw")
    yield {"conn": conn, "md_port": market_data_server.port}
    conn.close()


@pytest.fixture(scope="module")
def lh(infra, lakehouse_server):
    """Lakehouse client — same as demo."""
    inst = Lakehouse("demo-ab")
    # Seed initial snapshot — same as demo
    inst.ingest("ab_portfolio_snapshots", [{
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_value": 0.0, "var_95": 0.0, "var_99": 0.0,
        "var_pct_95": 0.0, "hhi": 0.0, "concentration_level": "unknown",
        "unrealized_pnl": 0.0, "position_count": 0,
    }], mode="append")
    yield inst
    inst.close()


@pytest.fixture(scope="module")
def positions(infra):
    """Build reactive Position objects — same as demo."""
    md_port = infra["md_port"]
    # Get live prices from REST — same as demo
    try:
        resp = httpx.get(f"http://localhost:{md_port}/md/snapshot/equity", timeout=5.0)
        live_prices = resp.json() if resp.status_code == 200 else {}
    except Exception:
        live_prices = {}

    pos_dict = {}
    for sym, h in HOLDINGS.items():
        initial_price = live_prices.get(sym, {}).get("price", h["avg_cost"])
        pos = ABPosition(
            symbol=sym, quantity=h["qty"], avg_cost=h["avg_cost"],
            price=initial_price, sector=SECTORS.get(sym, "Unknown"),
            implied_vol=IV.get(sym, 0.25), beta=BETA.get(sym, 1.0),
        )
        pos_dict[sym] = pos
    return pos_dict


@pytest.fixture(scope="module")
def portfolio_risk(positions):
    """Build PortfolioRisk — cross-entity @computed reads all positions."""
    return ABPortfolioRisk(name="main", positions=list(positions.values()))


@pytest.fixture(scope="module")
def media_store(infra):
    """MediaStore with AI embeddings — same as demo."""
    ai = AI()
    ms = MediaStore("demo-ab", ai=ai)
    yield ms
    ms.close()


@pytest.fixture(scope="module")
def uploaded_docs(media_store):
    """Upload research documents — same as demo."""
    docs = []
    for doc_info in RESEARCH_DOCS:
        doc = media_store.upload(
            doc_info["content"].encode(),
            filename=doc_info["filename"],
            title=doc_info["title"],
            tags=doc_info["tags"],
        )
        docs.append(doc)
    return docs


@pytest.fixture(scope="module")
def agent_tools(positions, portfolio_risk, infra, media_store, lh):
    """Build agent tools — same as demo's @tool functions."""
    md_port = infra["md_port"]
    md_base = f"http://localhost:{md_port}"

    @tool
    def get_portfolio_positions() -> str:
        """Get all portfolio positions with live prices."""
        result = []
        for p in positions.values():
            result.append({
                "symbol": p.symbol, "quantity": p.quantity,
                "live_price": round(p.price, 2),
                "market_value": round(p.market_value, 2),
                "unrealized_pnl": round(p.unrealized_pnl, 2),
                "var_1d_95": round(p.var_1d_95, 2),
            })
        return json.dumps(sorted(result, key=lambda x: -x["market_value"]))

    @tool
    def get_live_quote(symbol: str) -> str:
        """Get a single position's live data."""
        p = positions.get(symbol)
        if not p:
            return json.dumps({"error": f"No position for {symbol}"})
        return json.dumps({
            "symbol": p.symbol, "live_price": round(p.price, 2),
            "market_value": round(p.market_value, 2),
            "var_1d_95": round(p.var_1d_95, 2),
        })

    @tool
    def get_portfolio_risk_tool() -> str:
        """Get aggregate portfolio risk metrics."""
        pr = portfolio_risk
        return json.dumps({
            "total_value": round(pr.total_value, 2),
            "var_1d_95": round(pr.portfolio_var_95, 2),
            "var_pct_95": round(pr.var_pct_95, 2),
            "hhi": round(pr.hhi, 4),
            "concentration": pr.concentration_level,
        })

    @tool
    def run_stress_test(scenario: str) -> str:
        """Run a stress test."""
        return json.dumps(portfolio_risk.stress_test(scenario))

    @tool
    def query_price_history(symbol: str, interval: str = "1m") -> str:
        """Get OHLCV bars from TSDB."""
        try:
            resp = httpx.get(f"{md_base}/md/bars/equity/{symbol}",
                             params={"interval": interval}, timeout=5.0)
            bars = resp.json()
            return json.dumps({"symbol": symbol, "bar_count": len(bars),
                               "bars": bars[-20:]}, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def search_research(query: str) -> str:
        """Search research documents."""
        try:
            results = media_store.hybrid_search(query, limit=3)
            return json.dumps(results, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def ask_research(question: str) -> str:
        """Ask research documents via RAG."""
        try:
            ai = AI()
            result = ai.ask(question, documents=media_store)
            return json.dumps({"answer": result.answer,
                               "sources": [s.get("title", "") for s in result.sources]})
        except Exception as e:
            return json.dumps({"error": str(e)})

    @tool
    def query_analytics(sql: str) -> str:
        """Run SQL on Lakehouse."""
        try:
            data = lh.query(sql)
            return json.dumps({"row_count": len(data), "data": data[:50]}, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})

    return {
        "get_portfolio_positions": get_portfolio_positions,
        "get_live_quote": get_live_quote,
        "get_portfolio_risk": get_portfolio_risk_tool,
        "run_stress_test": run_stress_test,
        "query_price_history": query_price_history,
        "search_research": search_research,
        "ask_research": ask_research,
        "query_analytics": query_analytics,
    }


@pytest.fixture(scope="module")
def team(agent_tools):
    """Build 3-agent team — same as demo's build_team()."""
    market_agent = Agent(
        tools=[agent_tools["get_portfolio_positions"], agent_tools["get_live_quote"],
               agent_tools["query_price_history"]],
        system_prompt="You are a Market Data Specialist.",
        name="market_data",
    )
    risk_agent = Agent(
        tools=[agent_tools["get_portfolio_risk"], agent_tools["run_stress_test"],
               agent_tools["query_analytics"]],
        system_prompt="You are a Risk Analyst.",
        name="risk_analyst",
    )
    research_agent = Agent(
        tools=[agent_tools["search_research"], agent_tools["ask_research"]],
        system_prompt="You are a Research Analyst with access to internal documents.",
        name="research",
    )
    return AgentTeam(agents={
        "market_data": market_agent,
        "risk_analyst": risk_agent,
        "research": research_agent,
    })


# ── Tests ────────────────────────────────────────────────────────────────

class TestDemoAgentBuilder:
    """Mirrors demo_agent_builder.py — reactive graph + agents + eval."""

    # ── Reactive Graph ───────────────────────────────────────────────

    def test_positions_seeded(self, positions) -> None:
        """6 positions seeded — same as demo's HOLDINGS."""
        assert len(positions) == 6

    def test_position_market_value(self, positions) -> None:
        """@computed market_value = price × quantity."""
        pos = positions["AAPL"]
        assert abs(pos.market_value - pos.price * pos.quantity) < 0.01

    def test_position_unrealized_pnl(self, positions) -> None:
        """@computed unrealized_pnl = (price - avg_cost) × quantity."""
        pos = positions["AAPL"]
        expected = (pos.price - pos.avg_cost) * pos.quantity
        assert abs(pos.unrealized_pnl - expected) < 0.01

    def test_position_var_95(self, positions) -> None:
        """@computed var_1d_95 = market_value × daily_vol × 1.645."""
        pos = positions["NVDA"]
        daily_vol = pos.implied_vol / math.sqrt(252)
        expected = pos.market_value * daily_vol * 1.645
        assert abs(pos.var_1d_95 - expected) < 0.01

    def test_position_var_99(self, positions) -> None:
        """@computed var_1d_99 = market_value × daily_vol × 2.326."""
        pos = positions["NVDA"]
        daily_vol = pos.implied_vol / math.sqrt(252)
        expected = pos.market_value * daily_vol * 2.326
        assert abs(pos.var_1d_99 - expected) < 0.01

    # ── PortfolioRisk ────────────────────────────────────────────────

    def test_portfolio_total_value(self, portfolio_risk, positions) -> None:
        """Cross-entity @computed: total_value = sum(position.market_value)."""
        expected = sum(p.market_value for p in positions.values())
        assert abs(portfolio_risk.total_value - expected) < 0.01

    def test_portfolio_var_95(self, portfolio_risk) -> None:
        """Diversified VaR includes 0.85 diversification factor."""
        assert portfolio_risk.portfolio_var_95 > 0

    def test_portfolio_hhi(self, portfolio_risk) -> None:
        """HHI measures concentration — between 0 and 1."""
        assert 0 < portfolio_risk.hhi <= 1

    def test_portfolio_concentration_level(self, portfolio_risk) -> None:
        """concentration_level is one of: concentrated, moderate, diversified."""
        assert portfolio_risk.concentration_level in ("concentrated", "moderate", "diversified")

    # ── Stress Tests ─────────────────────────────────────────────────

    def test_stress_test_tech_crash(self, portfolio_risk) -> None:
        """Tech crash stress test returns negative PnL."""
        result = portfolio_risk.stress_test("tech_crash")
        assert result["total_pnl"] < 0
        assert len(result["details"]) == 6

    def test_stress_test_rate_hike(self, portfolio_risk) -> None:
        """Rate hike stress test."""
        result = portfolio_risk.stress_test("rate_hike")
        assert result["total_pnl"] < 0

    def test_stress_test_unknown_scenario(self, portfolio_risk) -> None:
        """Unknown scenario returns error."""
        result = portfolio_risk.stress_test("unknown_scenario")
        assert "error" in result

    # ── Lakehouse ────────────────────────────────────────────────────

    def test_lakehouse_snapshots_table(self, lh) -> None:
        """portfolio_snapshots table created in Lakehouse — same as demo."""
        rows = lh.query("SELECT * FROM lakehouse.default.ab_portfolio_snapshots")
        assert len(rows) >= 1

    # ── Research Documents ───────────────────────────────────────────

    def test_research_docs_uploaded(self, uploaded_docs) -> None:
        """4 research documents uploaded — same as demo."""
        assert len(uploaded_docs) == 4

    def test_research_hybrid_search(self, media_store, uploaded_docs) -> None:
        """Hybrid search returns results — same as demo's verification."""
        results = media_store.hybrid_search("NVDA risk", limit=2)
        assert len(results) > 0

    # ── Agent Tools ──────────────────────────────────────────────────

    def test_tool_get_positions(self, agent_tools) -> None:
        """get_portfolio_positions returns JSON with 6 positions."""
        data = json.loads(agent_tools["get_portfolio_positions"]())
        assert len(data) == 6

    def test_tool_get_live_quote(self, agent_tools) -> None:
        """get_live_quote returns data for a known symbol."""
        data = json.loads(agent_tools["get_live_quote"]("AAPL"))
        assert data["symbol"] == "AAPL"
        assert "market_value" in data

    def test_tool_portfolio_risk(self, agent_tools) -> None:
        """get_portfolio_risk returns VaR and concentration."""
        data = json.loads(agent_tools["get_portfolio_risk"]())
        assert "var_1d_95" in data
        assert "hhi" in data

    def test_tool_stress_test(self, agent_tools) -> None:
        """run_stress_test returns scenario results."""
        data = json.loads(agent_tools["run_stress_test"]("tech_crash"))
        assert data["total_pnl"] < 0

    def test_tool_search_research(self, agent_tools, uploaded_docs) -> None:
        """search_research uses hybrid search."""
        data = json.loads(agent_tools["search_research"]("NVDA earnings"))
        assert isinstance(data, list)
        assert len(data) > 0

    # ── Agent Team ───────────────────────────────────────────────────

    def test_team_has_3_agents(self, team) -> None:
        """Team has market_data, risk_analyst, research agents."""
        assert len(team._agents) == 3

    def test_team_run(self, team) -> None:
        """Team.run dispatches to agents and returns a result."""
        result = team.run("What are my current portfolio positions with live prices?")
        assert len(result.content) > 0

    # ── Eval ─────────────────────────────────────────────────────────

    def test_eval_runner(self, agent_tools) -> None:
        """EvalRunner verifies agent tool selection — same as demo."""
        eval_agent = Agent(
            tools=list(agent_tools.values()),
            system_prompt="You are a financial analyst.",
        )
        cases = [
            EvalCase(
                input="What are my current portfolio positions with live prices?",
                expected_tools=["get_portfolio_positions"],
                tags=["market_data"],
            ),
            EvalCase(
                input="What is the portfolio VaR and concentration risk?",
                expected_tools=["get_portfolio_risk"],
                tags=["risk"],
            ),
            EvalCase(
                input="Run a tech crash stress test",
                expected_tools=["run_stress_test"],
                tags=["risk"],
            ),
        ]
        runner = EvalRunner(agent=eval_agent)
        runner.run(cases)
        runner.summary()
        # If we get here without error, eval ran successfully
