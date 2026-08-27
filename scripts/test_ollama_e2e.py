#!/usr/bin/env python3
"""
E2E Test: Ollama local LLM — Factory Pattern + Pipeline Generation

Validates the full chain:
  1. Ollama reachability
  2. LLMClientFactory creates an OpenAILLMClient pointed at Ollama
  3. BaseLLMClient.complete() returns text
  4. BaseLLMClient.acomplete() returns text (async)
  5. NL → structured pipeline conversion via apex_workflow._convert_nl_to_structured
  6. Data Agent API health (if running at localhost:8001)

Usage (from project root):
  python scripts/test_ollama_e2e.py
  python scripts/test_ollama_e2e.py --model gemma3:4b
  python scripts/test_ollama_e2e.py --model mistral:7b
"""
import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Path setup ────────────────────────────────────────────────────────────────
# Two entries needed:
#   1. Project root  → enables  `from agents.data_agent.src.xxx import ...`
#   2. agents/data_agent → enables `from src.xxx import ...` used inside
#      agents/data_agent/src/__init__.py re-exports
ROOT = Path(__file__).parent.parent
DATA_AGENT_ROOT = ROOT / "agents" / "data_agent"
sys.path.insert(0, str(DATA_AGENT_ROOT))
sys.path.insert(0, str(ROOT))

# Force Ollama provider for this test run
os.environ.setdefault("LLM_PROVIDER", "ollama")
os.environ.setdefault("LLM_MODEL", "llama3.2:3b")
os.environ.setdefault("OLLAMA_BASE_URL", "http://localhost:11434/v1")
os.environ.setdefault("ENVIRONMENT", "dev")
os.environ.setdefault("DATABASE_URL", "postgresql://admin:admin123@localhost:5432/agentdb")


PASS = "PASS"
FAIL = "FAIL"
INFO = "INFO"


def log(level: str, msg: str):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}")


def section(title: str):
    print(f"\n{'-' * 60}")
    print(f"  {title}")
    print(f"{'-' * 60}")


results: list[tuple[str, bool, str]] = []


def record(name: str, passed: bool, detail: str = ""):
    results.append((name, passed, detail))
    level = PASS if passed else FAIL
    log(level, f"{name}{': ' + detail if detail else ''}")


# =============================================================================
# Test 1: Ollama reachability
# =============================================================================
def test_ollama_health(model: str):
    section("1. Ollama Server Health")
    import urllib.request
    import urllib.error

    try:
        with urllib.request.urlopen("http://localhost:11434/api/tags", timeout=5) as r:
            data = json.loads(r.read())
        names = [m["name"] for m in data.get("models", [])]
        log(INFO, f"Available models: {names}")

        if model in names:
            record("Model available", True, model)
        else:
            record("Model available", False, f"{model} not found — run: ollama pull {model}")
            print(f"\n  Run:  ollama pull {model}\n")
            sys.exit(1)
    except Exception as e:
        record("Ollama server", False, str(e))
        print(f"\n  Ollama is not running. Start it with:  ollama serve\n")
        sys.exit(1)


# =============================================================================
# Test 2: LLMClientFactory — sync complete()
# =============================================================================
def test_factory_sync(model: str):
    section("2. LLMClientFactory → BaseLLMClient.complete() [sync]")
    from agents.data_agent.src.config.settings import get_settings, clear_settings_cache
    from agents.data_agent.src.llm import LLMClientFactory

    # Override env and reload settings
    os.environ["LLM_PROVIDER"] = "ollama"
    os.environ["LLM_MODEL"] = model
    os.environ["OLLAMA_BASE_URL"] = "http://localhost:11434/v1"
    clear_settings_cache()
    settings = get_settings()

    log(INFO, f"provider={settings.llm_provider}  model={settings.llm_model}  base_url={settings.ollama_base_url}")

    try:
        client = LLMClientFactory.create(settings)
        record("Factory creates client", True, type(client).__name__)
    except Exception as e:
        record("Factory creates client", False, str(e))
        return

    t0 = time.monotonic()
    try:
        response = client.complete(
            messages=[{"role": "user", "content": "Say exactly: OLLAMA_OK"}],
            max_tokens=20,
        )
        elapsed = int((time.monotonic() - t0) * 1000)
        log(INFO, f"Response ({elapsed}ms): {response.strip()!r}")
        record("sync complete()", len(response) > 0, f"{elapsed}ms")
    except Exception as e:
        record("sync complete()", False, str(e))


# =============================================================================
# Test 3: LLMClientFactory — async acomplete()
# =============================================================================
async def _async_complete(model: str) -> tuple[bool, str]:
    from agents.data_agent.src.config.settings import get_settings
    from agents.data_agent.src.llm import LLMClientFactory

    settings = get_settings()
    client = LLMClientFactory.create(settings)

    t0 = time.monotonic()
    response = await client.acomplete(
        messages=[{"role": "user", "content": "Reply with one word: ASYNC_OK"}],
        max_tokens=20,
    )
    elapsed = int((time.monotonic() - t0) * 1000)
    return len(response) > 0, f"{elapsed}ms — {response.strip()!r}"


def test_factory_async(model: str):
    section("3. LLMClientFactory → BaseLLMClient.acomplete() [async]")
    try:
        ok, detail = asyncio.run(_async_complete(model))
        record("async acomplete()", ok, detail)
    except Exception as e:
        record("async acomplete()", False, str(e))


# =============================================================================
# Test 4: NL → Structured pipeline conversion
# =============================================================================
def test_nl_conversion(model: str):
    section("4. NL → Structured Pipeline (apex_workflow._convert_nl_to_structured)")

    try:
        from agents.data_agent.src.graphs.apex_workflow import _convert_nl_to_request
    except ImportError as e:
        record("import apex_workflow", False, str(e))
        return

    record("import apex_workflow", True)

    nl_input = {
        "natural_language": (
            "Create a daily batch pipeline that reads CSV sales data "
            "from GCS bucket gs://my-bucket/sales/raw/ and loads it into "
            "BigQuery dataset sales_dw, table daily_sales in the gold zone. "
            "The CSV has order_id, customer_id, product_id, quantity, price, order_date columns."
        ),
        "created_by": "ollama_test@local",
    }

    # Clear the singleton so it picks up the ollama settings we set above
    from agents.data_agent.src.llm.factory import get_llm_client
    get_llm_client.cache_clear()

    log(INFO, f"NL input: {nl_input['natural_language'][:80]}...")
    t0 = time.monotonic()
    try:
        result = _convert_nl_to_request(nl_input)
        elapsed = int((time.monotonic() - t0) * 1000)

        # Validate key fields
        feed = result.get("feed", {})
        source = result.get("source", {})
        target = result.get("target", {})
        confidence = result.get("confidence", 0)

        log(INFO, f"feed_name:   {feed.get('feed_name', 'N/A')}")
        log(INFO, f"source_type: {source.get('source_type', 'N/A')}")
        log(INFO, f"target:      {target.get('bq_dataset', 'N/A')}.{target.get('bq_table', 'N/A')}")
        log(INFO, f"confidence:  {confidence}")
        log(INFO, f"elapsed:     {elapsed}ms")

        checks = [
            bool(feed.get("feed_name")),
            bool(source.get("source_type")),
            bool(target.get("bq_dataset")),
            confidence >= 0.5,
        ]
        record("NL conversion produces structured output", all(checks), f"{elapsed}ms  confidence={confidence}")

        # Pretty-print abbreviated result
        print(json.dumps({
            "feed": feed,
            "source_type": source.get("source_type"),
            "target": target,
            "confidence": confidence,
        }, indent=2, default=str))

    except ValueError as e:
        # Low confidence is a valid model behaviour for small models
        elapsed = int((time.monotonic() - t0) * 1000)
        if "confidence too low" in str(e).lower():
            record("NL conversion produces structured output", False,
                   f"confidence too low ({elapsed}ms) — try a larger model like mistral:7b")
        else:
            record("NL conversion produces structured output", False, str(e))
    except Exception as e:
        record("NL conversion produces structured output", False, str(e))


# =============================================================================
# Test 5: NLTransformProcessor with Ollama
# =============================================================================
async def _test_nl_transform_async(model: str) -> tuple[bool, str]:
    from agents.data_agent.src.parsers.nl_transform_processor import NLTransformProcessor, TransformRequest
    from agents.data_agent.src.config.settings import get_settings
    from agents.data_agent.src.llm import LLMClientFactory

    settings = get_settings()
    llm_client = LLMClientFactory.create(settings)

    processor = NLTransformProcessor(llm_client=llm_client, validate_code=True)

    request = TransformRequest(
        description="filter rows where status equals COMPLETED",
        source_schema={
            "order_id": "string",
            "customer_id": "string",
            "status": "string",
            "amount": "decimal",
        },
    )

    t0 = time.monotonic()
    result = await processor.generate_transform(request)
    elapsed = int((time.monotonic() - t0) * 1000)
    return bool(result.pyspark_code), f"{elapsed}ms  intent={result.intent.value}  confidence={result.confidence:.2f}"


def test_nl_transform(model: str):
    section("5. NLTransformProcessor with Ollama (acomplete)")
    try:
        ok, detail = asyncio.run(_test_nl_transform_async(model))
        record("NLTransformProcessor.generate_transform()", ok, detail)
    except Exception as e:
        record("NLTransformProcessor.generate_transform()", False, str(e))


# =============================================================================
# Test 6: Data Agent API health (optional — needs server running)
# =============================================================================
def test_api_health():
    section("6. Data Agent API (localhost:8001) — optional")
    import urllib.request
    import urllib.error

    try:
        with urllib.request.urlopen("http://localhost:8001/health", timeout=3) as r:
            data = json.loads(r.read())
        record("API health", True, str(data.get("status", "ok")))
    except urllib.error.URLError:
        log(INFO, "API not running — skipping (start with: cd agents/data_agent && uvicorn src.api.main:app --port 8001)")
        results.append(("Data Agent API", None, "skipped"))


# =============================================================================
# Summary
# =============================================================================
def print_summary():
    section("RESULTS SUMMARY")
    passed = sum(1 for _, ok, _ in results if ok is True)
    failed = sum(1 for _, ok, _ in results if ok is False)
    skipped = sum(1 for _, ok, _ in results if ok is None)

    for name, ok, detail in results:
        if ok is True:
            status = "[PASS]"
        elif ok is False:
            status = "[FAIL]"
        else:
            status = "[SKIP]"
        detail_str = f" -- {detail}" if detail else ""
        print(f"  {status}  {name}{detail_str}")

    print()
    print(f"  Passed: {passed}  Failed: {failed}  Skipped: {skipped}")
    print()
    return failed == 0


# =============================================================================
# Entry point
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="E2E Ollama test")
    parser.add_argument("--model", default=os.getenv("LLM_MODEL", "llama3.2:3b"),
                        help="Ollama model name (default: llama3.2:3b)")
    args = parser.parse_args()

    model = args.model
    os.environ["LLM_MODEL"] = model

    print(f"\n{'=' * 60}")
    print(f"  Ollama E2E Test -- model: {model}")
    print(f"{'=' * 60}")

    test_ollama_health(model)
    test_factory_sync(model)
    test_factory_async(model)
    test_nl_conversion(model)
    test_nl_transform(model)
    test_api_health()

    ok = print_summary()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
