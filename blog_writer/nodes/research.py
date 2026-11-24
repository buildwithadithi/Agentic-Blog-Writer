from typing import List

from langchain_core.messages import HumanMessage, SystemMessage

from blog_writer.integrations.tavily import _tavily_search
from blog_writer.llm import llm
from blog_writer.models import EvidencePack
from blog_writer.state import State

RESEARCH_SYSTEM = """You are a research synthesizer for technical writing.

Given raw web search results, produce a deduplicated list of EvidenceItem objects.

Rules:
- Only include items with a non-empty url.
- Prefer relevant + authoritative sources (company blogs, docs, reputable outlets).
- If a published date is explicitly present in the result payload, keep it as YYYY-MM-DD.
  If missing or unclear, set published_at=null. Do NOT guess.
- Keep snippets short.
- Deduplicate by URL.
"""


def research_node(state: State) -> dict:
    # take the first 10 queries from state
    queries = (state.get("queries", []) or [])
    max_results = 6

    raw_results: List[dict] = []

    for query in queries:
        raw_results.extend(_tavily_search(query, max_results=max_results))

    if not raw_results:
        return {"evidence": []}

    extractor = llm.with_structured_output(EvidencePack)
    pack = extractor.invoke(
        [
            SystemMessage(content=RESEARCH_SYSTEM),
            HumanMessage(content=f"Raw results:\n{raw_results}"),
        ]
    )

    # Deduplicate by URL
    dedup = {}
    for evidence in pack.evidence:
        if evidence.url:
            dedup[evidence.url] = evidence

    return {"evidence": list(dedup.values())}
