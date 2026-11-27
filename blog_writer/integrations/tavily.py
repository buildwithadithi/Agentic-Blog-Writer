from typing import List

from langchain_tavily import TavilySearch


def _tavily_search(query: str, max_results: int = 5) -> List[dict]:
    tool = TavilySearch(max_results=max_results)
    response = tool.invoke({"query": query})

    if not isinstance(response, dict):
        raise TypeError("Tavily search response must be a mapping.")
    if response.get("error") is not None:
        raise RuntimeError("Tavily search failed.")

    results = response.get("results")
    if not isinstance(results, list):
        raise TypeError("Tavily search response must contain a results list.")

    normalized: List[dict] = []
    for result in results:
        normalized.append(
            {
                "title": result.get("title") or "",
                "url": result.get("url") or "",
                "snippet": result.get("content") or result.get("snippet") or "",
                "published_at": result.get("published_date") or result.get("published_at"),
                "source": result.get("source"),
            }
        )
    return normalized
