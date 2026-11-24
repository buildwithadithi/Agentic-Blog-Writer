from typing import List

from langchain_community.tools.tavily_search import TavilySearchResults


def _tavily_search(query: str, max_results: int = 5) -> List[dict]:
    tool = TavilySearchResults(max_results=max_results)
    results = tool.invoke({"query": query})

    normalized: List[dict] = []
    for result in results or []:
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
