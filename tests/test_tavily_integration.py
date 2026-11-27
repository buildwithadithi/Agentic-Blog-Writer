import unittest
from unittest.mock import patch

from blog_writer.integrations import tavily


class TavilyIntegrationTests(unittest.TestCase):
    def test_uses_current_tavily_tool_and_normalizes_search_results(self):
        response = {
            "results": [
                {
                    "title": "Result title",
                    "url": "https://example.test/article",
                    "content": "Relevant snippet",
                    "published_date": "2026-11-27",
                }
            ]
        }
        with patch.object(tavily, "TavilySearch") as search_factory:
            search_factory.return_value.invoke.return_value = response

            results = tavily._tavily_search("test query", max_results=3)

        search_factory.assert_called_once_with(max_results=3)
        search_factory.return_value.invoke.assert_called_once_with(
            {"query": "test query"}
        )
        self.assertEqual(
            results,
            [
                {
                    "title": "Result title",
                    "url": "https://example.test/article",
                    "snippet": "Relevant snippet",
                    "published_at": "2026-11-27",
                    "source": None,
                }
            ],
        )

    def test_search_error_response_is_not_silently_treated_as_no_results(self):
        with patch.object(tavily, "TavilySearch") as search_factory:
            search_factory.return_value.invoke.return_value = {
                "error": RuntimeError("private API detail")
            }

            with self.assertRaisesRegex(RuntimeError, "Tavily search failed"):
                tavily._tavily_search("test query")

    def test_invalid_response_shape_raises_clear_error(self):
        with patch.object(tavily, "TavilySearch") as search_factory:
            search_factory.return_value.invoke.return_value = {"results": None}

            with self.assertRaisesRegex(TypeError, "results list"):
                tavily._tavily_search("test query")


if __name__ == "__main__":
    unittest.main()
