import base64
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from blog_writer.integrations.wordpress_mcp import WordPressMCPClient
from blog_writer.nodes import wordpress


PNG_BYTES = b"\x89PNG\r\n\x1a\nmock-png"
JPEG_BYTES = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00mock-jpeg"


class MediaToolSession:
    def __init__(self):
        self.calls = []
        self.media = []
        self.fail_upload = False
        self.upload_error = "Upload rejected"
        self.expected_image_bytes = PNG_BYTES

    async def call_tool(self, tool_name, *, arguments):
        self.calls.append((tool_name, arguments))
        ability = arguments["name"]
        if ability == "wpcom-mcp/media-list":
            title = arguments["input"]["search"]
            matching = [item for item in self.media if item["title"] == title]
            return self._result({"data": matching})

        if ability == "wpcom/media-create":
            if self.fail_upload:
                return SimpleNamespace(
                    is_error=True,
                    structuredContent=None,
                    content=[SimpleNamespace(text=self.upload_error)],
                )
            input_data = arguments["input"]
            self.assert_upload_payload(arguments)
            self.media.append(
                {
                    "id": len(self.media) + 1,
                    "title": input_data["title"],
                    "mime_type": input_data["mime_type"],
                    "source_url": (
                        f"https://media.example.test/{input_data['filename']}"
                    ),
                }
            )
            return self._result({"data": self.media[-1]})

        raise AssertionError(f"Unexpected WordPress ability: {ability}")

    @staticmethod
    def _result(data):
        return SimpleNamespace(
            is_error=False,
            structuredContent=data,
            structured_content=None,
            content=[],
        )

    def assert_upload_payload(self, arguments):
        input_data = arguments["input"]
        assert (
            base64.b64decode(input_data["file_content_base64"])
            == self.expected_image_bytes
        )
        expected_mime = (
            "image/jpeg"
            if self.expected_image_bytes.startswith(b"\xff\xd8\xff")
            else "image/png"
        )
        assert input_data["mime_type"] == expected_mime
        assert arguments["user_confirmed"] is True
        assert arguments["summary"]


class WordPressMediaTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.previous_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)
        self.image_dir = Path(self.temp_dir.name) / "images"
        self.image_dir.mkdir()
        self.addAsyncCleanup(self._cleanup_temp_dir)

    async def _cleanup_temp_dir(self):
        os.chdir(self.previous_cwd)
        self.temp_dir.cleanup()

    def make_client(self):
        client = WordPressMCPClient()
        client.session = MediaToolSession()
        return client

    def write_png(self, filename):
        (self.image_dir / filename).write_bytes(PNG_BYTES)

    async def test_upload_replaces_local_reference_and_sends_confirmed_payload(self):
        self.write_png("cover.png")
        client = self.make_client()
        content = '<img src="../images/cover.png" alt="Cover">'

        result = await client.upload_images(
            content,
            [{"filename": "cover.png", "alt": "Cover image", "caption": "Cover"}],
        )

        self.assertEqual(
            result,
            '<img src="https://media.example.test/cover.png" alt="Cover">',
        )
        create_call = next(
            call for call in client.session.calls
            if call[1]["name"] == "wpcom/media-create"
        )
        self.assertEqual(
            base64.b64decode(
                create_call[1]["input"]["file_content_base64"]
            ),
            PNG_BYTES,
        )
        self.assertEqual(create_call[1]["input"]["alt_text"], "Cover image")
        self.assertEqual(create_call[1]["input"]["caption"], "Cover")
        self.assertTrue(create_call[1]["user_confirmed"])
        media = await client._find_uploaded_image(
            client.session.media[0]["title"],
            "image/png",
        )
        self.assertEqual(
            media,
            {
                "id": 1,
                "source_url": "https://media.example.test/cover.png",
            },
        )

    async def test_upload_maps_multiple_images(self):
        self.write_png("first.png")
        self.write_png("second.png")
        client = self.make_client()

        result = await client.upload_images(
            '<img src="../images/first.png"><img src="../images/second.png">'
        )

        self.assertEqual(
            result,
            '<img src="https://media.example.test/first.png">'
            '<img src="https://media.example.test/second.png">',
        )
        uploads = [
            call for call in client.session.calls
            if call[1]["name"] == "wpcom/media-create"
        ]
        self.assertEqual(len(uploads), 2)

    async def test_actual_jpeg_bytes_with_png_path_use_jpeg_upload_metadata(self):
        (self.image_dir / "cover.png").write_bytes(JPEG_BYTES)
        client = self.make_client()
        client.session.expected_image_bytes = JPEG_BYTES

        result = await client.upload_images(
            '<img src="../images/cover.png" alt="Cover">'
        )

        self.assertEqual(
            result,
            '<img src="https://media.example.test/cover.jpg" alt="Cover">',
        )
        create_call = next(
            call for call in client.session.calls
            if call[1]["name"] == "wpcom/media-create"
        )
        self.assertEqual(create_call[1]["input"]["filename"], "cover.jpg")
        self.assertEqual(create_call[1]["input"]["mime_type"], "image/jpeg")

    async def test_missing_image_fails_before_upload(self):
        client = self.make_client()

        with self.assertRaisesRegex(FileNotFoundError, "not found"):
            await client.upload_images('<img src="../images/missing.png">')

        self.assertEqual(client.session.calls, [])

    async def test_unsupported_image_format_is_rejected(self):
        (self.image_dir / "unsupported.bmp").write_bytes(b"BM-not-an-image")
        client = self.make_client()

        with self.assertRaisesRegex(ValueError, "unsupported or invalid"):
            await client.upload_images(
                '<img src="../images/unsupported.bmp">'
            )

        self.assertEqual(client.session.calls, [])

    async def test_upload_failure_is_reported(self):
        self.write_png("cover.png")
        client = self.make_client()
        client.session.fail_upload = True

        with self.assertRaisesRegex(RuntimeError, "WordPress operation failed"):
            await client.upload_images('<img src="../images/cover.png">')

    async def test_expired_wordpress_authentication_is_reported(self):
        self.write_png("cover.png")
        client = self.make_client()
        client.session.fail_upload = True
        client.session.upload_error = "401 Unauthorized: credentials expired"

        with self.assertRaisesRegex(RuntimeError, "401 Unauthorized"):
            await client.upload_images('<img src="../images/cover.png">')

    async def test_content_without_images_is_unchanged_and_does_not_call_mcp(self):
        client = self.make_client()
        content = "# Article\n\nNo image references."

        self.assertEqual(await client.upload_images(content), content)
        self.assertEqual(client.session.calls, [])

    async def test_image_markup_inside_code_fence_is_not_uploaded(self):
        client = self.make_client()
        content = '```html\n<img src="../images/example.png">\n```'

        self.assertEqual(await client.upload_images(content), content)
        self.assertEqual(client.session.calls, [])

    async def test_retry_reuses_uploaded_image_after_draft_failure(self):
        self.write_png("cover.png")
        client = self.make_client()
        client.connect = AsyncMock()
        client.close = AsyncMock()
        client.create_post = AsyncMock(
            side_effect=[
                RuntimeError("Temporary draft failure"),
                {
                    "id": 42,
                    "link": "https://example.test/draft/42",
                    "status": "draft",
                },
            ]
        )
        state = {
            "review_status": "approved",
            "topic": "Test article",
            "final": '<img src="../images/cover.png">',
            "image_specs": [{"filename": "cover.png"}],
            "wordpress_post_id": None,
        }

        with patch.object(
            wordpress,
            "WordPressMCPClient",
            return_value=client,
        ):
            with self.assertRaisesRegex(RuntimeError, "Temporary draft failure"):
                await wordpress.wordpress_draft_node(state)
            result = await wordpress.wordpress_draft_node(state)

        create_calls = [
            call for call in client.session.calls
            if call[1]["name"] == "wpcom/media-create"
        ]
        self.assertEqual(len(create_calls), 1)
        self.assertEqual(result["wordpress_post_id"], 42)
        self.assertEqual(client.create_post.await_count, 2)
        self.assertEqual(
            client.create_post.await_args.kwargs["content"],
            '<img src="https://media.example.test/cover.png">',
        )


if __name__ == "__main__":
    unittest.main()
