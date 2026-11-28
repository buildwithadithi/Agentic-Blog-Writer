from blog_writer.integrations.wordpress_mcp import (
    WordPressMCPClient,
    markdown_to_wordpress_html,
)
from blog_writer.state import State


async def wordpress_draft_node(state: State) -> dict:
    # 1. Only create a draft after content approval
    if state.get("review_status") != "approved":
        raise ValueError("Blog must be approved before creating a WordPress draft.")

    # 2. Prevent duplicate drafts when the graph resumes
    existing_post_id = state.get("wordpress_post_id")

    if existing_post_id:
        print(f"WordPress draft already exists: {existing_post_id}")

        return {
            "wordpress_post_id": existing_post_id,
            "wordpress_post_url": state.get("wordpress_post_url"),
            "wordpress_status": state.get("wordpress_status")
        }

    # 3. Get the blog content
    content = state.get("final")

    if not content:
        raise ValueError("Final blog content is missing.")

    # Use the topic as the initial post title
    title = state["topic"]

    # 4. Connect and create the WordPress draft
    client = WordPressMCPClient()

    try:
        await client.connect()

        content = await client.upload_images(
            content,
            state.get("image_specs", []),
        )
        content = markdown_to_wordpress_html(content)

        post = await client.create_post(
            title=title,
            content=content,
            confirmed=True
        )

        # 5. Extract the returned WordPress post details
        post_id = post["id"]
        post_url = post["link"]
        post_status = post["status"]

        print("\nWordPress draft created successfully!")
        print(f"Post ID: {post_id}")
        print(f"Title: {title}")
        print(f"Status: {post_status}")
        print(f"URL: {post_url}")

        return {
            "wordpress_post_id": post_id,
            "wordpress_post_url": post_url,
            "wordpress_status": post_status
        }

    finally:
        await client.close()
