import base64

import requests

from blog_writer.config import get_cloudflare_credentials


def _cloudflare_generate_image_bytes(prompt: str) -> bytes:
    account_id, api_token = get_cloudflare_credentials()

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/"
        f"{account_id}/ai/run/@cf/black-forest-labs/flux-1-schnell"
    )

    headers = {
        "Authorization": f"Bearer {api_token}"
    }

    payload = {
        "prompt": prompt
    }

    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=120
    )

    response.raise_for_status()

    data = response.json()

    image_base64 = data["result"]["image"]

    image_bytes = base64.b64decode(image_base64)

    return image_bytes
