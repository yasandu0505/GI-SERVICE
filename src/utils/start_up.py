import httpx

async def prepare_cache(majorKind: str, minorKind: str):
    url = "https://aaf8ece1-3077-4a52-ab05-183a424f6d93-dev.e1-us-east-azure.choreoapis.dev/data-platform/query-api/v1.0/v1/entities/search"

    payload = {
        "id": "",
        "kind": {
            "major": majorKind,
            "minor": minorKind
        },
        "name": "",
        "created": "",
        "terminated": ""
    }

    headers = {
        "Content-Type": "application/json",
        # "Authorization": f"Bearer {token}"
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
    except Exception as e:
        data = {"error": str(e)}

    return data
