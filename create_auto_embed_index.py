#!/usr/bin/env python3
"""
Create an Atlas Vector Search auto-embed index on the
reviews_2023_33categories_100keach_Health_and_Household view (Cluster1)
using the Atlas Admin API directly (supports views).
"""

import json
import os
import requests
from requests.auth import HTTPDigestAuth

# ---------------------------------------------------------------------------
# Atlas Admin API credentials
# Set these via environment variables or fill in directly.
# ---------------------------------------------------------------------------
ATLAS_PUBLIC_KEY  = os.getenv("ATLAS_PUBLIC_KEY",  "rqubxbto")
ATLAS_PRIVATE_KEY = os.getenv("ATLAS_PRIVATE_KEY", "c01b6442-6e29-498f-81bc-efcad2812d43")
ATLAS_PROJECT_ID  = os.getenv("ATLAS_PROJECT_ID",  "637bd10077946f6fbeadeb68")
CLUSTER_NAME      = os.getenv("ATLAS_CLUSTER_NAME", "Cluster1")

DB_NAME    = "amazon-reviews"
VIEW_NAME  = "reviews_2023_33categories_100keach_Health_and_Household"
INDEX_NAME = "auto_embed_vector_index"

ATLAS_API_BASE = "https://cloud.mongodb.com/api/atlas/v2"

index_definition = {
    "name": INDEX_NAME,
    "type": "vectorSearch",
    "database": DB_NAME,
    "collectionName": VIEW_NAME,
    "definition": {
        "fields": [
            {
                "type": "autoEmbed",
                "path": "text",
                "modality": "text",
                "model": "voyage-4"
            }
        ]
    }
}

def main():
    url = f"{ATLAS_API_BASE}/groups/{ATLAS_PROJECT_ID}/clusters/{CLUSTER_NAME}/search/indexes"

    print(f"Creating auto-embed vector search index '{INDEX_NAME}' on '{VIEW_NAME}' via Atlas Admin API ...")

    response = requests.post(
        url,
        auth=HTTPDigestAuth(ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY),
        headers={"Content-Type": "application/json", "Accept": "application/vnd.atlas.2024-05-30+json"},
        data=json.dumps(index_definition),
    )

    if response.status_code in (200, 201):
        result = response.json()
        print("Index creation initiated successfully.")
        print(f"Index ID : {result.get('indexID') or result.get('name')}")
        print(f"Status   : {result.get('status', 'PENDING')}")
        print("Note: Atlas builds the index asynchronously — check Atlas UI for build progress.")
    else:
        print(f"Error {response.status_code}: {response.text}")

if __name__ == "__main__":
    main()
