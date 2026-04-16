#!/usr/bin/env python3
"""
Create an Atlas Vector Search auto-embed index on the
reviews_2023_33categories_100keach_Health_and_Household view (Cluster1).

The index automatically generates embeddings from the `text` field
using Voyage AI and stores them as scalar-quantized vectors.
"""

from pymongo import MongoClient
from pymongo.operations import SearchIndexModel

CLUSTER1_URI = 'mongodb+srv://prakul_test_3:uWkQJho8D3yUUkNc@cluster1.5ssvrrc.mongodb.net/?appName=Cluster1'
DB_NAME      = 'amazon-reviews'
VIEW_NAME    = 'reviews_2023_33categories_100keach_Health_and_Household'
INDEX_NAME   = 'auto_embed_vector_index'

index_definition = SearchIndexModel(
    name=INDEX_NAME,
    type="vectorSearch",
    definition={
        "fields": [
            {
                "type": "autoEmbed",
                "path": "text",
                "modality": "text",
                "model": "voyage-4" 
            }
        ]
    })

def main():
    client = MongoClient(CLUSTER1_URI, serverSelectionTimeoutMS=10_000)
    print(client[DB_NAME].list_collection_names())
    collection = client[DB_NAME][VIEW_NAME]

    print(f"Creating auto-embed vector search index '{INDEX_NAME}' on '{VIEW_NAME}' ...")
    collection.create_search_index(model=index_definition)
    print("Index creation initiated successfully.")
    print("Note: Atlas builds the index asynchronously — check Atlas UI for build progress.")

    client.close()

if __name__ == "__main__":
    main()
