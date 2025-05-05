from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass

from elasticsearch import Elasticsearch

from mcp.server.fastmcp import Context, FastMCP

# --- CONFIG ---
ES_HOST = "https://localhost:9200"
ES_USERNAME = "elastic"
ES_PASSWORD = "changeme"
ES_INDEX = "gkg"

# Named MCP server
mcp = FastMCP("ES App", dependencies=["elasticsearch"])

@dataclass
class AppContext:
    es: Elasticsearch


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Manage application lifecycle with Elasticsearch"""
    # Connect to Elasticsearch
    es = Elasticsearch(
        ES_HOST,
        basic_auth=(ES_USERNAME, ES_PASSWORD),
        verify_certs=False  # Set to True in production
    )
    try:
        yield AppContext(es=es)
    finally:
        # Optional: Close transport explicitly
        await es.close()


# # Attach lifespan
# mcp = FastMCP("ES App", lifespan=app_lifespan)


# @mcp.tool()
# def query_es(ctx: Context) -> str:
#     """Query Elasticsearch GKG index"""
#     es = ctx.request_context.lifespan_context.es
#     response = es.search(index=ES_INDEX, query={"match_all": {}})
#     return str(response)
