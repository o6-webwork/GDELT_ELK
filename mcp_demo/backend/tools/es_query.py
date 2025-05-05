from mcp.server.fastmcp import Context
from backend.main import mcp  # ensures tool is registered to server

ES_INDEX = "gkg"

@mcp.tool()
def query_es(ctx: Context, keyword: str) -> str:
    es = ctx.request_context.lifespan_context.es
    query = {
        "query": {
            "match": {"content": keyword}
        }
    }
    result = es.search(index=ES_INDEX, body=query)
    return str(result)
