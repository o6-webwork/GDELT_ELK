from mcp.server.fastmcp import FastMCP
from context import app_lifespan
# from tools import query_es  # ensures tool is registered

mcp = FastMCP("My App", lifespan=app_lifespan)

if __name__ == "__main__":
    mcp.run()