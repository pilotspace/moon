# AI Agent Tools with Moon

Use Moon as a tool provider for AI agents with OpenAI-compatible function calling schemas.

## What this demonstrates

AI agents (like those built with OpenAI's Assistants API, LangChain, or custom loops) need **tools** to interact with data. Moon provides four powerful tools:

| Tool | Moon Command | What it does |
|------|-------------|-------------|
| `search` | `FT.SEARCH` | Find documents by semantic similarity |
| `navigate` | `FT.NAVIGATE` | Multi-hop graph+vector retrieval |
| `recommend` | `FT.RECOMMEND` | Find similar items from examples |
| `cache_lookup` | `FT.CACHESEARCH` | Check semantic cache before expensive operations |

## Prerequisites

- Moon server running on `localhost:6379`
- Python 3.9+

## Setup

```bash
cd examples/ai-agent-tools
pip install -r requirements.txt
```

## Run

```bash
python agent_tools.py
```

## How it works

1. **Define tool schemas** in OpenAI function-calling format (JSON Schema)
2. **Simulate an agent loop**: user query -> LLM decides which tool to call -> execute tool against Moon -> return result to LLM -> generate answer
3. **Show the full interaction**: tool selection, parameters, Moon response, and final answer

The example uses a simulated LLM that pattern-matches queries to tools, so no API key is needed. The tool schemas and Moon interaction are fully real.

## Tool schemas

Each tool is defined as a JSON function schema compatible with OpenAI's function calling:

```json
{
  "type": "function",
  "function": {
    "name": "search_documents",
    "description": "Search for documents by semantic similarity",
    "parameters": {
      "type": "object",
      "properties": {
        "query": { "type": "string", "description": "Natural language query" },
        "k": { "type": "integer", "description": "Number of results" }
      },
      "required": ["query"]
    }
  }
}
```

## Integration patterns

- **OpenAI Assistants**: Pass the tool schemas to `tools` parameter
- **LangChain**: Wrap each tool as a `BaseTool` subclass
- **Custom agent loops**: Parse function call responses and execute against Moon

## Next steps

- Try the [RAG Quickstart](../rag-quickstart/) for the full retrieval pipeline
- Try the [GraphRAG](../graphrag/) for knowledge graph integration
- See the [Python SDK docs](../../sdk/python/) for the complete API
