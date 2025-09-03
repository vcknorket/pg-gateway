# Example Python WebSocket Worker

Here is the complete, final content of the `updated_query_service.py` file. You can copy and paste this code to update your existing Python service.

This version includes:
1.  A new WebSocket endpoint at `/ws/pgwire`.
2.  An updated `execute_and_stream_query` function that is compatible with the gateway's streaming protocol (`schema`, `data`, `complete`).
3.  A robust mapping from Trino data types to the required PostgreSQL OIDs.

```python
import asyncio
import hashlib
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Tuple

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# Assuming these models are defined in a separate file as per the original structure
# In a real scenario, these would be imported from ..core.models
class SaasLakeRequest:
    def __init__(self, api_key: str, access_token: str, query: str, **kwargs):
        self.api_key = api_key
        self.access_token = access_token
        self.query = query

class SaasLakeResponse:
    def __init__(self, data: Any, **kwargs):
        self.data = data

class QueryRequest:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

# Assuming this function is defined in a separate file as per the original structure
async def get_or_create_session(queryRequest: QueryRequest) -> Any:
    # Placeholder for the real session creation logic
    # This should be replaced with the actual implementation from the user's codebase.
    # For example: from ..core.pool_manager import get_or_create_session
    pass

# --- Type Mapping ---
# This dictionary maps Trino data type names (as strings) to their
# corresponding PostgreSQL Object IDs (OIDs). This is crucial for the gateway
# to correctly form the RowDescription message for the PG client.
TRINO_TYPE_TO_PG_OID = {
    # Text Types
    "varchar": 25, "char": 25, "json": 114,
    # Numeric Types
    "bigint": 20, "integer": 23, "smallint": 21, "tinyint": 21,
    "double": 701, "real": 700, "decimal": 1700,
    # Boolean Type
    "boolean": 16,
    # Date/Time Types
    "date": 1082, "timestamp": 1114, "timestamp with time zone": 1184,
    "time": 1083, "time with time zone": 1266,
    # Default OID for any unmapped types
    "DEFAULT": 25, # TEXT
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(root_path="/query-service")


@app.websocket("/ws/pgwire")
async def pgwire(websocket: WebSocket):
    """
    This is the new WebSocket endpoint specifically for the pg-gateway.
    It uses the same authentication and query execution logic as the saaslake
    endpoint but ensures the response conforms to the streaming protocol
    required by the gateway (schema, data, complete).
    """
    await websocket.accept()

    try:
        while True:
            msgstr = await websocket.receive_text()
            valid = True
            queryRequest = None
            try:
                msmgjson = json.loads(msgstr)
                pgwire_request = SaasLakeRequest(**msmgjson)

                # This block for authenticating and getting Trino credentials
                # is copied from the original saaslake endpoint.
                async with httpx.AsyncClient() as client:
                    url = "https://testapp-app.stratscient.com/api/v1/saas_lake"
                    headers = {
                        "Authorization": f"bearer {pgwire_request.api_key}",
                        "Content-Type": "application/json",
                    }
                    body = {"access_token": pgwire_request.access_token}
                    response = await client.request("GET", url, headers=headers, content=json.dumps(body))
                    response.raise_for_status()
                    response_data = SaasLakeResponse(**response.json())
                    if not response_data.data.args:
                        raise ValueError("Data asset response has empty args")
                    args = response_data.data.args[0]

                    queryRequest = QueryRequest(
                        cluster_url="knorket-test-gcp-trino.kloudsoft.co",
                        port=443,
                        username=args.username,
                        password=args.password,
                        user_id=str(args.user_id),
                        query=pgwire_request.query,
                        http_scheme="https",
                        catalog=args.catalog,
                        schema="",
                        query_id=str(response_data.data.task_id), # Placeholder, real ID generated in execute
                    )

            except Exception as e:
                logger.error(f"{pgwire.__name__}: {e}")
                valid = False

            if valid and queryRequest:
                asyncio.create_task(execute_and_stream_query(websocket, queryRequest))
            else:
                await websocket.send_json({"error": "invalid msg", "msg": msgstr})

    except WebSocketDisconnect:
        logger.info(f"{websocket.client}: Disconnected")


async def execute_and_stream_query(
    websocket: WebSocket, queryRequest: QueryRequest, data_asset: str | None = None
):
    """
    MODIFIED: This function now executes a query and streams the results
    back to the gateway using the required `schema`, `data`, `complete` protocol.
    """
    query_id = str(uuid.uuid4())
    logger.info(f"[{query_id}] EXECUTING QUERY: {queryRequest.query}")

    try:
        conn = await get_or_create_session(queryRequest)
        cur = await conn.cursor()
        await cur.execute(queryRequest.query)

        # --- Send Schema Message with Correct Type Mapping ---
        if cur.description:
            columns = [
                {
                    "name": col[0],
                    "typeOID": TRINO_TYPE_TO_PG_OID.get(col[1], TRINO_TYPE_TO_PG_OID["DEFAULT"]),
                }
                for col in cur.description
            ]
            schema_message = {
                "query_id": query_id,
                "type": "schema",
                "payload": {"columns": columns},
            }
            await websocket.send_json(schema_message)

        total_rows = 0
        chunk_size = 100000
        while True:
            rows = await cur.fetchmany(chunk_size)
            if not rows:
                break

            # Convert all row values to strings for transport
            string_rows = [[str(item) if item is not None else None for item in row] for row in rows]
            total_rows += len(string_rows)

            data_message = {
                "query_id": query_id,
                "type": "data",
                "payload": string_rows,
            }
            if data_asset is not None:
                data_message["data_asset"] = data_asset
            await websocket.send_json(data_message)

        # Determine the command tag
        command_tag = queryRequest.query.lstrip().split(" ")[0].upper()
        if command_tag not in ["SELECT", "WITH"]:
            command_tag = f"{command_tag} {total_rows}"
        else:
            command_tag = f"SELECT {total_rows}"

        complete_message = {
            "query_id": query_id,
            "type": "complete",
            "payload": { "commandTag": command_tag, "total_rows": total_rows }
        }
        if data_asset is not None:
            complete_message["data_asset"] = data_asset
        await websocket.send_json(complete_message)

    except Exception as e:
        logger.error(f"{execute_and_stream_query.__name__}: {e}")
        try:
            error_message = {
                "query_id": query_id,
                "type": "error",
                "payload": {"message": str(e), "code": "XX000"},
            }
            if data_asset is not None:
                error_message["data_asset"] = data_asset
            await websocket.send_json(error_message)
        except Exception as e_ws:
            logger.error(f"Failed to send error over WebSocket: {e_ws}")
```
