"""MCP Server 12306 — Streamable HTTP 传输层

基于 FastAPI + uvicorn 的 HTTP 传输层，提供 MCP Streamable HTTP (2025-03-26) 端点。
所有业务逻辑委托给 services.ticket_service 共享模块。
"""

import asyncio
import json
import uuid
from datetime import datetime
from typing import Dict

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from .services.ticket_service import (
    MCP_PROTOCOL_VERSION,
    SERVER_NAME,
    SERVER_VERSION,
    MCP_TOOLS,
    station_service,
    settings,
    logger,
    query_tickets_validated,
    query_ticket_price_validated,
    search_stations_validated,
    query_transfer_validated,
    get_train_route_stations_validated,
    get_train_no_by_train_code_validated,
    get_current_time_validated,
)

# Connected clients for HTTP session management
connected_clients: Dict[str, Dict] = {}

app = FastAPI(
    title=SERVER_NAME,
    version=SERVER_VERSION,
    description="基于MCP协议(2025-03-26 Streamable HTTP)的12306火车票查询服务，支持直达、过站和换乘查询",
    debug=settings.debug
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.get("/")
async def root():
    return {
        "name": SERVER_NAME,
        "version": SERVER_VERSION,
        "status": "running",
        "mcp_endpoint": "/mcp",
        "protocol_version": MCP_PROTOCOL_VERSION,
        "transport": "Streamable HTTP (2025-03-26)",
        "stations_loaded": len(station_service.stations),
        "tools": [tool["name"] for tool in MCP_TOOLS],
        "active_sessions": len(connected_clients)
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "stations": len(station_service.stations),
        "active_sessions": len(connected_clients)
    }


@app.get("/schema/tools")
async def get_tools_schema():
    return {
        "tools": MCP_TOOLS,
        "schema_version": "http://json-schema.org/draft-07/schema#"
    }


# ── MCP Streamable HTTP Transport Endpoints (2025-03-26 spec) ──

@app.options("/mcp")
async def mcp_options():
    return JSONResponse(
        {},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, Mcp-Session-Id",
        }
    )


@app.get("/mcp")
async def mcp_endpoint_get(request: Request):
    session_id = str(uuid.uuid4())
    logger.info(f"New MCP GET connection established - Session ID: {session_id}")

    connected_clients[session_id] = {
        "connected_at": datetime.now().isoformat(),
        "user_agent": request.headers.get("user-agent", ""),
        "client_ip": request.client.host if request.client else "unknown",
        "initialized": False,
        "protocol_version": MCP_PROTOCOL_VERSION
    }

    async def generate_events():
        try:
            while True:
                await asyncio.sleep(30)
                yield f"event: ping\ndata: {{\"timestamp\": \"{datetime.now().isoformat()}\"}}\n\n"
        except asyncio.CancelledError:
            logger.info(f"MCP GET connection closed - Session ID: {session_id}")
        except Exception as e:
            logger.error(f"MCP GET error for session {session_id}: {e}")
        finally:
            if session_id in connected_clients:
                del connected_clients[session_id]

    return StreamingResponse(
        generate_events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "X-Accel-Buffering": "no",
            "Mcp-Session-Id": session_id
        }
    )


@app.post("/mcp")
async def mcp_endpoint_post(request: Request):
    request_id = None
    try:
        data = await request.json()

        if not isinstance(data, dict) or data.get("jsonrpc") != "2.0":
            raise HTTPException(status_code=400, detail="Invalid JSON-RPC 2.0 message")

        method = data.get("method")
        params = data.get("params", {})
        request_id = data.get("id")

        if not method:
            raise HTTPException(status_code=400, detail="Method is required")

        logger.info(f"Received MCP request: {method} (ID: {request_id})")

        # ── initialize ──
        if method == "initialize":
            client_protocol_version = params.get("protocolVersion", MCP_PROTOCOL_VERSION)
            client_info = params.get("clientInfo", {})

            logger.info(f"Initialize request - Client Protocol: {client_protocol_version}")
            logger.info(f"Client Info: {client_info}")

            session_id = str(uuid.uuid4())
            connected_clients[session_id] = {
                "connected_at": datetime.now().isoformat(),
                "user_agent": request.headers.get("user-agent", ""),
                "client_ip": request.client.host if request.client else "unknown",
                "initialized": False,
                "protocol_version": client_protocol_version
            }

            accepted_version = client_protocol_version if client_protocol_version else MCP_PROTOCOL_VERSION

            response = {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": accepted_version,
                    "serverInfo": {
                        "name": SERVER_NAME,
                        "version": SERVER_VERSION,
                        "description": "12306火车票查询服务，提供车票查询、车站搜索、中转查询等功能"
                    },
                    "capabilities": {
                        "tools": {}
                    }
                }
            }

            logger.info(f"Initialize response sent - Protocol: {accepted_version}, Session: {session_id}")
            return JSONResponse(
                response,
                headers={
                    "Mcp-Session-Id": session_id,
                    "Access-Control-Allow-Origin": "*"
                }
            )

        # ── session validation ──
        session_id = request.headers.get("mcp-session-id")
        if not session_id:
            logger.error("Missing Mcp-Session-Id header for non-initialize request")
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32000,
                        "message": "Bad Request: No valid session ID provided"
                    }
                },
                status_code=400
            )

        if session_id not in connected_clients:
            logger.error(f"Invalid session ID: {session_id}")
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32000,
                        "message": "Invalid session ID"
                    }
                },
                status_code=404
            )

        logger.info(f"Processing message for session: {session_id}")

        # ── tools/list ──
        if method == "tools/list":
            logger.info("Tools list requested")
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {"tools": MCP_TOOLS}
            })

        # ── tools/call ──
        if method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})

            if not tool_name:
                raise HTTPException(status_code=400, detail="Tool name is required")

            logger.info(f"Executing tool: {tool_name}")
            logger.info(f"Arguments: {arguments}")

            try:
                tool_map = {
                    "query-tickets": query_tickets_validated,
                    "query-ticket-price": query_ticket_price_validated,
                    "search-stations": search_stations_validated,
                    "query-transfer": query_transfer_validated,
                    "get-train-route-stations": get_train_route_stations_validated,
                    "get-train-no-by-train-code": get_train_no_by_train_code_validated,
                    "get-current-time": get_current_time_validated,
                }
                handler = tool_map.get(tool_name)
                if handler:
                    content = await handler(arguments)
                else:
                    content = [{"type": "text", "text": f"未知工具: {tool_name}"}]

                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": content,
                        "isError": False
                    }
                }
                logger.info(f"Tool {tool_name} executed successfully")

            except Exception as tool_error:
                logger.error(f"Tool execution error: {tool_error}")
                response = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{
                            "type": "text",
                            "text": f"工具执行失败: {str(tool_error)}"
                        }],
                        "isError": True
                    }
                }

            return JSONResponse(response)

        # ── notifications/* ──
        if method and method.startswith("notifications/"):
            notification_type = method.replace("notifications/", "")
            logger.info(f"Received notification: {notification_type}")
            if notification_type == "initialized":
                logger.info("Client initialized successfully - MCP handshake complete!")
                if session_id in connected_clients:
                    connected_clients[session_id]["initialized"] = True
            return Response(status_code=202)

        # ── ping ──
        if method == "ping":
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "timestamp": datetime.now().isoformat(),
                    "status": "alive"
                }
            })

        # ── unknown method ──
        logger.warning(f"Unknown method: {method}")
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32601,
                "message": "Method not found",
                "data": {"method": method}
            }
        }, status_code=404)

    except json.JSONDecodeError:
        logger.error("Invalid JSON in request")
        return JSONResponse(
            {
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32700,
                    "message": "Parse error"
                }
            },
            status_code=400
        )
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return JSONResponse(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": str(e)}
                }
            },
            status_code=500
        )


@app.delete("/mcp")
async def mcp_endpoint_delete(request: Request):
    session_id = request.headers.get("mcp-session-id")

    if not session_id:
        return JSONResponse(
            {"error": "Missing Mcp-Session-Id header"},
            status_code=400
        )

    if session_id in connected_clients:
        del connected_clients[session_id]
        logger.info(f"Session terminated: {session_id}")
        return Response(status_code=200)
    else:
        return JSONResponse(
            {"error": "Invalid session ID"},
            status_code=404
        )


@app.on_event("startup")
async def startup_event():
    logger.info("启动12306 MCP服务器...")
    logger.info(f"协议版本: {MCP_PROTOCOL_VERSION}")
    logger.info(f"传输类型: Streamable HTTP")
    logger.info("正在加载车站数据...")
    await station_service.load_stations()
    logger.info(f"已加载 {len(station_service.stations)} 个车站")


async def main_server():
    logger.info("启动12306 MCP服务器...")
    logger.info(f"协议版本: {MCP_PROTOCOL_VERSION}")
    logger.info(f"传输类型: Streamable HTTP")
    logger.info(f"MCP端点: http://{settings.server_host}:{settings.server_port}/mcp")
    logger.info(f"健康检查: http://{settings.server_host}:{settings.server_port}/health")

    config = uvicorn.Config(
        app,
        host=settings.server_host,
        port=settings.server_port,
        log_level=settings.log_level.lower()
    )
    uvicorn_server = uvicorn.Server(config)
    await uvicorn_server.serve()


def main():
    asyncio.run(main_server())


if __name__ == "__main__":
    main()
