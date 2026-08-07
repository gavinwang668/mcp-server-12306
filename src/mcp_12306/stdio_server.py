"""MCP Server 12306 — Stdio 传输实现

通过标准输入/输出（stdio）与 MCP 客户端通信，不使用任何 HTTP 框架。
适用于 Claude Desktop 等本地 MCP 客户端。

复用 ``server.py`` 中的核心 Server 实例（工具注册与业务逻辑与 HTTP 模式一致）。
"""

import asyncio
import logging

from mcp.server.stdio import stdio_server

from .server import server
from .services.ticket_service import station_service

logger = logging.getLogger(__name__)


async def run_stdio_server() -> None:
    """运行 stdio 服务器"""
    import mcp_12306

    logger.info("启动 mcp-server-12306 (stdio 模式)...")
    logger.info(f"版本: {mcp_12306.__version__}")

    logger.info("正在加载车站数据...")

    # 加载车站数据
    await station_service.load_stations()
    logger.info(f"已加载 {len(station_service.stations)} 个车站")

    # 运行服务器
    async with stdio_server() as (read_stream, write_stream):
        logger.info("MCP Server 已启动，等待客户端连接...")
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


def main() -> None:
    """Stdio 模式 CLI 入口 — 参数解析 + 启动服务器"""
    import sys
    import argparse
    from mcp_12306 import __version__

    parser = argparse.ArgumentParser(
        description="MCP Server for 12306 Ticket Query (Stdio Mode)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 启动 MCP 服务器（通过 stdin 等待 JSON-RPC 消息）
  mcp-server-12306

  # 查看版本
  mcp-server-12306 --version
"""
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}"
    )
    parser.parse_args()

    try:
        asyncio.run(run_stdio_server())
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在关闭服务器...")
    except Exception as e:
        logger.error(f"服务器运行失败: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
