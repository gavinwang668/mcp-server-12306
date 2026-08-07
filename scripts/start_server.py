"""12306 MCP 服务器启动脚本 — Streamable HTTP 模式一键启动"""

import asyncio
import sys
import logging
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# 导入配置
try:
    from mcp_12306.utils.config import get_settings
    settings = get_settings()
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
except Exception:
    log_level = logging.INFO

# 配置日志
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def check_environment() -> bool:
    """检查运行环境"""
    logger.info("检查运行环境...")

    # 检查Python版本
    if sys.version_info < (3, 10):
        logger.error(f"Python版本过低: {sys.version_info}，需要Python 3.10+")
        return False

    try:
        # 检查必要的包（starlette/uvicorn 由 mcp 传递提供）
        import mcp
        import starlette
        import uvicorn
        import httpx2
        logger.info("✅ 所有必要包已安装")
        return True
    except ImportError as e:
        logger.error(f"❌ 缺少必要包: {e}")
        logger.error("请运行: uv sync")
        return False


def main() -> None:
    """主函数"""
    try:
        # 环境检查
        if not check_environment():
            sys.exit(1)

        # 导入并运行服务器
        from mcp_12306.http_server import main_server

        logger.info("🚀 启动12306 MCP服务器...")

        asyncio.run(main_server())

    except ImportError as e:
        logger.error(f"❌ 导入错误: {e}")
        logger.error("请确保已正确安装依赖: uv sync")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 启动失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()