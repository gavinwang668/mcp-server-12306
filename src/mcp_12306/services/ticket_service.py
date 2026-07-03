"""共享业务逻辑层 — stdio 与 Streamable HTTP 两种传输模式共用

不依赖任何 HTTP 框架，仅使用 httpx + pydantic-settings + pytz + aiofiles。
"""

import asyncio
import json
import logging
import re
import httpx
from datetime import datetime, date
from typing import Dict, List, Any
import pytz

from .station_service import StationService
from ..utils.config import get_settings
from ..utils.date_utils import validate_date, validate_date_not_past
from .. import __version__

settings = get_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
station_service = StationService()

# MCP Protocol Version
MCP_PROTOCOL_VERSION = "2025-03-26"
SERVER_NAME = "mcp-server-12306"
SERVER_VERSION = __version__

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

# 中国铁路 12306 API 常量 - URL
HTTP_URLS = {
    "init": "https://kyfw.12306.cn/otn/leftTicket/init",
    "query_left_ticket": "https://kyfw.12306.cn/otn/leftTicket/queryI",
    "query_transfer": "https://kyfw.12306.cn/lcquery/queryI",
    "query_price": "https://kyfw.12306.cn/otn/leftTicketPrice/queryAllPublicPrice",
    "query_route_stations": "https://kyfw.12306.cn/otn/czxx/queryByTrainNo",
}

# 中国铁路 12306 API 通用请求头
HTTP_HEADERS = {
    "User-Agent": USER_AGENT,
    "Referer": "https://kyfw.12306.cn/otn/leftTicket/init",
    "Host": "kyfw.12306.cn",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://kyfw.12306.cn"
}

HTTP_TIMEOUT = 8
HTTP_FOLLOW_REDIRECTS = True


def create_12306_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        follow_redirects=HTTP_FOLLOW_REDIRECTS,
        timeout=HTTP_TIMEOUT,
        verify=False
    )


def get_redirect_info(resp: httpx.Response) -> Dict[str, Any]:
    return {
        "final_url": str(resp.url),
        "redirect_chain": [
            {
                "status_code": r.status_code,
                "location": r.headers.get("location", ""),
                "url": str(r.url)
            }
            for r in resp.history
        ],
    }


def is_12306_error_response(resp: httpx.Response) -> bool:
    final_url = str(resp.url)
    return (
        resp.status_code != 200
        or "error.html" in final_url
        or "/ntce/" in final_url
        or "resources/error" in final_url
    )


async def ensure_telecode(val: str) -> str | None:
    """车站名/三字码自动转换，无法识别时返回 None"""
    if val.isalpha() and val.isupper() and len(val) == 3:
        return val
    code = await station_service.get_station_code(val)
    return code


def parse_ticket_string(ticket_str: str, query: dict) -> dict | None:
    parts = ticket_str.split('|')
    if len(parts) < 35:
        return None
    return {
        "train_no": parts[3],
        "start_time": parts[8],
        "arrive_time": parts[9],
        "duration": parts[10],
        "business_seat_num": parts[32] or "",
        "first_class_num": parts[31] or "",
        "second_class_num": parts[30] or "",
        "advanced_soft_sleeper_num": parts[21] or "",
        "soft_sleeper_num": parts[23] or "",
        "dongwo_num": parts[33] or "",
        "hard_sleeper_num": parts[28] or "",
        "soft_seat_num": parts[24] or "",
        "hard_seat_num": parts[29] or "",
        "no_seat_num": parts[26] or "",
        "from_station": query["from_station"],
        "to_station": query["to_station"],
        "train_date": query["train_date"]
    }


# MCP Tools Definition
MCP_TOOLS = [
    {
        "name": "query-tickets",
        "description": "官方12306余票/车次/座席/时刻一站式查询。输入出发站、到达站、日期，返回所有可购车次、时刻、历时、各席别余票等详细信息。支持中文名、三字码。\n\n【智能筛选指南】返回结果通常包含出发/到达城市的所有相关车站（如北京/北京西/北京南）。请根据用户输入语境灵活处理：\n1. 用户仅输入城市名（如'九江'）：请展示所有相关站点的车次，不要过滤。\n2. 用户指定具体车站（如'九江站'）：优先展示匹配车站的车次，但若其他同城车站有更优方案（如时间更短、有票），也应作为补充选项提供。\n请避免机械地仅通过字符串匹配过滤车次，以免遗漏用户可能感兴趣的出行方案。",
        "inputSchema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "车票查询参数",
            "description": "查询火车票所需的参数",
            "properties": {
                "from_station": {"type": "string", "title": "出发站", "description": "出发车站名称，例如：北京、上海、广州", "minLength": 1},
                "to_station": {"type": "string", "title": "到达站", "description": "到达车站名称，例如：北京、上海、广州", "minLength": 1},
                "train_date": {"type": "string", "title": "出发日期", "description": "出发日期，格式：YYYY-MM-DD", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}
            },
            "required": ["from_station", "to_station", "train_date"],
            "additionalProperties": False
        }
    },
    {
        "name": "query-ticket-price",
        "description": "查询火车票价信息。输入出发站、到达站、日期，返回各车次的票价详情。支持指定车次号过滤。\n\n【智能筛选指南】返回结果通常包含出发/到达城市的所有相关车站（如北京/北京西/北京南）。请根据用户输入语境灵活处理：\n1. 用户仅输入城市名（如'九江'）：请展示所有相关站点的车次，不要过滤。\n2. 用户指定具体车站（如'九江站'）：优先展示匹配车站的车次，但若其他同城车站有更优方案（如时间更短、有票），也应作为补充选项提供。\n请避免机械地仅通过字符串匹配过滤车次，以免遗漏用户可能感兴趣的出行方案。",
        "inputSchema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "票价查询参数",
            "properties": {
                "from_station": {"type": "string", "title": "出发站", "minLength": 1},
                "to_station": {"type": "string", "title": "到达站", "minLength": 1},
                "train_date": {"type": "string", "title": "出发日期", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                "train_code": {"type": "string", "title": "车次号（可选）", "description": "指定车次号（如G123），若提供则只返回该车次信息"},
                "purpose_codes": {"type": "string", "title": "乘客类型", "description": "ADULT=成人, 0X=学生", "default": "ADULT"}
            },
            "required": ["from_station", "to_station", "train_date"],
            "additionalProperties": False
        }
    },
    {
        "name": "search-stations",
        "description": "智能车站搜索。支持中文名、拼音、简拼、三字码（Code）。可用于模糊搜索（如'北京'），也可用于精确获取车站代码（如输入'BJP'返回北京站信息）。",
        "inputSchema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "车站搜索参数",
            "description": "搜索火车站所需的参数",
            "properties": {
                "query": {"type": "string", "title": "搜索关键词", "description": "车站搜索关键词，支持：车站名称、拼音、简拼等", "minLength": 1, "maxLength": 20},
                "limit": {"type": "integer", "title": "结果数量限制", "description": "返回结果的最大数量", "minimum": 1, "maximum": 50, "default": 10}
            },
            "required": ["query"],
            "additionalProperties": False
        }
    },
    {
        "name": "query-transfer",
        "description": "官方中转换乘方案查询。输入出发站、到达站、日期，可选中转站/无座/学生票，自动分页抓取全部中转方案，输出每段车次、时刻、余票、等候时间、总历时等详细信息。",
        "inputSchema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "中转查询参数",
            "description": "查询A到B的中转换乘（含一次换乘）",
            "properties": {
                "from_station": {"type": "string", "title": "出发站"},
                "to_station": {"type": "string", "title": "到达站"},
                "train_date": {"type": "string", "title": "出发日期", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                "middle_station": {"type": "string", "title": "中转站（可选）", "description": "指定中转站名称或三字码，可选"},
                "isShowWZ": {"type": "string", "title": "是否显示无座车次（Y/N）", "description": "Y=显示无座车次，N=不显示，默认N", "default": "N"},
                "purpose_codes": {"type": "string", "title": "乘客类型（00=普通，0X=学生）", "description": "00为普通，0X为学生，默认00"}
            },
            "required": ["from_station", "to_station", "train_date"],
            "additionalProperties": False
        }
    },
    {
        "name": "get-train-route-stations",
        "description": "列车经停站全表查询。支持输入车次号或官方编号，自动转换，返回所有经停站、到发时刻、停留时间。支持三字码/全名。",
        "inputSchema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "列车经停站查询参数",
            "properties": {
                "train_no": {"type": "string", "title": "车次编码", "minLength": 1},
                "from_station": {"type": "string", "title": "出发站id", "minLength": 1},
                "to_station": {"type": "string", "title": "到达站id", "minLength": 1},
                "train_date": {"type": "string", "title": "出发日期", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}
            },
            "required": ["train_no", "from_station", "to_station", "train_date"],
            "additionalProperties": False
        }
    },
    {
        "name": "get-train-no-by-train-code",
        "description": "车次号转官方唯一编号（train_no），支持三字码/全名。常用于经停站查询前置转换。",
        "inputSchema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "车次号转编号参数",
            "properties": {
                "train_code": {"type": "string", "title": "车次号", "minLength": 1},
                "from_station": {"type": "string", "title": "出发站id或全名", "minLength": 1},
                "to_station": {"type": "string", "title": "到达站id或全名", "minLength": 1},
                "train_date": {"type": "string", "title": "出发日期", "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}
            },
            "required": ["train_code", "from_station", "to_station", "train_date"],
            "additionalProperties": False
        }
    },
    {
        "name": "get-current-time",
        "description": "获取当前日期和时间信息，支持相对日期计算。返回当前日期、时间，以及常用的相对日期（明天、后天等），方便用户在查询火车票时选择正确的日期。",
        "inputSchema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "title": "获取当前时间参数",
            "description": "获取当前时间和日期信息",
            "properties": {
                "timezone": {"type": "string", "title": "时区", "description": "时区设置，默认为中国时区", "default": "Asia/Shanghai"},
                "format": {"type": "string", "title": "日期格式", "description": "返回的日期格式，默认为YYYY-MM-DD", "default": "YYYY-MM-DD"}
            },
            "additionalProperties": False
        }
    }
]


# ========== 车站模糊搜索工具 ==========

async def search_stations_validated(args: dict) -> list:
    query = args.get("query", "").strip()
    limit = args.get("limit", 10)
    if not query:
        return [{"type": "text", "text": json.dumps({"success": False, "error": "请输入搜索关键词"}, ensure_ascii=False)}]
    if not isinstance(limit, int) or limit < 1 or limit > 50:
        limit = 10
    result = await station_service.search_stations(query, limit)
    if result.stations:
        stations_data = []
        for station in result.stations:
            station_dict = {
                "name": station.name,
                "code": station.code,
                "pinyin": station.pinyin,
                "py_short": station.py_short if station.py_short else "",
            }
            if hasattr(station, 'num') and station.num:
                station_dict["num"] = station.num
            stations_data.append(station_dict)

        response_data = {
            "success": True,
            "query": query,
            "count": len(stations_data),
            "stations": stations_data
        }
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
    else:
        response_data = {
            "success": False,
            "query": query,
            "count": 0,
            "stations": [],
            "message": "未找到匹配的车站",
            "suggestions": [
                "尝试完整城市名称 (如: 北京)",
                "尝试拼音 (如: beijing)",
                "尝试简拼 (如: bj)",
                "检查拼写是否正确"
            ]
        }
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]


# ========== 车票查询工具 ==========

async def query_tickets_validated(args: dict) -> list:
    try:
        from_station = args.get("from_station", "").strip()
        to_station = args.get("to_station", "").strip()
        train_date = args.get("train_date", "").strip()
        logger.info(f"查询参数: {from_station} -> {to_station} ({train_date})")
        errors = []
        if not from_station:
            errors.append("出发站不能为空")
        if not to_station:
            errors.append("到达站不能为空")
        if not train_date:
            errors.append("出发日期不能为空")
        elif not validate_date(train_date):
            errors.append("日期格式错误，请使用 YYYY-MM-DD 格式")
        else:
            is_valid, error_msg = validate_date_not_past(train_date)
            if not is_valid:
                errors.append(error_msg)

        if errors:
            response_data = {"success": False, "errors": errors}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
        from_code = await ensure_telecode(from_station)
        to_code = await ensure_telecode(to_station)
        if not from_code or not to_code:
            suggestions = []
            if not from_code:
                result = await station_service.search_stations(from_station, 3)
                if result.stations:
                    suggestions.append({"station_type": "from", "input": from_station, "matches": [{"name": s.name, "code": s.code, "pinyin": s.pinyin, "py_short": s.py_short} for s in result.stations]})
            if not to_code:
                result = await station_service.search_stations(to_station, 3)
                if result.stations:
                    suggestions.append({"station_type": "to", "input": to_station, "matches": [{"name": s.name, "code": s.code, "pinyin": s.pinyin, "py_short": s.py_short} for s in result.stations]})
            response_data = {"success": False, "error": "车站名称无效", "suggestions": suggestions, "hint": "可尝试拼音、简拼、三字码或用 search_stations 工具辅助查询"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        url_init = HTTP_URLS["init"]
        url_u = HTTP_URLS["query_left_ticket"]
        headers = HTTP_HEADERS.copy()
        max_retries = 3
        last_exception = None
        tickets_data = []

        for attempt in range(max_retries):
            try:
                async with create_12306_client() as client:
                    await client.get(url_init, headers=headers)
                    params = {
                        "leftTicketDTO.train_date": train_date,
                        "leftTicketDTO.from_station": from_code,
                        "leftTicketDTO.to_station": to_code,
                        "purpose_codes": "ADULT"
                    }
                    resp = await client.get(url_u, headers=headers, params=params)
                    logger.info(f"12306 leftTicket status: {resp.status_code}, redirect: {get_redirect_info(resp)}")
                    if is_12306_error_response(resp):
                        logger.error(f"12306接口返回异常: {resp.status_code}, final_url: {resp.url}, body: {resp.text}")
                        response_data = {
                            "success": False,
                            "error": "12306接口返回异常或反爬虫拦截",
                            "status_code": resp.status_code,
                            "final_url": str(resp.url),
                            "detail": resp.text[:200]
                        }
                        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
                    try:
                        data = resp.json().get("data", {})
                        tickets_data = data.get("result", [])
                        break
                    except Exception as e:
                        logger.error(f"12306响应解析失败: {repr(e)}，原始内容: {resp.text}")
                        response_data = {"success": False, "error": "12306响应解析失败", "detail": f"{type(e).__name__}: {str(e)}"}
                        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
            except (httpx.TimeoutException, httpx.NetworkError, httpx.ConnectError) as e:
                last_exception = e
                if attempt < max_retries - 1:
                    logger.warning(f"查询车票网络请求失败，正在重试 ({attempt + 1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"查询车票网络请求重试次数已耗尽: {str(e)}")
        else:
            response_data = {"success": False, "error": f"网络请求失败 (已重试{max_retries}次): {str(last_exception)}"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
        tickets = []
        for ticket_str in tickets_data:
            ticket = parse_ticket_string(ticket_str, {
                "from_station": from_station,
                "to_station": to_station,
                "train_date": train_date
            })
            if ticket:
                tickets.append(ticket)
        if tickets:
            trains_list = []
            for i, ticket in enumerate(tickets, 1):
                ticket_str = tickets_data[i-1] if i-1 < len(tickets_data) else None
                from_station_name = to_station_name = from_code_actual = to_code_actual = None
                if ticket_str:
                    parts = ticket_str.split('|')
                    from_code_actual = parts[6] if len(parts) > 6 else None
                    to_code_actual = parts[7] if len(parts) > 7 else None
                    from_station_obj = await station_service.get_station_by_code(from_code_actual) if from_code_actual else None
                    to_station_obj = await station_service.get_station_by_code(to_code_actual) if to_code_actual else None
                    from_station_name = from_station_obj.name if from_station_obj else (from_code_actual or "未知")
                    to_station_name = to_station_obj.name if to_station_obj else (to_code_actual or "未知")

                seats = {}
                if ticket['business_seat_num']: seats["business"] = ticket['business_seat_num']
                if ticket['first_class_num']: seats["first_class"] = ticket['first_class_num']
                if ticket['second_class_num']: seats["second_class"] = ticket['second_class_num']
                if ticket['advanced_soft_sleeper_num']: seats["advanced_soft_sleeper"] = ticket['advanced_soft_sleeper_num']
                if ticket['soft_sleeper_num']: seats["soft_sleeper"] = ticket['soft_sleeper_num']
                if ticket['hard_sleeper_num']: seats["hard_sleeper"] = ticket['hard_sleeper_num']
                if ticket['soft_seat_num']: seats["soft_seat"] = ticket['soft_seat_num']
                if ticket['hard_seat_num']: seats["hard_seat"] = ticket['hard_seat_num']
                if ticket['no_seat_num']: seats["no_seat"] = ticket['no_seat_num']
                if ticket['dongwo_num']: seats["dongwo"] = ticket['dongwo_num']

                train_data = {
                    "train_no": ticket['train_no'],
                    "from_station": from_station_name,
                    "from_station_code": from_code_actual,
                    "to_station": to_station_name,
                    "to_station_code": to_code_actual,
                    "start_time": ticket['start_time'],
                    "arrive_time": ticket['arrive_time'],
                    "duration": ticket['duration'],
                    "seats": seats
                }
                trains_list.append(train_data)

            response_data = {
                "success": True,
                "from_station": from_station,
                "to_station": to_station,
                "train_date": train_date,
                "count": len(trains_list),
                "trains": trains_list
            }
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
        else:
            response_data = {
                "success": False,
                "from_station": from_station,
                "to_station": to_station,
                "train_date": train_date,
                "count": 0,
                "trains": [],
                "message": "未找到该线路的余票"
            }
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
    except Exception as e:
        import traceback
        error_detail = f"{type(e).__name__}: {str(e)}"
        logger.error(f"查询车票失败: {error_detail}\n{traceback.format_exc()}")
        response_data = {"success": False, "error": "查询失败", "detail": error_detail}
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]


# ========== 车次号转编号工具 ==========

async def get_train_no_by_train_code_validated(args: dict) -> list:
    train_code = args.get("train_code", "").strip().upper()
    from_station = args.get("from_station", "").strip().upper()
    to_station = args.get("to_station", "").strip().upper()
    train_date = args.get("train_date", "").strip()

    is_valid, error_msg = validate_date_not_past(train_date)
    if not is_valid:
        response_data = {"success": False, "error": error_msg}
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

    from_code = await ensure_telecode(from_station)
    if not from_code:
        response_data = {"success": False, "error": f"出发站无效或无法识别：{from_station}"}
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
    from_station = from_code

    to_code = await ensure_telecode(to_station)
    if not to_code:
        response_data = {"success": False, "error": f"到达站无效或无法识别：{to_station}"}
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
    to_station = to_code

    url_init = HTTP_URLS["init"]
    url_u = HTTP_URLS["query_left_ticket"]
    headers = HTTP_HEADERS.copy()

    async with create_12306_client() as client:
        await client.get(url_init, headers=headers)
        params = {
            "leftTicketDTO.train_date": train_date,
            "leftTicketDTO.from_station": from_station,
            "leftTicketDTO.to_station": to_station,
            "purpose_codes": "ADULT"
        }
        resp = await client.get(url_u, headers=headers, params=params)
        try:
            data = resp.json().get("data", {})
            tickets_data = data.get("result", [])
        except Exception:
            response_data = {"success": False, "error": "12306反爬拦截或数据异常，请稍后重试"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

    if not tickets_data:
        response_data = {"success": False, "error": f"未找到该线路的余票数据（{from_station}->{to_station} {train_date}）"}
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

    def extract_train_info(ticket_str):
        try:
            parts = ticket_str.split('|')
            idx = parts.index('预订')
            return {
                "train_no": parts[idx+1].strip(),
                "train_code": parts[idx+2].strip().upper()
            }
        except Exception:
            return None

    found = None
    for ticket_str in tickets_data:
        info = extract_train_info(ticket_str)
        if info and info["train_code"] == train_code:
            found = info["train_no"]
            break

    if not found:
        debug_codes = []
        for ticket_str in tickets_data:
            info = extract_train_info(ticket_str)
            if info:
                debug_codes.append(info["train_code"])

        response_data = {
            "success": False,
            "train_code": train_code,
            "from_station": from_station,
            "to_station": to_station,
            "train_date": train_date,
            "error": "未找到该车次号的列车编号",
            "available_trains": debug_codes
        }
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

    response_data = {
        "success": True,
        "train_code": train_code,
        "train_no": found,
        "from_station": from_station,
        "to_station": to_station,
        "train_date": train_date
    }
    return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]


# ========== 经停站查询工具 ==========

async def get_train_route_stations_validated(args: dict) -> list:
    try:
        train_no = args.get("train_no", "").strip()
        from_station = args.get("from_station", "").strip().upper()
        to_station = args.get("to_station", "").strip().upper()
        train_date = args.get("train_date", "").strip()

        if not train_no:
            response_data = {"success": False, "error": "车次编号(train_no)不能为空"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
        if not from_station:
            response_data = {"success": False, "error": "出发站不能为空"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
        if not to_station:
            response_data = {"success": False, "error": "到达站不能为空"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
        if not train_date:
            response_data = {"success": False, "error": "出发日期不能为空"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        is_valid, error_msg = validate_date_not_past(train_date)
        if not is_valid:
            response_data = {"success": False, "error": error_msg}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        def is_telecode(val):
            return val.isalpha() and val.isupper() and len(val) == 3

        if not is_telecode(from_station):
            code = await station_service.get_station_code(from_station)
            if not code:
                response_data = {"success": False, "error": f"出发站无效或无法识别：{from_station}"}
                return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
            from_station = code

        if not is_telecode(to_station):
            code = await station_service.get_station_code(to_station)
            if not code:
                response_data = {"success": False, "error": f"到达站无效或无法识别：{to_station}"}
                return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
            to_station = code

        is_train_code = bool(re.match(r'^[A-Z]+\d+$', train_no))

        if is_train_code:
            logger.info(f"检测到车次号 {train_no}，正在转换为列车编号...")
            convert_args = {
                "train_code": train_no,
                "from_station": from_station,
                "to_station": to_station,
                "train_date": train_date
            }
            convert_result = await get_train_no_by_train_code_validated(convert_args)

            if not convert_result or not convert_result[0].get("text"):
                response_data = {"success": False, "error": f"无法获取车次 {train_no} 的列车编号"}
                return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

            result_json_str = convert_result[0].get("text", "{}")
            result_data = json.loads(result_json_str)
            if not result_data.get("success"):
                return convert_result

            actual_train_no = result_data.get("train_no")
            if not actual_train_no:
                response_data = {"success": False, "error": f"无法解析车次 {train_no} 的列车编号"}
                return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
            logger.info(f"车次 {train_no} 转换为列车编号: {actual_train_no}")
        else:
            actual_train_no = train_no
            logger.info(f"使用列车编号: {actual_train_no}")

        url = HTTP_URLS["query_route_stations"]
        params = {
            "train_no": actual_train_no,
            "from_station_telecode": from_station,
            "to_station_telecode": to_station,
            "depart_date": train_date
        }

        headers = HTTP_HEADERS.copy()

        max_retries = 3
        last_exception = None
        json_data = None

        for attempt in range(max_retries):
            try:
                async with create_12306_client() as client:
                    init_resp = await client.get("https://kyfw.12306.cn/otn/leftTicket/init", headers=headers)
                    logger.info(f"12306 init status: {init_resp.status_code}")

                    resp = await client.get(url, headers=headers, params=params)
                    logger.info(f"12306 route query status: {resp.status_code}, redirect: {get_redirect_info(resp)}")

                    if resp.status_code != 200:
                        logger.error(f"12306接口返回异常状态码: {resp.status_code}, body: {resp.text}")
                        response_data = {"success": False, "error": f"12306接口返回异常: {resp.status_code}"}
                        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

                    if "error.html" in str(resp.url) or "ntce" in str(resp.url):
                        response_data = {"success": False, "error": "12306反爬虫拦截，请稍后重试或更换网络环境"}
                        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

                    try:
                        json_data = resp.json()
                        logger.info(f"12306 response keys: {list(json_data.keys()) if json_data else 'None'}")
                        break
                    except Exception as e:
                        logger.error(f"12306响应解析失败: {str(e)}, body: {resp.text}")
                        response_data = {"success": False, "error": f"12306响应解析失败: {str(e)}"}
                        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
            except (httpx.TimeoutException, httpx.NetworkError, httpx.ConnectError) as e:
                last_exception = e
                if attempt < max_retries - 1:
                    logger.warning(f"查询经停站网络请求失败，正在重试 ({attempt + 1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"查询经停站网络请求重试次数已耗尽: {str(e)}")
        else:
            response_data = {"success": False, "error": f"网络请求失败 (已重试{max_retries}次): {str(last_exception)}"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        if not json_data:
            response_data = {"success": False, "error": "12306接口返回空数据"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        data = json_data.get("data", {})
        stations = data.get("data", [])

        if not stations and "middleList" in data:
            stations = []
            for m in data["middleList"]:
                if "fullList" in m:
                    stations.extend(m["fullList"])
        if not stations and "fullList" in data:
            stations = data["fullList"]
        if not stations and "route" in data:
            stations = data["route"]

        if not stations:
            response_data = {"success": False, "train_no": train_no, "error": "未找到经停站信息"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        stations_list = []
        for station in stations:
            station_data = {
                "station_no": station.get("station_no", station.get("from_station_no", "")),
                "station_name": station.get("station_name", station.get("from_station_name", "")),
                "arrive_time": station.get("arrive_time", "----"),
                "start_time": station.get("start_time", "----"),
                "stopover_time": station.get("stopover_time", "----")
            }
            stations_list.append(station_data)

        response_data = {
            "success": True,
            "train_no": train_no,
            "train_date": train_date,
            "count": len(stations_list),
            "stations": stations_list
        }
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

    except Exception as e:
        logger.error(f"查询经停站失败: {repr(e)}")
        response_data = {"success": False, "error": "查询经停站失败", "detail": str(e)}
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]


# ========== 中转换乘查询工具 ==========

async def query_transfer_validated(args: dict) -> list:
    try:
        from_station = args.get("from_station", "").strip()
        to_station = args.get("to_station", "").strip()
        train_date = args.get("train_date", "").strip()
        middle_station = args.get("middle_station", "").strip() if "middle_station" in args else ""
        isShowWZ = args.get("isShowWZ", "N").strip().upper() or "N"
        purpose_codes = args.get("purpose_codes", "00").strip().upper() or "00"

        if not from_station or not to_station or not train_date:
            response_data = {"success": False, "error": "请输入出发站、到达站和出发日期"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        is_valid, error_msg = validate_date_not_past(train_date)
        if not is_valid:
            response_data = {"success": False, "error": error_msg}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        from_code = await ensure_telecode(from_station)
        to_code = await ensure_telecode(to_station)
        if not from_code:
            response_data = {"success": False, "error": f"出发站无效或无法识别：{from_station}"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
        if not to_code:
            response_data = {"success": False, "error": f"到达站无效或无法识别：{to_station}"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        middle_station_code = ""
        if middle_station:
            middle_station_code = await ensure_telecode(middle_station)
            if not middle_station_code:
                logger.warning(f"无法识别中转站: {middle_station}")
                middle_station_code = middle_station

        url_init = HTTP_URLS["init"]
        url = HTTP_URLS["query_transfer"]
        headers = HTTP_HEADERS.copy()

        all_transfer_list = []
        max_retries = 3
        last_exception = None

        for attempt in range(max_retries):
            try:
                async with create_12306_client() as client:
                    await client.get(url_init, headers=headers)

                    page_size = 10
                    result_index = 0
                    page_num = 1

                    while True:
                        params = {
                            "train_date": train_date,
                            "from_station_telecode": from_code,
                            "to_station_telecode": to_code,
                            "middle_station": middle_station_code,
                            "result_index": str(result_index),
                            "can_query": "Y",
                            "isShowWZ": isShowWZ,
                            "purpose_codes": purpose_codes,
                            "channel": "E"
                        }

                        resp = await client.get(url, headers=headers, params=params)

                        logger.info(f"12306 transfer query status: {resp.status_code}, redirect: {get_redirect_info(resp)}")

                        if is_12306_error_response(resp):
                            if page_num == 1:
                                response_data = {
                                    "success": False,
                                    "error": "12306接口返回异常或反爬虫拦截",
                                    "status_code": resp.status_code,
                                    "final_url": str(resp.url),
                                    "detail": resp.text[:200]
                                }
                                return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
                            else:
                                break

                        try:
                            data = resp.json().get("data", {})
                            transfer_list = data.get("middleList", [])
                        except Exception:
                            if page_num == 1:
                                response_data = {"success": False, "error": "12306反爬拦截或数据异常，请稍后重试"}
                                return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
                            else:
                                break

                        if not transfer_list:
                            break

                        all_transfer_list.extend(transfer_list)

                        if len(transfer_list) < page_size:
                            break

                        result_index += page_size
                        page_num += 1

                    break
            except (httpx.TimeoutException, httpx.NetworkError, httpx.ConnectError) as e:
                last_exception = e
                all_transfer_list = []
                if attempt < max_retries - 1:
                    logger.warning(f"中转查询网络请求失败，正在重试 ({attempt + 1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"中转查询网络请求重试次数已耗尽: {str(e)}")
        else:
            response_data = {"success": False, "error": f"网络请求失败 (已重试{max_retries}次): {str(last_exception)}"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        if not all_transfer_list:
            response_data = {
                "success": False,
                "from_station": from_station,
                "to_station": to_station,
                "train_date": train_date,
                "count": 0,
                "transfers": [],
                "message": "未查到中转方案"
            }
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        transfers_list = []
        for item in all_transfer_list:
            try:
                full_list = item.get("fullList") or item.get("trainList") or []
                if len(full_list) < 2:
                    continue

                segments = []
                for seg in full_list:
                    seats = {}
                    seat_num = seg.get("swz_num", "")
                    if seat_num and seat_num != "--" and seat_num != "":
                        seats["商务座"] = seat_num
                    seat_num = seg.get("tz_num", "")
                    if seat_num and seat_num != "--" and seat_num != "":
                        seats["特等座"] = seat_num
                    seat_num = seg.get("zy_num", "")
                    if seat_num and seat_num != "--" and seat_num != "":
                        seats["一等座"] = seat_num
                    seat_num = seg.get("ze_num", "")
                    if seat_num and seat_num != "--" and seat_num != "":
                        seats["二等座"] = seat_num
                    seat_num = seg.get("gr_num", "")
                    if seat_num and seat_num != "--" and seat_num != "":
                        seats["高级软卧"] = seat_num
                    seat_num = seg.get("rw_num", "")
                    if seat_num and seat_num != "--" and seat_num != "":
                        seats["软卧"] = seat_num
                    seat_num = seg.get("rz_num", "")
                    if seat_num and seat_num != "--" and seat_num != "":
                        seats["一等卧"] = seat_num
                    seat_num = seg.get("yw_num", "")
                    if seat_num and seat_num != "--" and seat_num != "":
                        seats["硬卧"] = seat_num
                    seat_num = seg.get("yz_num", "")
                    if seat_num and seat_num != "--" and seat_num != "":
                        seats["硬座"] = seat_num
                    seat_num = seg.get("wz_num", "")
                    if seat_num and seat_num != "--" and seat_num != "":
                        seats["无座"] = seat_num

                    segment_data = {
                        "train_code": seg.get("station_train_code", ""),
                        "from_station": seg.get("from_station_name", ""),
                        "to_station": seg.get("to_station_name", ""),
                        "start_time": seg.get("start_time", ""),
                        "arrive_time": seg.get("arrive_time", ""),
                        "duration": seg.get("lishi", ""),
                        "seats": seats
                    }
                    segments.append(segment_data)

                transfer_data = {
                    "middle_station": item.get("middle_station_name") or (full_list[0].get("to_station_name", "") if full_list else ""),
                    "wait_time": item.get("wait_time", ""),
                    "total_duration": item.get("all_lishi", ""),
                    "segments": segments
                }
                transfers_list.append(transfer_data)

            except Exception as e:
                logger.warning(f"解析中转方案失败: {e}")
                continue

        response_data = {
            "success": True,
            "from_station": from_station,
            "to_station": to_station,
            "train_date": train_date,
            "count": len(transfers_list),
            "transfers": transfers_list
        }
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

    except Exception as e:
        logger.error(f"查询中转失败: {repr(e)}")
        response_data = {"success": False, "error": "查询中转失败", "detail": str(e)}
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]


# ========== 票价查询工具 ==========

async def query_ticket_price_validated(args: dict) -> list:
    try:
        from_station = args.get("from_station", "").strip()
        to_station = args.get("to_station", "").strip()
        train_date = args.get("train_date", "").strip()
        purpose_codes = args.get("purpose_codes", "ADULT").strip()
        train_code = args.get("train_code", "").strip().upper()

        if not from_station or not to_station or not train_date:
            response_data = {"success": False, "error": "请输入出发站、到达站和出发日期"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        is_valid, error_msg = validate_date_not_past(train_date)
        if not is_valid:
            response_data = {"success": False, "error": error_msg}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        from_code = await ensure_telecode(from_station)
        to_code = await ensure_telecode(to_station)

        if not from_code:
            response_data = {"success": False, "error": f"出发站无效: {from_station}"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
        if not to_code:
            response_data = {"success": False, "error": f"到达站无效: {to_station}"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        url_init = HTTP_URLS["init"]
        url_price = HTTP_URLS["query_price"]
        headers = HTTP_HEADERS.copy()

        params = {
            "leftTicketDTO.train_date": train_date,
            "leftTicketDTO.from_station": from_code,
            "leftTicketDTO.to_station": to_code,
            "purpose_codes": purpose_codes
        }

        max_retries = 3
        last_exception = None
        json_data = None

        for attempt in range(max_retries):
            try:
                async with create_12306_client() as client:
                    await client.get(url_init, headers=headers)
                    resp = await client.get(url_price, headers=headers, params=params)
                    logger.info(f"12306 price query status: {resp.status_code}, redirect: {get_redirect_info(resp)}")

                    if is_12306_error_response(resp):
                        logger.error(f"12306接口返回异常: {resp.status_code}, final_url: {resp.url}")
                        response_data = {
                            "success": False,
                            "error": "12306接口返回异常或反爬虫拦截",
                            "status_code": resp.status_code,
                            "final_url": str(resp.url),
                            "detail": resp.text[:200]
                        }
                        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

                    try:
                        json_data = resp.json()
                        break
                    except Exception as e:
                        logger.error(f"12306响应解析失败: {str(e)}")
                        response_data = {"success": False, "error": "12306响应解析失败", "detail": str(e)}
                        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
            except (httpx.TimeoutException, httpx.NetworkError, httpx.ConnectError) as e:
                last_exception = e
                if attempt < max_retries - 1:
                    logger.warning(f"票价查询网络请求失败，正在重试 ({attempt + 1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(1)
                else:
                    logger.error(f"票价查询网络请求重试次数已耗尽: {str(e)}")
        else:
            response_data = {"success": False, "error": f"网络请求失败 (已重试{max_retries}次): {str(last_exception)}"}
            return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]

        if json_data and "data" in json_data:
            result_data = []
            price_map = {
                "wz_price": "无座",
                "yz_price": "硬座",
                "yw_price": "硬卧",
                "rw_price": "软卧",
                "gr_price": "高级软卧",
                "ze_price": "二等座",
                "zy_price": "一等座",
                "swz_price": "商务座",
                "tdz_price": "特等座",
                "dw_price": "动卧"
            }

            for item in json_data.get("data", []):
                query_left_new_dto = item.get("queryLeftNewDTO", {})

                current_train_code = query_left_new_dto.get("station_train_code", "")
                if train_code and current_train_code != train_code:
                    continue

                train_info = {
                    "train_no": query_left_new_dto.get("train_no"),
                    "train_code": current_train_code,
                    "from_station": query_left_new_dto.get("from_station_name"),
                    "to_station": query_left_new_dto.get("to_station_name"),
                    "start_time": query_left_new_dto.get("start_time"),
                    "arrive_time": query_left_new_dto.get("arrive_time"),
                    "duration": query_left_new_dto.get("lishi"),
                    "train_class_name": query_left_new_dto.get("train_class_name"),
                    "prices": {}
                }

                for key, name in price_map.items():
                    price_val = query_left_new_dto.get(key)
                    if price_val and price_val != "--":
                        try:
                            if price_val.isdigit():
                                price_int = int(price_val)
                                price_str = str(price_int)
                                if len(price_str) == 1:
                                    formatted_price = "0." + price_str
                                else:
                                    formatted_price = price_str[:-1] + "." + price_str[-1]
                                train_info["prices"][name] = formatted_price
                            else:
                                train_info["prices"][name] = price_val
                        except Exception:
                            train_info["prices"][name] = price_val

                result_data.append(train_info)

            final_response = {
                "success": True,
                "from_station": from_station,
                "to_station": to_station,
                "train_date": train_date,
                "count": len(result_data),
                "data": result_data
            }
            return [{"type": "text", "text": json.dumps(final_response, ensure_ascii=False)}]

        return [{"type": "text", "text": json.dumps(json_data, ensure_ascii=False)}]

    except Exception as e:
        logger.error(f"查询票价失败: {repr(e)}")
        response_data = {"success": False, "error": "查询票价失败", "detail": str(e)}
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]


# ========== 当前时间工具 ==========

async def get_current_time_validated(args: dict) -> list:
    try:
        timezone_str = args.get("timezone", "Asia/Shanghai")
        try:
            tz = pytz.timezone(timezone_str)
            now = datetime.now(tz)
        except pytz.exceptions.UnknownTimeZoneError:
            tz = pytz.timezone("Asia/Shanghai")
            now = datetime.now(tz)

        response_data = {
            "success": True,
            "timezone": tz.zone,
            "datetime": now.strftime("%Y-%m-%d %H:%M:%S"),
            "date": now.strftime("%Y-%m-%d"),
            "time": now.strftime("%H:%M:%S"),
            "timestamp": int(now.timestamp())
        }
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
    except Exception as e:
        logger.error(f"获取时间信息失败: {repr(e)}")
        response_data = {"success": False, "error": "获取时间信息失败", "detail": str(e)}
        return [{"type": "text", "text": json.dumps(response_data, ensure_ascii=False)}]
