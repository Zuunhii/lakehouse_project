import os, json, re, argparse, time
from typing import Any, Dict, List
import requests

# ===== Config =====
SCHEMA_SILVER = os.getenv("TRINO_SCHEMA_SILVER", "silver")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg").strip()

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:3b-instruct")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "120"))
OLLAMA_NUM_PREDICT = int(os.getenv("OLLAMA_NUM_PREDICT", "450"))
OLLAMA_TEMPERATURE = float(os.getenv("OLLAMA_TEMPERATURE", "0.1"))

FORBIDDEN_SQL = [" drop ", " delete ", " insert ", " update ", " alter ", " merge ", " create ", " grant ", " revoke ", ";"]

# ===== Helpers =====
def _repair_json_newlines(s: str) -> str:
    out, in_str, esc = [], False, False
    for ch in s:
        if in_str:
            if esc:
                out.append(ch); esc = False; continue
            if ch == "\\": out.append(ch); esc = True; continue
            if ch == '"': out.append(ch); in_str = False; continue
            if ch in ("\n", "\r"): out.append("\\n"); continue
            out.append(ch)
        else:
            if ch == '"': out.append(ch); in_str = True
            else: out.append(ch)
    return "".join(out)

def _extract_json(text: str) -> Dict[str, Any]:
    t = text.strip()
    # strip codefence
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)

    try:
        return json.loads(t)
    except Exception:
        pass

    first, last = t.find("{"), t.rfind("}")
    if first != -1 and last != -1 and last > first:
        cand = _repair_json_newlines(t[first:last+1])
        return json.loads(cand)

    raise ValueError(f"Cannot parse JSON from LLM: {text[:300]}")

def call_ollama(prompt: str) -> Dict[str, Any]:
    url = f"{OLLAMA_URL}/api/generate"
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt + "\n\nNHẮC LẠI: chỉ trả JSON thuần, KHÔNG markdown, KHÔNG ```.",
        "stream": False,
        "options": {"temperature": OLLAMA_TEMPERATURE, "num_predict": OLLAMA_NUM_PREDICT},
    }
    r = requests.post(url, json=payload, timeout=OLLAMA_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    text = (data.get("response") or "").strip()
    try:
        return json.loads(text)
    except Exception:
        return _extract_json(text)

# ===== Intent contract =====
ALLOWED_INTENTS = {
    # xem sample rows theo issue flag
    "sample_rows": {"table": "detail|header", "issue": "detail.linetotal_mismatch|detail.discount_invalid|detail.dup_key|header.totaldue_invalid|header.status_null|header.orderdate_null|header.dup_key|header.null_key", "limit": "int"},
    # pareto theo issue
    "pareto_issues": {"limit": "int"},
    # dropped keys raw -> clean
    "dropped_keys": {"table": "detail|header", "limit": "int"},
    # lỗi tập trung theo product (detail)
    "by_product": {"issue": "detail.linetotal_mismatch|detail.discount_invalid|detail.dup_key|detail.null_key", "limit": "int"},
}

def build_intent_prompt(run_date: str, question_vi: str) -> str:
    return f"""
Bạn là bộ phân tích câu hỏi Data Quality để chọn INTENT và tham số.
CHỈ TRẢ VỀ 1 JSON. Không markdown.

Ngày điều tra (run_date): {run_date}
Câu hỏi người dùng: {question_vi}

Các INTENT hợp lệ:
1) sample_rows: lấy sample rows lỗi theo 1 issue. fields: table, issue, limit
2) pareto_issues: bảng thống kê issue/cnt. fields: limit
3) dropped_keys: key xuất hiện ở raw nhưng không có ở clean. fields: table, limit
Lưu ý: dropped_keys là tìm KEY/ROW bị loại (raw có, clean không có), KHÔNG phải “cột bị loại”.
4) by_product: lỗi tập trung theo productid (detail). fields: issue, limit

Ràng buộc:
- limit mặc định 20 nếu user không nói
- table mặc định 'detail' nếu không nói
- issue phải nằm trong danh sách cho phép (nếu user nói mơ hồ, chọn issue lớn nhất thường gặp: detail.linetotal_mismatch)
- output JSON theo format:

{{
  "intent": "sample_rows|pareto_issues|dropped_keys|by_product",
  "params": {{
     "table": "detail|header",
     "issue": "....",
     "limit": 20
  }},
  "reason": "1 câu tiếng Việt giải thích vì sao chọn intent này"
}}
""".strip()

def validate_intent_obj(obj: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        raise ValueError("intent obj must be dict")

    intent = str(obj.get("intent", "")).strip()
    if intent not in ALLOWED_INTENTS:
        raise ValueError(f"invalid intent: {intent}")

    params = obj.get("params") or {}
    if not isinstance(params, dict):
        params = {}

    # ---- limit (default + clamp) ----
    limit = params.get("limit", 20)
    try:
        limit = int(limit)
    except Exception:
        limit = 20
    limit = max(1, min(200, limit))

    # ---- normalize theo intent ----
    cleaned: Dict[str, Any] = {"limit": limit}

    if intent in ("sample_rows", "dropped_keys"):
        table = str(params.get("table", "detail")).strip().lower()
        if table not in ("detail", "header"):
            table = "detail"
        cleaned["table"] = table

    if intent in ("sample_rows", "by_product"):
        issue = str(params.get("issue", "detail.linetotal_mismatch")).strip()
        allowed_issue = ALLOWED_INTENTS[intent]["issue"].split("|")
        if issue not in allowed_issue:
            issue = "detail.linetotal_mismatch"
        cleaned["issue"] = issue

    # ---- set back ----
    obj["intent"] = intent
    obj["params"] = cleaned
    obj["reason"] = str(obj.get("reason", "")).strip()[:300]

    return obj

# ===== SQL rendering (deterministic templates) =====
def _tbl(name: str) -> str:
    return f"{TRINO_CATALOG}.{SCHEMA_SILVER}.{name}"

def _assert_safe_sql(sql: str, run_date: str) -> None:
    low = f" {sql.lower()} "
    if not (low.strip().startswith("select") or low.strip().startswith("with")):
        raise ValueError("SQL must start with SELECT/WITH")
    if any(bad in low for bad in FORBIDDEN_SQL):
        raise ValueError("SQL contains forbidden keyword")
    if f"date '{run_date}'".lower() not in low:
        raise ValueError("SQL must contain etl_date = DATE 'run_date'")

def render_sql(intent: str, params: Dict[str, Any], run_date: str) -> List[str]:
    date_filter = f"etl_date = DATE '{run_date}'"
    out: List[str] = []

    if intent == "pareto_issues":
        sql = f"""
SELECT issue, cnt
FROM (
  SELECT 'detail.linetotal_mismatch' AS issue, sum(f_linetotal_mismatch) AS cnt FROM {_tbl('dq_sales_order_detail_raw')} WHERE {date_filter}
  UNION ALL SELECT 'detail.discount_invalid', sum(f_discount_invalid) FROM {_tbl('dq_sales_order_detail_raw')} WHERE {date_filter}
  UNION ALL SELECT 'detail.dup_key', sum(f_dup_key) FROM {_tbl('dq_sales_order_detail_raw')} WHERE {date_filter}
  UNION ALL SELECT 'detail.null_key', sum(f_null_key) FROM {_tbl('dq_sales_order_detail_raw')} WHERE {date_filter}

  UNION ALL SELECT 'header.totaldue_invalid', sum(f_totaldue_invalid) FROM {_tbl('dq_sales_order_header_raw')} WHERE {date_filter}
  UNION ALL SELECT 'header.status_null', sum(f_status_null) FROM {_tbl('dq_sales_order_header_raw')} WHERE {date_filter}
  UNION ALL SELECT 'header.orderdate_null', sum(f_orderdate_null) FROM {_tbl('dq_sales_order_header_raw')} WHERE {date_filter}
  UNION ALL SELECT 'header.dup_key', sum(f_dup_key) FROM {_tbl('dq_sales_order_header_raw')} WHERE {date_filter}
  UNION ALL SELECT 'header.null_key', sum(f_null_key) FROM {_tbl('dq_sales_order_header_raw')} WHERE {date_filter}
)
WHERE cnt IS NOT NULL AND cnt > 0
ORDER BY cnt DESC
LIMIT {params["limit"]}
""".strip()
        _assert_safe_sql(sql, run_date)
        out.append(sql)
        return out

    if intent == "dropped_keys":
        table = params["table"]
        lim = params["limit"]
        if table == "detail":
            sql = f"""
WITH r AS (
  SELECT DISTINCT salesorderid, salesorderdetailid
  FROM {_tbl('dq_sales_order_detail_raw')}
  WHERE {date_filter}
),
c AS (
  SELECT DISTINCT salesorderid, salesorderdetailid
  FROM {_tbl('dq_sales_order_detail_clean')}
  WHERE {date_filter}
)
SELECT r.*
FROM r
LEFT JOIN c
  ON r.salesorderid = c.salesorderid
 AND r.salesorderdetailid = c.salesorderdetailid
WHERE c.salesorderid IS NULL
LIMIT {lim}
""".strip()
        else:
            sql = f"""
WITH r AS (
  SELECT DISTINCT salesorderid
  FROM {_tbl('dq_sales_order_header_raw')}
  WHERE {date_filter}
),
c AS (
  SELECT DISTINCT salesorderid
  FROM {_tbl('dq_sales_order_header_clean')}
  WHERE {date_filter}
)
SELECT r.salesorderid
FROM r
LEFT JOIN c ON r.salesorderid = c.salesorderid
WHERE c.salesorderid IS NULL
LIMIT {lim}
""".strip()
        _assert_safe_sql(sql, run_date)
        out.append(sql)
        return out

    if intent == "sample_rows":
        issue = params["issue"]
        table = params["table"]
        lim = params["limit"]

        if issue.startswith("detail.") or table == "detail":
            flag = issue.split(".", 1)[1]
            flag_col = {
                "linetotal_mismatch": "f_linetotal_mismatch",
                "discount_invalid": "f_discount_invalid",
                "dup_key": "f_dup_key",
                "null_key": "f_null_key",
            }.get(flag, "f_linetotal_mismatch")

            sql = f"""
SELECT *
FROM {_tbl('dq_sales_order_detail_raw')}
WHERE {date_filter}
  AND {flag_col} = 1
LIMIT {lim}
""".strip()
        else:
            flag = issue.split(".", 1)[1]
            flag_col = {
                "totaldue_invalid": "f_totaldue_invalid",
                "status_null": "f_status_null",
                "orderdate_null": "f_orderdate_null",
                "dup_key": "f_dup_key",
                "null_key": "f_null_key",
            }.get(flag, "f_totaldue_invalid")

            sql = f"""
SELECT *
FROM {_tbl('dq_sales_order_header_raw')}
WHERE {date_filter}
  AND {flag_col} = 1
LIMIT {lim}
""".strip()

        _assert_safe_sql(sql, run_date)
        out.append(sql)
        return out

    if intent == "by_product":
        # chỉ detail raw, group productid
        issue = params["issue"]
        lim = params["limit"]
        flag = issue.split(".", 1)[1]
        flag_col = {
            "linetotal_mismatch": "f_linetotal_mismatch",
            "discount_invalid": "f_discount_invalid",
            "dup_key": "f_dup_key",
            "null_key": "f_null_key",
        }.get(flag, "f_linetotal_mismatch")

        sql = f"""
SELECT productid, count(*) AS bad_cnt
FROM {_tbl('dq_sales_order_detail_raw')}
WHERE {date_filter}
  AND {flag_col} = 1
GROUP BY 1
ORDER BY bad_cnt DESC
LIMIT {lim}
""".strip()
        _assert_safe_sql(sql, run_date)
        out.append(sql)
        return out

    raise ValueError("unhandled intent")

def run_copilot(run_date: str, question: str) -> Dict[str, Any]:
    prompt = build_intent_prompt(run_date, question)
    obj = call_ollama(prompt)
    obj = validate_intent_obj(obj)
    sqls = render_sql(obj["intent"], obj["params"], run_date)
    return {"intent": obj["intent"], "params": obj["params"], "reason": obj["reason"], "sql": sqls}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_date", required=True)
    ap.add_argument("--question", required=True)
    args = ap.parse_args()

    res = run_copilot(args.run_date, args.question)
    print(json.dumps(res, ensure_ascii=False, indent=2))
