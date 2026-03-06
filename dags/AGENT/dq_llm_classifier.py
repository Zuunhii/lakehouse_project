import os
import json
import re
import argparse
import time
from typing import Any, Dict, List

import requests
from airflow.hooks.base import BaseHook

from trino.dbapi import connect
from trino.auth import BasicAuthentication


# ----------------------------
# CONFIG
# ----------------------------
TRINO_CONN_ID = "trino_default"

# thêm ở đầu file (cùng chỗ config)
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg").strip()

SCHEMA_SILVER = os.getenv("TRINO_SCHEMA_SILVER", "silver")
SCHEMA_GOLD = os.getenv("TRINO_SCHEMA_GOLD", "gold")

# Ollama local
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:3b-instruct")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "120"))  # seconds
OLLAMA_NUM_PREDICT = int(os.getenv("OLLAMA_NUM_PREDICT", "900"))  # ~max tokens output
OLLAMA_TEMPERATURE = float(os.getenv("OLLAMA_TEMPERATURE", "0.2"))
OLLAMA_RETRIES = int(os.getenv("OLLAMA_RETRIES", "2"))
OLLAMA_RETRY_SLEEP = float(os.getenv("OLLAMA_RETRY_SLEEP", "1.5"))

FORBIDDEN_SQL = [" drop ", " delete ", " insert ", " update ", " alter ", " merge ", " create ", " grant ", " revoke "]


# ----------------------------
# Helpers
# ----------------------------
def esc_sql(s: str) -> str:
    return str(s).replace("'", "''")


def trino_conn(schema: str):
    """
    Lấy config từ Airflow Connection trino_default.
    Extra JSON hỗ trợ:
      - catalog: iceberg/hive/...
      - http_scheme: https/http
      - verify: false/true (self-signed -> false cho demo)
    """
    c = BaseHook.get_connection(TRINO_CONN_ID)
    extra = c.extra_dejson or {}

    catalog = extra.get("catalog") or "iceberg"
    http_scheme = extra.get("http_scheme") or "https"

    verify = extra.get("verify", False)
    if isinstance(verify, str):
        verify = verify.strip().lower() in ("1", "true", "yes", "y")

    auth = None
    if c.password:
        auth = BasicAuthentication(c.login, c.password)

    return connect(
        host=c.host,
        port=int(c.port or 8443),
        user=c.login,
        auth=auth,
        http_scheme=http_scheme,
        verify=verify,
        catalog=catalog,
        schema=schema,
    )


def fetch_all(conn, sql: str) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


# ----------------------------
# JSON extraction (robust)
# ----------------------------
def _repair_json_newlines(s: str) -> str:
    """
    JSON chuẩn không cho newline thật nằm trong string.
    Hàm này đổi newline trong vùng "..." thành \\n.
    """
    out = []
    in_str = False
    esc = False

    for ch in s:
        if in_str:
            if esc:
                out.append(ch)
                esc = False
                continue

            if ch == "\\":
                out.append(ch)
                esc = True
                continue

            if ch == '"':
                out.append(ch)
                in_str = False
                continue

            if ch == "\n" or ch == "\r":
                out.append("\\n")
                continue

            out.append(ch)
        else:
            if ch == '"':
                out.append(ch)
                in_str = True
            else:
                out.append(ch)

    return "".join(out)


def _extract_json(text: str) -> Dict[str, Any]:
    t = text.strip()

    # Remove code fences if present
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)

    # Try direct parse
    try:
        return json.loads(t)
    except Exception:
        pass

    # Try extract biggest {...} block
    first = t.find("{")
    last = t.rfind("}")
    if first != -1 and last != -1 and last > first:
        candidate = t[first:last + 1]
        candidate2 = _repair_json_newlines(candidate)
        return json.loads(candidate2)

    raise ValueError(f"Cannot parse JSON from LLM response: {text[:400]}")


# ----------------------------
# Ollama call
# ----------------------------
def call_ollama(prompt: str) -> dict:
    """
    Call local Ollama (no API key).
    Endpoint: POST {OLLAMA_URL}/api/generate
    """
    url = f"{OLLAMA_URL}/api/generate"

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": OLLAMA_TEMPERATURE,
            "num_predict": OLLAMA_NUM_PREDICT,
        },
    }

    last_err = None
    for attempt in range(max(1, OLLAMA_RETRIES)):
        try:
            r = requests.post(url, json=payload, timeout=OLLAMA_TIMEOUT)
            # Retry on transient server errors
            if r.status_code in (429, 500, 502, 503, 504) and attempt < OLLAMA_RETRIES - 1:
                time.sleep(OLLAMA_RETRY_SLEEP)
                continue

            r.raise_for_status()
            data = r.json()

            # Ollama returns {"response": "...", "done": true, ...}
            text = (data.get("response") or "").strip()
            if not text:
                raise RuntimeError(f"Ollama response missing 'response': {data}")

            # Debug light
            print("ollama_done:", data.get("done"), flush=True)
            if "eval_count" in data:
                print("ollama_eval_count:", data.get("eval_count"), flush=True)
            if "prompt_eval_count" in data:
                print("ollama_prompt_eval_count:", data.get("prompt_eval_count"), flush=True)
            print("text_len:", len(text), flush=True)
            print("text_tail:", text[-300:], flush=True)

            # Parse JSON (ollama không có responseSchema nên phải tự parse)
            try:
                return json.loads(text)
            except Exception:
                return _extract_json(text)

        except Exception as e:
            last_err = e
            if attempt < OLLAMA_RETRIES - 1:
                time.sleep(OLLAMA_RETRY_SLEEP)
                continue
            break

    raise RuntimeError(f"Ollama call failed after retries: {last_err}")


# ----------------------------
# Validation
# ----------------------------
def validate_llm_obj(obj: Dict[str, Any], run_date: str) -> Dict[str, Any]:
    """
    New contract:
      LLM returns ONLY:
        {
          "narrative": "...",
          "top_issues": [{"issue": "...", "cnt": number, "explanation": "..."}, ...]
        }
    Verdict + drilldown_sql_* are deterministic in Python, not generated by LLM.
    """
    if not isinstance(obj, dict):
        raise ValueError("LLM output must be a JSON object (dict)")

    # Only require these 2 keys
    must = ["narrative", "top_issues"]
    for k in must:
        if k not in obj:
            raise ValueError(f"LLM JSON missing key: {k}")

    # narrative
    narrative = str(obj.get("narrative", "")).strip()
    if not narrative:
        narrative = f"Ngày {run_date}: (LLM trả narrative rỗng)."
    if len(narrative) > 2000:
        narrative = narrative[:2000]
    obj["narrative"] = narrative

    # top_issues
    ti = obj.get("top_issues")
    if not isinstance(ti, list):
        # Nếu LLM trả sai format, convert về list 1 phần tử
        obj["top_issues"] = [{
            "issue": "unknown",
            "cnt": None,
            "explanation": str(ti)
        }]
        return obj

    cleaned: List[Dict[str, Any]] = []
    for x in ti[:5]:
        if not isinstance(x, dict):
            cleaned.append({"issue": "unknown", "cnt": None, "explanation": str(x)})
            continue

        issue = str(x.get("issue", "unknown")).strip() or "unknown"

        # cnt: cố gắng ép về number, nếu không được thì None
        cnt_raw = x.get("cnt", None)
        try:
            cnt = None if cnt_raw is None else float(cnt_raw)
            # nếu muốn giữ int đẹp:
            if cnt is not None and cnt.is_integer():
                cnt = int(cnt)
        except Exception:
            cnt = None

        explanation = str(x.get("explanation", "")).strip()
        if not explanation:
            explanation = "Không có giải thích."

        cleaned.append({"issue": issue, "cnt": cnt, "explanation": explanation})

    obj["top_issues"] = cleaned
    return obj


# ----------------------------
# Metrics loading (từ dbt tables)
# ----------------------------
def load_metrics(run_date: str) -> Dict[str, Any]:
    gold = trino_conn(SCHEMA_GOLD)
    silver = trino_conn(SCHEMA_SILVER)

    summary_sql = f"""
    SELECT etl_date, object_name, dataset, dq_class, row_cnt
    FROM {SCHEMA_GOLD}.dq_compare_daily_summary
    WHERE etl_date BETWEEN date '{run_date}' - interval '6' day AND date '{run_date}'
    """
    summary_7d = fetch_all(gold, summary_sql)

    def get_cnt(dataset: str, dq_class: str, object_name: str | None = None) -> int:
        s = 0
        for r in summary_7d:
            if str(r["etl_date"]) != run_date:
                continue
            if r["dataset"] != dataset or r["dq_class"] != dq_class:
                continue
            if object_name is not None and r["object_name"] != object_name:
                continue
            s += int(r["row_cnt"])
        return s


    raw_fail = get_cnt("raw", "FAIL")
    raw_warn = get_cnt("raw", "WARN")
    raw_pass = get_cnt("raw", "PASS")
    raw_cnt = raw_fail + raw_warn + raw_pass

    clean_fail = get_cnt("clean", "FAIL")
    clean_warn = get_cnt("clean", "WARN")
    clean_pass = get_cnt("clean", "PASS")
    clean_cnt = clean_fail + clean_warn + clean_pass

    delta = raw_cnt - clean_cnt

    top_issue_sql = f"""
    SELECT issue, cnt
    FROM (
    -- DETAIL RAW
    SELECT 'detail.null_key' AS issue, sum(f_null_key) AS cnt
        FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = DATE '{run_date}'
    UNION ALL SELECT 'detail.dup_key', sum(f_dup_key)
        FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = DATE '{run_date}'
    UNION ALL SELECT 'detail.qty_invalid', sum(f_qty_invalid)
        FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = DATE '{run_date}'
    UNION ALL SELECT 'detail.price_invalid', sum(f_price_invalid)
        FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = DATE '{run_date}'
    UNION ALL SELECT 'detail.discount_invalid', sum(f_discount_invalid)
        FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = DATE '{run_date}'
    UNION ALL SELECT 'detail.linetotal_invalid', sum(f_linetotal_invalid)
        FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = DATE '{run_date}'
    UNION ALL SELECT 'detail.linetotal_mismatch', sum(f_linetotal_mismatch)
        FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = DATE '{run_date}'

    -- HEADER RAW
    UNION ALL SELECT 'header.null_key', sum(f_null_key)
        FROM {SCHEMA_SILVER}.dq_sales_order_header_raw WHERE etl_date = DATE '{run_date}'
    UNION ALL SELECT 'header.orderdate_null', sum(f_orderdate_null)
        FROM {SCHEMA_SILVER}.dq_sales_order_header_raw WHERE etl_date = DATE '{run_date}'
    UNION ALL SELECT 'header.status_null', sum(f_status_null)
        FROM {SCHEMA_SILVER}.dq_sales_order_header_raw WHERE etl_date = DATE '{run_date}'
    UNION ALL SELECT 'header.totaldue_invalid', sum(f_totaldue_invalid)
        FROM {SCHEMA_SILVER}.dq_sales_order_header_raw WHERE etl_date = DATE '{run_date}'
    UNION ALL SELECT 'header.dup_key', sum(f_dup_key)
        FROM {SCHEMA_SILVER}.dq_sales_order_header_raw WHERE etl_date = DATE '{run_date}'
    )
    WHERE cnt IS NOT NULL
    ORDER BY cnt DESC
    LIMIT 5
    """

    top_issues_today = fetch_all(silver, top_issue_sql)

    return {
        "raw_cnt": raw_cnt,
        "raw_pass": raw_pass,
        "raw_warn": raw_warn,
        "raw_fail": raw_fail,
        "clean_cnt": clean_cnt,
        "clean_pass": clean_pass,
        "clean_warn": clean_warn,
        "clean_fail": clean_fail,
        "delta_raw_clean": delta,
        "top_issues_today": top_issues_today,
        "summary_7d": summary_7d,
    }

def build_prompt(run_date: str, m: Dict[str, Any]) -> str:
    """
    Prompt version “nhẹ” để tránh Ollama timeout:
    - LLM CHỈ viết narrative + giải thích top issues (không bắt sinh SQL).
    - verdict được truyền vào từ code deterministic (m["verdict"]).
    - top_issues_today là list [{"issue": "...", "cnt": ...}, ...] (từ SQL).
    """
    verdict = m.get("verdict", "WARN")

    # Lấy top issues (đã tính từ SQL) – chỉ đưa top 10 vào prompt cho gọn
    top_issues_in = (m.get("top_issues_today") or [])[:10]

    # Ensure json dumps tiếng Việt không bị escape
    top_issues_json = json.dumps(top_issues_in, ensure_ascii=False)

    return f"""
Bạn là trợ lý tóm tắt Data Quality cho pipeline lakehouse.
CHỈ TRẢ VỀ 1 JSON DUY NHẤT (không markdown, không ```), không thêm chữ nào khác.
KHÔNG được bịa số: mọi con số phải lấy từ INPUT bên dưới.

========================
INPUT (ngày {run_date})
========================
VERDICT (đã tính theo rule, không được đổi): {verdict}

RAW:   total={m["raw_cnt"]}, PASS={m["raw_pass"]}, WARN={m["raw_warn"]}, FAIL={m["raw_fail"]}
CLEAN: total={m["clean_cnt"]}, PASS={m["clean_pass"]}, WARN={m["clean_warn"]}, FAIL={m["clean_fail"]}
delta_raw_clean = {m["delta_raw_clean"]}

Top issues (RAW flags) - list issue/cnt:
{top_issues_json}

========================
YÊU CẦU (BẮT BUỘC)
========================
1) narrative (Tiếng Việt, tối đa 8 dòng):
   - BẮT BUỘC nhắc đủ 5 số: RAW total, CLEAN total, FAIL, WARN, delta_raw_clean
   - Nêu 1–2 issue lớn nhất (dựa trên cnt trong Top issues input)
   - Kết luận phải khớp với VERDICT đã cho (không được nói “không có FAIL” nếu FAIL > 0)
2) top_issues (tối đa 5 phần tử):
   - Chỉ được chọn từ danh sách Top issues input (không bịa issue mới)
   - Mỗi phần tử gồm: issue, cnt, explanation (1 câu ngắn, giải thích lỗi đó nghĩa là gì/ảnh hưởng gì)
   - cnt phải giữ nguyên đúng số từ input
   - explanation BẮT BUỘC viết tiếng Việt

========================
OUTPUT (CHỈ JSON)
========================
{{
  "narrative": "text",
  "top_issues": [
    {{"issue": "string", "cnt": number, "explanation": "string"}}
  ]
}}
""".strip()




def build_drilldown_sql(run_date: str) -> Dict[str, str]:
    cat = os.getenv("TRINO_CATALOG", "iceberg").strip()

    detail_raw_tbl   = f"{cat}.{SCHEMA_SILVER}.dq_sales_order_detail_raw"
    detail_clean_tbl = f"{cat}.{SCHEMA_SILVER}.dq_sales_order_detail_clean"
    hdr_raw_tbl      = f"{cat}.{SCHEMA_SILVER}.dq_sales_order_header_raw"
    hdr_clean_tbl    = f"{cat}.{SCHEMA_SILVER}.dq_sales_order_header_clean"

    date_filter = f"etl_date = DATE '{run_date}'"

    # ---------------------------------------------------------
    # SQL1: sample rows lỗi nặng nhất (FAIL trước, score cao trước)
    # ---------------------------------------------------------
    sql1 = f"""
WITH detail_bad AS (
  SELECT
    'detail' AS object_name,
    etl_date,
    dq_class,
    dq_score,

    salesorderid,
    salesorderdetailid,
    productid,

    CAST(NULL AS bigint)  AS customerid,
    CAST(NULL AS bigint)  AS creditcardid,
    CAST(NULL AS bigint)  AS shiptoaddressid,
    CAST(NULL AS bigint)  AS status,
    CAST(NULL AS date)    AS orderdate,
    CAST(NULL AS double)  AS totaldue,

    CAST(orderqty AS bigint)          AS orderqty,
    CAST(unitprice AS double)         AS unitprice,
    CAST(unitpricediscount AS double) AS unitpricediscount,
    CAST(linetotal AS double)         AS linetotal,

    f_null_key,
    f_dup_key,
    f_qty_invalid,
    f_price_invalid,
    f_discount_invalid,
    f_linetotal_invalid,
    f_linetotal_mismatch,

    CAST(NULL AS bigint) AS f_orderdate_null,
    CAST(NULL AS bigint) AS f_status_null,
    CAST(NULL AS bigint) AS f_totaldue_invalid
  FROM {detail_raw_tbl}
  WHERE {date_filter}
    AND (
      f_null_key = 1 OR f_dup_key = 1 OR f_qty_invalid = 1 OR f_price_invalid = 1 OR
      f_discount_invalid = 1 OR f_linetotal_invalid = 1 OR f_linetotal_mismatch = 1
    )
),
header_bad AS (
  SELECT
    'header' AS object_name,
    etl_date,
    dq_class,
    dq_score,

    salesorderid,
    CAST(NULL AS bigint) AS salesorderdetailid,
    CAST(NULL AS bigint) AS productid,

    CAST(customerid AS bigint)      AS customerid,
    CAST(creditcardid AS bigint)    AS creditcardid,
    CAST(shiptoaddressid AS bigint) AS shiptoaddressid,
    CAST(status AS bigint)          AS status,
    CAST(orderdate AS date)         AS orderdate,
    CAST(totaldue AS double)        AS totaldue,

    CAST(NULL AS bigint)  AS orderqty,
    CAST(NULL AS double)  AS unitprice,
    CAST(NULL AS double)  AS unitpricediscount,
    CAST(NULL AS double)  AS linetotal,

    f_null_key,
    f_dup_key,
    CAST(NULL AS bigint) AS f_qty_invalid,
    CAST(NULL AS bigint) AS f_price_invalid,
    CAST(NULL AS bigint) AS f_discount_invalid,
    CAST(NULL AS bigint) AS f_linetotal_invalid,
    CAST(NULL AS bigint) AS f_linetotal_mismatch,

    f_orderdate_null,
    f_status_null,
    f_totaldue_invalid
  FROM {hdr_raw_tbl}
  WHERE {date_filter}
    AND (
      f_null_key = 1 OR f_dup_key = 1 OR
      f_orderdate_null = 1 OR f_status_null = 1 OR f_totaldue_invalid = 1
    )
)
SELECT *
FROM (
  SELECT * FROM detail_bad
  UNION ALL
  SELECT * FROM header_bad
)
ORDER BY
  CASE dq_class WHEN 'FAIL' THEN 2 WHEN 'WARN' THEN 1 ELSE 0 END DESC,
  dq_score DESC
LIMIT 200
""".strip()

    # ---------------------------------------------------------
    # SQL2: Pareto issues (lọc cnt > 0 cho sạch)
    # ---------------------------------------------------------
    sql2 = f"""
SELECT issue, cnt
FROM (
  SELECT 'detail.null_key' AS issue, sum(f_null_key) AS cnt FROM {detail_raw_tbl} WHERE {date_filter}
  UNION ALL SELECT 'detail.dup_key', sum(f_dup_key) FROM {detail_raw_tbl} WHERE {date_filter}
  UNION ALL SELECT 'detail.qty_invalid', sum(f_qty_invalid) FROM {detail_raw_tbl} WHERE {date_filter}
  UNION ALL SELECT 'detail.price_invalid', sum(f_price_invalid) FROM {detail_raw_tbl} WHERE {date_filter}
  UNION ALL SELECT 'detail.discount_invalid', sum(f_discount_invalid) FROM {detail_raw_tbl} WHERE {date_filter}
  UNION ALL SELECT 'detail.linetotal_invalid', sum(f_linetotal_invalid) FROM {detail_raw_tbl} WHERE {date_filter}
  UNION ALL SELECT 'detail.linetotal_mismatch', sum(f_linetotal_mismatch) FROM {detail_raw_tbl} WHERE {date_filter}

  UNION ALL SELECT 'header.null_key', sum(f_null_key) FROM {hdr_raw_tbl} WHERE {date_filter}
  UNION ALL SELECT 'header.dup_key', sum(f_dup_key) FROM {hdr_raw_tbl} WHERE {date_filter}
  UNION ALL SELECT 'header.orderdate_null', sum(f_orderdate_null) FROM {hdr_raw_tbl} WHERE {date_filter}
  UNION ALL SELECT 'header.status_null', sum(f_status_null) FROM {hdr_raw_tbl} WHERE {date_filter}
  UNION ALL SELECT 'header.totaldue_invalid', sum(f_totaldue_invalid) FROM {hdr_raw_tbl} WHERE {date_filter}
)
WHERE cnt IS NOT NULL AND cnt > 0
ORDER BY cnt DESC
LIMIT 10
""".strip()

    # ---------------------------------------------------------
    # SQL3: keys dropped in clean (DISTINCT để tránh dup keys)
    # ---------------------------------------------------------
    sql3 = f"""
WITH r_detail AS (
  SELECT DISTINCT salesorderid, salesorderdetailid
  FROM {detail_raw_tbl}
  WHERE {date_filter}
),
c_detail AS (
  SELECT DISTINCT salesorderid, salesorderdetailid
  FROM {detail_clean_tbl}
  WHERE {date_filter}
),
dropped_detail AS (
  SELECT 'detail' AS object_name, r.salesorderid, r.salesorderdetailid
  FROM r_detail r
  LEFT JOIN c_detail c
    ON r.salesorderid = c.salesorderid AND r.salesorderdetailid = c.salesorderdetailid
  WHERE c.salesorderid IS NULL
),
r_header AS (
  SELECT DISTINCT salesorderid
  FROM {hdr_raw_tbl}
  WHERE {date_filter}
),
c_header AS (
  SELECT DISTINCT salesorderid
  FROM {hdr_clean_tbl}
  WHERE {date_filter}
),
dropped_header AS (
  SELECT 'header' AS object_name, r.salesorderid, CAST(NULL AS bigint) AS salesorderdetailid
  FROM r_header r
  LEFT JOIN c_header c
    ON r.salesorderid = c.salesorderid
  WHERE c.salesorderid IS NULL
)
SELECT * FROM dropped_detail
UNION ALL
SELECT * FROM dropped_header
LIMIT 200
""".strip()

    return {
        "drilldown_sql_1": sql1,
        "drilldown_sql_2": sql2,
        "drilldown_sql_3": sql3,
    }


# ----------------------------
# Write output table
# ----------------------------
def upsert_report(run_date: str, metrics: Dict[str, Any], llm_obj: Dict[str, Any]) -> None:
    gold = trino_conn(SCHEMA_GOLD)
    cur = gold.cursor()

    cur.execute(
        f"DELETE FROM {SCHEMA_GOLD}.dq_agent_report_daily "
        f"WHERE etl_date = DATE '{run_date}' AND object_name = 'daily'"
    )

    top_issues_str = json.dumps(llm_obj["top_issues"], ensure_ascii=False)

    insert_sql = f"""
    INSERT INTO {SCHEMA_GOLD}.dq_agent_report_daily (
      etl_date, object_name, window_days,
      raw_cnt, clean_cnt, fail_cnt, warn_cnt, pass_cnt, delta_raw_clean,
      top_issues, compare_changes, narrative,
      drilldown_sql_1, drilldown_sql_2, drilldown_sql_3,
      created_at
    ) VALUES (
      DATE '{run_date}', 'daily', 7,
      {int(metrics["raw_cnt"])}, {int(metrics["clean_cnt"])}, {int(metrics["raw_fail"])}, {int(metrics["raw_warn"])}, {int(metrics["raw_pass"])}, {int(metrics["delta_raw_clean"])},
      '{esc_sql(top_issues_str)}', '',
      '{esc_sql(llm_obj["narrative"])}',
      '{esc_sql(llm_obj["drilldown_sql_1"])}', '{esc_sql(llm_obj["drilldown_sql_2"])}', '{esc_sql(llm_obj["drilldown_sql_3"])}',
      current_timestamp(6)
    )
    """
    cur.execute(insert_sql)


# ----------------------------
# Main
# ----------------------------
def main(run_date: str) -> None:
    print(
        f"[dq_llm_classifier] run_date={run_date} conn_id={TRINO_CONN_ID} "
        f"ollama_model={OLLAMA_MODEL} ollama_url={OLLAMA_URL}"
    )

    # 1) Load metrics deterministic (từ SQL)
    metrics = load_metrics(run_date)

    # 2) Verdict deterministic (không nhờ LLM)
    issues_today = metrics.get("top_issues_today") or []
    any_issue_gt0 = any((x.get("cnt", 0) or 0) > 0 for x in issues_today)

    if metrics["raw_fail"] > 0 or metrics["delta_raw_clean"] > 0:
        verdict = "FAIL"
    elif metrics["raw_warn"] > 0 or any_issue_gt0:
        verdict = "WARN"
    else:
        verdict = "PASS"

    metrics["verdict"] = verdict  # để build_prompt dùng

    # 3) Drilldown SQL deterministic (không nhờ LLM)
    drills = build_drilldown_sql(run_date)  # <- nhớ đã thêm hàm này

    # 4) Call LLM chỉ để viết narrative + giải thích top issues (fallback nếu timeout)
    prompt = build_prompt(run_date, metrics)

    try:
        llm_small = call_ollama(prompt)
        llm_small = validate_llm_obj(llm_small, run_date)  # <- validate bản mới (chỉ narrative/top_issues)
    except Exception as e:
        # fallback: không để DAG chết khi Ollama timeout
        llm_small = {
            "narrative": (
                f"Ngày {run_date}: RAW total={metrics['raw_cnt']}, CLEAN total={metrics['clean_cnt']}, "
                f"FAIL={metrics['raw_fail']}, WARN={metrics['raw_warn']}, delta_raw_clean={metrics['delta_raw_clean']}.\n"
                f"Verdict={verdict}. (Fallback narrative do LLM lỗi: {type(e).__name__})"
            ),
            "top_issues": (issues_today[:5] if isinstance(issues_today, list) else []),
        }

    # 5) Hợp nhất thành object “đầy đủ” để upsert vào bảng report
    llm_obj_full = {
        "verdict": verdict,
        "narrative": llm_small["narrative"],
        "top_issues": llm_small.get("top_issues", [])[:5],
        "drilldown_sql_1": drills["drilldown_sql_1"],
        "drilldown_sql_2": drills["drilldown_sql_2"],
        "drilldown_sql_3": drills["drilldown_sql_3"],
    }

    # 6) Upsert report
    upsert_report(run_date, metrics, llm_obj_full)
    print("[dq_llm_classifier] inserted report into gold.dq_agent_report_daily")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.run_date)
