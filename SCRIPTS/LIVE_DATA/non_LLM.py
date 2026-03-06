import os
import json
import re
import argparse
from typing import Any, Dict, List

import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from trino.dbapi import connect
from trino.auth import BasicAuthentication


# ----------------------------
# CONFIG
# ----------------------------
TRINO_CONN_ID = "trino_default"  # <- theo yêu cầu của mày

SCHEMA_SILVER = os.getenv("TRINO_SCHEMA_SILVER", "silver")
SCHEMA_GOLD = os.getenv("TRINO_SCHEMA_GOLD", "gold")

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
GEMINI_TIMEOUT = int(os.getenv("GEMINI_TIMEOUT", "60"))

FORBIDDEN_SQL = [" drop ", " delete ", " insert ", " update ", " alter ", " merge ", " create ", " grant ", " revoke "]


# ----------------------------
# Helpers
# ----------------------------
def esc_sql(s: str) -> str:
    return str(s).replace("'", "''")


def get_gemini_key() -> str:
    key = os.getenv("GEMINI_API_KEY")
    if key:
        return key
    try:
        key = Variable.get("GEMINI_API_KEY")
        if key:
            return key
    except Exception:
        pass
    raise RuntimeError("Missing GEMINI_API_KEY (env) or Airflow Variable GEMINI_API_KEY")


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
# Gemini call + JSON extraction
# ----------------------------
def _extract_json(text: str) -> Dict[str, Any]:
    t = text.strip()

    # Remove code fences if present
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)

    # direct parse
    try:
        return json.loads(t)
    except Exception:
        pass

    # find first JSON object
    first = t.find("{")
    last = t.rfind("}")
    if first != -1 and last != -1 and last > first:
        return json.loads(t[first:last + 1])

    raise ValueError(f"Cannot parse JSON from Gemini response: {text[:400]}")


def call_gemini(prompt: str) -> Dict[str, Any]:
    key = get_gemini_key()
    url = (
        "https://generativelanguage.googleapis.com/v1beta/"
        f"models/{GEMINI_MODEL}:generateContent?key={key}"
    )
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 1400},
    }

    r = requests.post(url, json=payload, timeout=GEMINI_TIMEOUT)
    r.raise_for_status()

    data = r.json()
    try:
        text = data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as e:
        raise RuntimeError(f"Unexpected Gemini response: {data}") from e

    return _extract_json(text)


# ----------------------------
# Validation
# ----------------------------
def validate_llm_obj(obj: Dict[str, Any], run_date: str) -> Dict[str, Any]:
    must = ["verdict", "narrative", "top_issues", "drilldown_sql_1", "drilldown_sql_2", "drilldown_sql_3"]
    for k in must:
        if k not in obj:
            raise ValueError(f"LLM JSON missing key: {k}")

    verdict = str(obj["verdict"]).upper().strip()
    if verdict not in ("PASS", "WARN", "FAIL"):
        obj["verdict"] = "WARN"
    else:
        obj["verdict"] = verdict

    narrative = str(obj["narrative"]).strip()
    if len(narrative) > 2000:
        narrative = narrative[:2000]
    obj["narrative"] = narrative

    if not isinstance(obj["top_issues"], list):
        obj["top_issues"] = [{"issue": "unknown", "cnt": None, "explanation": str(obj["top_issues"])}]
    obj["top_issues"] = obj["top_issues"][:5]

    for k in ("drilldown_sql_1", "drilldown_sql_2", "drilldown_sql_3"):
        sql = str(obj[k]).strip().rstrip().rstrip(";")
        low = f" {sql.lower()} "
        if not (low.strip().startswith("select") or low.strip().startswith("with")):
            raise ValueError(f"{k} must be SELECT/WITH only")
        if any(bad in low for bad in FORBIDDEN_SQL):
            raise ValueError(f"{k} contains forbidden keywords")
        if "etl_date" not in low:
            raise ValueError(f"{k} must include etl_date filter")
        obj[k] = sql

    return obj


# ----------------------------
# Metrics loading (từ dbt tables)
# ----------------------------
def load_metrics(run_date: str) -> Dict[str, Any]:
    gold = trino_conn(SCHEMA_GOLD)
    silver = trino_conn(SCHEMA_SILVER)

    # 7-day summary from gold.dq_compare_daily_summary (detail)
    summary_sql = f"""
    SELECT etl_date, object_name, dataset, dq_class, row_cnt
    FROM {SCHEMA_GOLD}.dq_compare_daily_summary
    WHERE etl_date BETWEEN date '{run_date}' - interval '6' day AND date '{run_date}'
      AND object_name = 'detail'
    """
    summary_7d = fetch_all(gold, summary_sql)

    def get_cnt(dataset: str, dq_class: str) -> int:
        for r in summary_7d:
            if str(r["etl_date"]) == run_date and r["dataset"] == dataset and r["dq_class"] == dq_class:
                return int(r["row_cnt"])
        return 0

    raw_fail = get_cnt("raw", "FAIL")
    raw_warn = get_cnt("raw", "WARN")
    raw_pass = get_cnt("raw", "PASS")
    raw_cnt = raw_fail + raw_warn + raw_pass

    clean_fail = get_cnt("clean", "FAIL")
    clean_warn = get_cnt("clean", "WARN")
    clean_pass = get_cnt("clean", "PASS")
    clean_cnt = clean_fail + clean_warn + clean_pass

    delta = raw_cnt - clean_cnt

    # Top issues today (RAW flags)
    top_issue_sql = f"""
    SELECT * FROM (
      SELECT 'null_key' as issue, sum(f_null_key) as cnt FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = date '{run_date}'
      UNION ALL SELECT 'dup_key', sum(f_dup_key) FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = date '{run_date}'
      UNION ALL SELECT 'qty_invalid', sum(f_qty_invalid) FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = date '{run_date}'
      UNION ALL SELECT 'price_invalid', sum(f_price_invalid) FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = date '{run_date}'
      UNION ALL SELECT 'discount_invalid', sum(f_discount_invalid) FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = date '{run_date}'
      UNION ALL SELECT 'linetotal_invalid', sum(f_linetotal_invalid) FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = date '{run_date}'
      UNION ALL SELECT 'linetotal_mismatch', sum(f_linetotal_mismatch) FROM {SCHEMA_SILVER}.dq_sales_order_detail_raw WHERE etl_date = date '{run_date}'
    )
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
    # Rule-guided: LLM "phân lớp" theo rule rõ ràng + viết giải thích.
    return f"""
Bạn là hệ thống phân loại chất lượng dữ liệu (data quality classifier).
Bạn PHẢI phân loại theo rule dưới đây, không được bịa số, không được suy diễn ngoài dữ liệu.

INPUT (ngày {run_date}):
RAW: total={m["raw_cnt"]}, PASS={m["raw_pass"]}, WARN={m["raw_warn"]}, FAIL={m["raw_fail"]}
CLEAN: total={m["clean_cnt"]}, PASS={m["clean_pass"]}, WARN={m["clean_warn"]}, FAIL={m["clean_fail"]}
delta_raw_clean = {m["delta_raw_clean"]}

Top issues (RAW flags):
{json.dumps(m["top_issues_today"], ensure_ascii=False)}

Rule phân loại (bắt buộc):
- verdict = FAIL nếu (RAW_FAIL > 0) OR (delta_raw_clean > 0)
- verdict = WARN nếu không FAIL, nhưng (RAW_WARN > 0) OR (bất kỳ issue cnt > 0)
- verdict = PASS nếu RAW_FAIL=0, RAW_WARN=0, delta_raw_clean=0 và tất cả issue cnt = 0

YÊU CẦU OUTPUT:
Chỉ trả JSON HỢP LỆ (không markdown, không ```), đúng keys:
- verdict: PASS/WARN/FAIL
- narrative: tiếng Việt, tối đa 8 dòng, giải thích dựa trên số liệu input (nhắc đúng các con số)
- top_issues: tối đa 5 phần tử {{issue, cnt, explanation}} (explanation ngắn)
- drilldown_sql_1/2/3: SQL Trino chỉ SELECT hoặc WITH+SELECT; có filter etl_date = date '{run_date}'; không có dấu ';'

Không được bịa thêm metric nào ngoài INPUT.
""".strip()


# ----------------------------
# Write output table
# ----------------------------
def upsert_report(run_date: str, metrics: Dict[str, Any], llm_obj: Dict[str, Any]) -> None:
    gold = trino_conn(SCHEMA_GOLD)
    cur = gold.cursor()

    # 1 record/day/object_name=detail
    cur.execute(
        f"DELETE FROM {SCHEMA_GOLD}.dq_agent_report_daily "
        f"WHERE etl_date = date '{run_date}' AND object_name = 'detail'"
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
      date '{run_date}', 'detail', 7,
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
    print(f"[dq_llm_classifier] run_date={run_date} conn_id={TRINO_CONN_ID} model={GEMINI_MODEL}")

    metrics = load_metrics(run_date)
    prompt = build_prompt(run_date, metrics)

    llm_obj = call_gemini(prompt)
    llm_obj = validate_llm_obj(llm_obj, run_date)

    upsert_report(run_date, metrics, llm_obj)
    print("[dq_llm_classifier] inserted report into gold.dq_agent_report_daily")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run_date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    main(args.run_date)
