import os
import pandas as pd
import streamlit as st
import psycopg2
from dotenv import load_dotenv

load_dotenv()

_SHARED_USER = os.getenv("DB_USER", "testdb_user")
_SHARED_PASSWORD = os.getenv("DB_PASSWORD", "testdb123")

DB_CONFIG_DIGG = {
    "host": os.getenv("DB_HOST_DIGG", "localhost"),
    "port": os.getenv("DB_PORT_DIGG", "5432"),
    "dbname": os.getenv("DB_NAME_DIGG", "diggpgsq1_db"),
    "user": _SHARED_USER,
    "password": _SHARED_PASSWORD,
}

DB_CONFIG_MBR = {
    "host": os.getenv("DB_HOST_MBR", "localhost"),
    "port": os.getenv("DB_PORT_MBR", "5432"),
    "dbname": os.getenv("DB_NAME_MBR", "mbrgpgsq1_clinical_data_db"),
    "user": _SHARED_USER,
    "password": _SHARED_PASSWORD,
}

MEMBER_DATA_TABLES = [
    {"schema": "incentives", "table": "incentives_transaction_summary", "id_column": "primary_member_plan_id", "operation": "DELETE"},
    {"schema": "incentives", "table": "incentives_transaction", "id_column": "primary_member_plan_id", "operation": "DELETE"},
    {"schema": "digital_journey", "table": "member_content_transaction", "id_column": "member_plan_id", "operation": "DELETE"},
    {"schema": "digital_journey", "table": "hra_member_category_status", "id_column": "member_plan_id", "operation": "UPDATE", "update_set": "status_id = 3"},
]

HRA_DATA_TABLES = [
    {"schema": "member_clinical_data", "table": "member_assessment_detail_hist", "id_column": "primary_member_plan_id", "operation": "DELETE"},
    {"schema": "member_clinical_data", "table": "member_assessment_detail", "id_column": "primary_member_plan_id", "operation": "DELETE"},
    {"schema": "member_clinical_data", "table": "member_assessment_header_hist", "id_column": "primary_member_plan_id", "operation": "DELETE"},
    {"schema": "member_clinical_data", "table": "member_assessment_header", "id_column": "primary_member_plan_id", "operation": "DELETE"},
]

for key in ("digg_preview", "mbr_preview", "digg_preview_err", "mbr_preview_err",
             "digg_status", "mbr_status"):
    if key not in st.session_state:
        st.session_state[key] = None


def get_connection(db_config):
    return psycopg2.connect(**db_config)


def _check_connection(db_config):
    """Return (True, None) on success or (False, error_message) on failure."""
    try:
        get_connection(db_config).close()
        return True, None
    except Exception as e:
        return False, str(e)


if st.session_state.digg_status is None:
    ok, err = _check_connection(DB_CONFIG_DIGG)
    st.session_state.digg_status = {"ok": ok, "err": err}
if st.session_state.mbr_status is None:
    ok, err = _check_connection(DB_CONFIG_MBR)
    st.session_state.mbr_status = {"ok": ok, "err": err}


def run_select(cursor, schema, table, id_column, member_id):
    fqn = f"{schema}.{table}"
    cursor.execute(f"SELECT COUNT(*) FROM {fqn} WHERE {id_column} = %s", (member_id,))
    return cursor.fetchone()[0]


def run_operation(cursor, entry, member_id):
    fqn = f'{entry["schema"]}.{entry["table"]}'
    id_col = entry["id_column"]
    if entry["operation"] == "DELETE":
        cursor.execute(f"DELETE FROM {fqn} WHERE {id_col} = %s", (member_id,))
    elif entry["operation"] == "UPDATE":
        cursor.execute(f"UPDATE {fqn} SET {entry['update_set']} WHERE {id_col} = %s", (member_id,))
    return cursor.rowcount


def render_html_table(df):
    st.markdown(df.to_html(index=False, classes="aws-tbl", border=0), unsafe_allow_html=True)


def execute_script(tables, member_id, script_label, status_container, db_config):
    results = []
    try:
        conn = get_connection(db_config)
        conn.autocommit = False
        cur = conn.cursor()
        for entry in tables:
            fqn = f'{entry["schema"]}.{entry["table"]}'
            op = entry["operation"]
            pre = run_select(cur, entry["schema"], entry["table"], entry["id_column"], member_id)
            affected = run_operation(cur, entry, member_id)
            post = run_select(cur, entry["schema"], entry["table"], entry["id_column"], member_id)
            results.append({"Table": fqn, "Op": op, "Before": pre, "Affected": affected, "After": post, "Status": "OK"})
        conn.commit()
        cur.close()
        conn.close()
        with status_container:
            st.markdown('<div class="flash flash-ok">Completed successfully.</div>', unsafe_allow_html=True)
            render_html_table(pd.DataFrame(results))
        return True, results
    except Exception as exc:
        try:
            conn.rollback()
            conn.close()
        except Exception:
            pass
        with status_container:
            st.markdown(f'<div class="flash flash-err">Failed: {exc}</div>', unsafe_allow_html=True)
            if results:
                render_html_table(pd.DataFrame(results))
        return False, str(exc)


def fetch_preview(tables, member_id, db_config):
    results = []
    conn = get_connection(db_config)
    cur = conn.cursor()
    for entry in tables:
        fqn = f'{entry["schema"]}.{entry["table"]}'
        count = run_select(cur, entry["schema"], entry["table"], entry["id_column"], member_id)
        results.append({"Table": fqn, "Records": count})
    cur.close()
    conn.close()
    return results


def _sidebar_db_card(label, cfg, color, status):
    if status and status["ok"]:
        dot = '<span class="status-dot dot-ok"></span>'
        status_text = '<span class="status-text text-ok">Connected</span>'
        err_row = ""
    elif status and not status["ok"]:
        dot = '<span class="status-dot dot-err"></span>'
        status_text = '<span class="status-text text-err">Disconnected</span>'
        short_err = (status["err"] or "Unknown error")[:80]
        err_row = f'<div class="db-err">{short_err}</div>'
    else:
        dot = '<span class="status-dot dot-unk"></span>'
        status_text = '<span class="status-text text-unk">Checking...</span>'
        err_row = ""
    return (
        f'<div class="db-card">'
        f'<div class="db-label" style="color:{color}">{dot}{label} {status_text}</div>'
        f'<table class="db-meta">'
        f'<tr><td>Host</td><td>{cfg["host"]}</td></tr>'
        f'<tr><td>Port</td><td>{cfg["port"]}</td></tr>'
        f'<tr><td>DB</td><td>{cfg["dbname"]}</td></tr>'
        f'<tr><td>User</td><td>{cfg["user"]}</td></tr>'
        f'</table>{err_row}</div>'
    )


# ── Page config & CSS ─────────────────────────────────────────────────────────

st.set_page_config(page_title="Automated Test Data Set Up Tool", page_icon="🗄️", layout="wide")

st.markdown("""
<style>
/* ── Hide native Streamlit chrome ──────────────────────── */
#MainMenu, footer { display: none !important; }
header[data-testid="stHeader"] { height: 0 !important; min-height: 0 !important; padding: 0 !important; }

/* ── Page layout ───────────────────────────────────────── */
.block-container { padding: 0 1.8rem 0.6rem !important; }
div[data-testid="stHorizontalBlock"] > div { padding: 0 0.35rem; }

/* ── App header bar ────────────────────────────────────── */
.app-header {
    background: #232F3E; color: #fff; padding: 0.5rem 1.2rem;
    margin: 0 -1.8rem 0.7rem -1.8rem;
    display: flex; align-items: center; gap: 0.7rem;
}
.app-header-title {
    font-size: 1rem; font-weight: 700; letter-spacing: 0.02em;
    font-family: "Amazon Ember", -apple-system, BlinkMacSystemFont, sans-serif;
}
.app-header-badge {
    font-size: 0.58rem; font-weight: 600; background: #FF9900;
    color: #16191f; padding: 0.12rem 0.45rem; border-radius: 3px;
    text-transform: uppercase; letter-spacing: 0.04em;
}
.app-header-sub {
    font-size: 0.68rem; color: #aab7c4; margin-left: auto;
}

/* ── Sidebar ───────────────────────────────────────────── */
section[data-testid="stSidebar"] {
    background: #232F3E; width: 240px !important; min-width: 240px !important;
}
section[data-testid="stSidebar"] [data-testid="stSidebarContent"] {
    padding: 0.6rem 0.75rem !important;
}
section[data-testid="stSidebar"] * { color: #d5dbdb !important; }
section[data-testid="stSidebar"] button {
    font-size: 0.68rem !important; padding: 0.18rem 0.55rem !important;
    background: #37475A !important; border: 1px solid #56687a !important;
    color: #FF9900 !important; border-radius: 3px; margin-top: 0.15rem;
}
section[data-testid="stSidebar"] button:hover { background: #485769 !important; }
.sidebar-title {
    font-size: 0.72rem; font-weight: 700; color: #8c9bad !important;
    text-transform: uppercase; letter-spacing: 0.08em;
    margin: 0 0 0.4rem 0; padding-bottom: 0.25rem;
    border-bottom: 1px solid #3d4f63;
}
.db-card {
    background: #2e3b4e; border-radius: 4px; padding: 0.45rem 0.6rem;
    margin-bottom: 0.4rem;
}
.db-label {
    font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.05em; margin-bottom: 0.2rem;
}
.db-meta { width: 100%; border-collapse: collapse; }
.db-meta td {
    font-size: 0.67rem; padding: 0.08rem 0; color: #aab7c4 !important;
    font-family: "SF Mono", "Menlo", monospace; border: none; line-height: 1.35;
}
.db-meta td:first-child {
    width: 32px; color: #6c7a8a !important; font-weight: 600; padding-right: 0.4rem;
}
.sidebar-divider { border: none; border-top: 1px solid #3d4f63; margin: 0.35rem 0; }

/* ── Status indicator ──────────────────────────────────── */
.status-dot {
    display: inline-block; width: 7px; height: 7px; border-radius: 50%;
    margin-right: 5px; vertical-align: middle; position: relative; top: -1px;
}
.dot-ok  { background: #1d8102; box-shadow: 0 0 4px #1d8102; }
.dot-err { background: #d13212; box-shadow: 0 0 4px #d13212; }
.dot-unk { background: #8c9bad; }
.status-text {
    font-size: 0.6rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.04em; vertical-align: middle; margin-left: 0.15rem;
}
.text-ok  { color: #1d8102 !important; }
.text-err { color: #d13212 !important; }
.text-unk { color: #8c9bad !important; }
.db-err {
    font-size: 0.6rem; color: #e07060 !important; margin-top: 0.25rem;
    padding: 0.2rem 0.35rem; background: rgba(209,50,18,0.1);
    border-radius: 2px; line-height: 1.3; word-break: break-word; white-space: normal;
}

/* ── Tabs ──────────────────────────────────────────────── */
button[data-baseweb="tab"] {
    font-size: 0.78rem; font-weight: 600; padding: 0.4rem 1.3rem;
    color: #545b64; background: transparent; border: none;
    border-bottom: 3px solid transparent; text-transform: uppercase; letter-spacing: 0.04em;
}
button[data-baseweb="tab"]:hover { color: #232F3E; border-bottom-color: #d5dbdb; }
button[data-baseweb="tab"][aria-selected="true"] {
    color: #232F3E; border-bottom-color: #FF9900 !important;
}
div[data-baseweb="tab-list"] { gap: 0; border-bottom: 2px solid #e1e4e8; }
div[data-baseweb="tab-panel"] { padding-top: 0.6rem; }

/* ── Buttons ───────────────────────────────────────────── */
.stButton > button {
    font-size: 0.76rem; font-weight: 600; padding: 0.3rem 0.9rem;
    border-radius: 3px; border: 1px solid #aab7b8;
    background: linear-gradient(to bottom, #fafafa, #e7e9eb); color: #16191f;
}
.stButton > button:hover { background: linear-gradient(to bottom, #e7e9eb, #d5dbdb); }
.stButton > button[kind="primary"],
.stButton > button[data-testid="stBaseButton-primary"] {
    background: linear-gradient(to bottom, #f7ca64, #f0ad00) !important;
    border-color: #c08a00 !important; color: #16191f !important;
}
.stButton > button[kind="primary"]:hover,
.stButton > button[data-testid="stBaseButton-primary"]:hover {
    background: linear-gradient(to bottom, #f0ad00, #e09900) !important;
}

/* ── Data tables ───────────────────────────────────────── */
.aws-tbl { width: 100%; border-collapse: collapse; font-size: 0.76rem; }
.aws-tbl th {
    background: #fafafa; color: #545b64; font-weight: 700;
    text-transform: uppercase; font-size: 0.68rem; letter-spacing: 0.03em;
    padding: 0.3rem 0.5rem; border-bottom: 2px solid #d5dbdb;
    text-align: left; white-space: nowrap;
}
.aws-tbl td {
    padding: 0.25rem 0.5rem; border-bottom: 1px solid #eaeded;
    color: #16191f; white-space: nowrap;
}
.aws-tbl tr:hover td { background: #f1f3f3; }

/* ── Flash messages ────────────────────────────────────── */
.flash { font-size: 0.76rem; padding: 0.3rem 0.6rem; border-radius: 3px; margin-bottom: 0.35rem; border-left: 4px solid; }
.flash-ok  { background: #f2f8f0; border-color: #1d8102; color: #1d8102; }
.flash-err { background: #fdf3f1; border-color: #d13212; color: #d13212; }

/* ── Section card ──────────────────────────────────────── */
.section-card {
    border: 1px solid #eaeded; border-radius: 4px;
    padding: 0.5rem 0.75rem; margin-bottom: 0.5rem; background: #fff;
}
.section-card h4 { margin: 0 0 0.15rem 0; font-size: 0.8rem; color: #232F3E; }
.section-card p  { margin: 0; font-size: 0.72rem; color: #545b64; }

/* ── Panel header (column label) ───────────────────────── */
.panel-hdr {
    font-size: 0.73rem; font-weight: 700; color: #232F3E;
    text-transform: uppercase; letter-spacing: 0.03em;
    margin-bottom: 0.3rem; padding-bottom: 0.15rem;
    border-bottom: 2px solid #FF9900; display: inline-block;
}

/* ── Inputs ────────────────────────────────────────────── */
.stTextInput input { font-size: 0.8rem; padding: 0.3rem 0.45rem; border-radius: 3px; }
.stTextInput label { font-size: 0.76rem !important; font-weight: 600; }

/* ── Alerts compact ────────────────────────────────────── */
div[data-testid="stAlert"] { padding: 0.3rem 0.6rem !important; font-size: 0.74rem; }
</style>
""", unsafe_allow_html=True)

# ── App header ────────────────────────────────────────────────────────────────
st.markdown(
    '<div class="app-header">'
    '<div class="app-header-title">Automated Test Data Set Up Tool</div>'
    '<span class="app-header-badge">Internal</span>'
    '<div class="app-header-sub">PostgreSQL Admin Console</div>'
    '</div>',
    unsafe_allow_html=True,
)

col_input, _ = st.columns([2, 4])
with col_input:
    member_id_str = st.text_input("Member Plan ID", placeholder="e.g. 2459267294")

if member_id_str:
    try:
        member_id = int(member_id_str)
    except ValueError:
        st.error("Must be numeric.")
        st.stop()
else:
    member_id = None

tab1, tab2, tab3 = st.tabs(["PREVIEW", "RESET MEMBER DATA", "RESET HRA / CLINICAL"])

# ── Tab 1: Preview ────────────────────────────────────────────────────────────
with tab1:
    st.markdown(
        '<div class="section-card">'
        "<h4>Record Count Preview</h4>"
        "<p>SELECT COUNT on every target table for the given member.</p>"
        "</div>",
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div class="panel-hdr">DIGG &mdash; Member Data</div>', unsafe_allow_html=True)
        btn_sel_digg = st.button("Query DIGG", key="sel_member", disabled=member_id is None)
    with c2:
        st.markdown('<div class="panel-hdr">MBR &mdash; HRA / Clinical</div>', unsafe_allow_html=True)
        btn_sel_mbr = st.button("Query MBR", key="sel_hra", disabled=member_id is None)

    if btn_sel_digg and member_id:
        try:
            st.session_state.digg_preview = fetch_preview(MEMBER_DATA_TABLES, member_id, DB_CONFIG_DIGG)
            st.session_state.digg_preview_err = None
        except Exception as e:
            st.session_state.digg_preview = None
            st.session_state.digg_preview_err = str(e)

    if btn_sel_mbr and member_id:
        try:
            st.session_state.mbr_preview = fetch_preview(HRA_DATA_TABLES, member_id, DB_CONFIG_MBR)
            st.session_state.mbr_preview_err = None
        except Exception as e:
            st.session_state.mbr_preview = None
            st.session_state.mbr_preview_err = str(e)

    r1, r2 = st.columns(2)
    with r1:
        if st.session_state.digg_preview_err:
            st.markdown(f'<div class="flash flash-err">{st.session_state.digg_preview_err}</div>', unsafe_allow_html=True)
        elif st.session_state.digg_preview:
            render_html_table(pd.DataFrame(st.session_state.digg_preview))
    with r2:
        if st.session_state.mbr_preview_err:
            st.markdown(f'<div class="flash flash-err">{st.session_state.mbr_preview_err}</div>', unsafe_allow_html=True)
        elif st.session_state.mbr_preview:
            render_html_table(pd.DataFrame(st.session_state.mbr_preview))

# ── Tab 2: Reset Member Data ─────────────────────────────────────────────────
with tab2:
    st.markdown(
        '<div class="section-card">'
        "<h4>Reset Member Data</h4>"
        "<p>DELETE from <b>incentives</b> &amp; <b>digital_journey</b>; UPDATE hra_member_category_status to 3.</p>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.warning("This permanently modifies data. Verify the Member Plan ID.", icon="⚠️")
    btn_reset_member = st.button("Execute Reset", key="reset_member", type="primary", disabled=member_id is None)
    reset_member_box = st.container()
    if btn_reset_member and member_id:
        with st.spinner("Resetting..."):
            execute_script(MEMBER_DATA_TABLES, member_id, "Reset Member Data", reset_member_box, DB_CONFIG_DIGG)

# ── Tab 3: Reset HRA / Clinical ──────────────────────────────────────────────
with tab3:
    st.markdown(
        '<div class="section-card">'
        "<h4>Reset HRA / Clinical Data</h4>"
        "<p>DELETE from <b>member_clinical_data</b> — assessment headers, details &amp; history.</p>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.warning("This permanently modifies data. Verify the Member Plan ID.", icon="⚠️")
    btn_reset_hra = st.button("Execute Reset", key="reset_hra", type="primary", disabled=member_id is None)
    reset_hra_box = st.container()
    if btn_reset_hra and member_id:
        with st.spinner("Resetting..."):
            execute_script(HRA_DATA_TABLES, member_id, "Reset HRA Data", reset_hra_box, DB_CONFIG_MBR)

# ── Sidebar ───────────────────────────────────────────────────────────────────

if st.session_state.get("_toast"):
    msg, icon = st.session_state.pop("_toast")
    st.toast(msg, icon=icon)

with st.sidebar:
    st.markdown('<div class="sidebar-title">Database Connections</div>', unsafe_allow_html=True)

    st.markdown(_sidebar_db_card("DIGG DB", DB_CONFIG_DIGG, "#FF9900", st.session_state.digg_status), unsafe_allow_html=True)
    if st.button("Check Connection", key="test_digg"):
        ok, err = _check_connection(DB_CONFIG_DIGG)
        st.session_state.digg_status = {"ok": ok, "err": err}
        if ok:
            st.session_state["_toast"] = ("DIGG DB — Connection successful", "✅")
        else:
            st.session_state["_toast"] = (f"DIGG DB — {err[:60]}", "🚨")
        st.rerun()

    st.markdown('<hr class="sidebar-divider">', unsafe_allow_html=True)

    st.markdown(_sidebar_db_card("MBR DB", DB_CONFIG_MBR, "#48b9c7", st.session_state.mbr_status), unsafe_allow_html=True)
    if st.button("Check Connection", key="test_mbr"):
        ok, err = _check_connection(DB_CONFIG_MBR)
        st.session_state.mbr_status = {"ok": ok, "err": err}
        if ok:
            st.session_state["_toast"] = ("MBR DB — Connection successful", "✅")
        else:
            st.session_state["_toast"] = (f"MBR DB — {err[:60]}", "🚨")
        st.rerun()
