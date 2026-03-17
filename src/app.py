import streamlit as st
import os
import sys
import re
import pandas as pd
import sqlite3
import streamlit.components.v1 as components
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from src.agent.llm_agent import run_mock_agent, run_real_agent
from src.agent.agent_tools_ext import generate_e2e_lineage_graph

TARGET_DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')
EMAIL_LOG_PATH = os.path.join(BASE_DIR, 'logs', 'email_audit.log')
os.makedirs(os.path.dirname(EMAIL_LOG_PATH), exist_ok=True)

st.set_page_config(page_title="Enterprise Data Lineage & Impact Agent", layout="wide", page_icon="🔗")

# ========== PREMIUM CSS ==========
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Outfit:wght@600;700;800&display=swap');

:root {
    --primary: #2563EB;
    --surface: #FFFFFF;
    --bg: #F1F5F9;
    --border: #E2E8F0;
    --text: #0F172A;
    --muted: #64748B;
}

.stApp { background: var(--bg); color: var(--text); font-family: 'Inter', sans-serif; }

.header-bar {
    background: linear-gradient(135deg, #1E3A8A 0%, #2563EB 55%, #7C3AED 100%);
    border-radius: 16px; padding: 28px 36px; margin-bottom: 24px;
    position: relative; overflow: hidden;
    box-shadow: 0 20px 40px rgba(37,99,235,0.22);
}
.header-bar::after {
    content:''; position:absolute; top:-40%; right:-8%; width:300px; height:300px;
    background:rgba(255,255,255,0.06); border-radius:50%;
}
.header-title { font-family:'Outfit',sans-serif; font-size:2rem; font-weight:800; color:#fff; margin:0 0 6px; }
.header-sub   { color:rgba(255,255,255,0.72); font-size:0.95rem; margin:0; }

.stat-card {
    background:#fff; border-radius:14px; padding:20px 22px;
    display:flex; flex-direction:column; gap:6px;
    box-shadow:0 1px 3px rgba(0,0,0,0.06),0 6px 16px rgba(0,0,0,0.04);
    border:1px solid var(--border);
    transition:transform 0.15s ease,box-shadow 0.15s ease;
}
.stat-card:hover { transform:translateY(-2px); box-shadow:0 8px 24px rgba(0,0,0,0.09); }
.stat-icon  { font-size:1.5rem; }
.stat-value { font-size:2.1rem; font-weight:800; font-family:'Outfit',sans-serif; }
.stat-label { font-size:0.75rem; font-weight:700; color:var(--muted); letter-spacing:0.4px; text-transform:uppercase; }
.stat-names { font-size:0.74rem; color:var(--muted); line-height:1.4; }

.risk-high   { background:#FEE2E2; border-left:5px solid #DC2626; color:#7F1D1D; padding:12px 14px; border-radius:10px; margin:5px 0; }
.risk-medium { background:#FEF3C7; border-left:5px solid #D97706; color:#78350F; padding:12px 14px; border-radius:10px; margin:5px 0; }
.risk-low    { background:#DCFCE7; border-left:5px solid #059669; color:#064E3B; padding:12px 14px; border-radius:10px; margin:5px 0; }

.badge-fail { background:#FEE2E2; color:#DC2626; padding:3px 10px; border-radius:20px; font-size:0.78rem; font-weight:700; }
.badge-ok   { background:#DCFCE7; color:#059669; padding:3px 10px; border-radius:20px; font-size:0.78rem; font-weight:700; }

[data-testid="column"]:nth-of-type(2) {
    position:fixed; bottom:20px; right:20px;
    width:440px !important; height:82vh;
    background:#fff; box-shadow:0 20px 60px rgba(0,0,0,0.18),0 0 0 1px rgba(0,0,0,0.06);
    border-radius:20px; padding:20px; border:1px solid var(--border);
    overflow-y:auto; z-index:9999; display:flex; flex-direction:column;
}
[data-testid="column"]:nth-of-type(2) .stChatInputContainer {
    position:sticky; bottom:0; background:#fff;
    padding-top:10px; z-index:10000; border-top:1px solid var(--border);
}

div[data-testid="stChatMessage"] {
    background:#F8FAFC; border:1px solid var(--border);
    border-radius:12px; padding:14px 18px; margin-bottom:8px;
}

.stTabs [data-baseweb="tab-list"] { gap:6px; background:transparent; }
.stTabs [data-baseweb="tab"] {
    height:38px; background:#fff; border-radius:8px;
    color:var(--muted); font-weight:600; font-size:0.84rem;
    border:1px solid var(--border); padding:4px 14px; transition:all 0.18s ease;
}
.stTabs [aria-selected="true"] {
    background:var(--primary) !important; color:#fff !important;
    border-color:var(--primary) !important; box-shadow:0 4px 12px rgba(37,99,235,0.3);
}
h3 { font-family:'Outfit',sans-serif !important; color:var(--text) !important; }
.stDataFrame { border-radius:10px; overflow:hidden; }
</style>
""", unsafe_allow_html=True)

# ========== DB HELPER ==========
def get_db_conn():
    return sqlite3.connect(TARGET_DB_PATH)

# ========== STATIC INVENTORY DATA ==========
STATIC_SOURCES = ["Customers","Products","Stores","Employees","Suppliers","Promotions",
                   "Orders","OrderLines","Returns","Invoices","PurchaseOrders","Vendors",
                   "HR_Records","ServiceTickets","Accounts"]
STATIC_CSVS  = ["Sales_Export.csv","Inventory_Snapshot.csv","Returns_Flat.csv","Promo_Export.csv","Budget_Forecast.csv"]
STATIC_APIS  = ["marketingcloud.io/campaigns","salesforce.com/leads","payments.stripe.com/transactions",
                 "shopify.com/orders","currencies.exchange.io/rates"]
STATIC_BIS   = ["Sales Performance Dashboard","Customer Segmentation Report","Margin Analysis Report",
                 "Campaign ROI Report","Monthly Revenue Trend","Product Performance Report",
                 "Tax Reconciliation Report","Store Performance Dashboard","Inventory Health Report",
                 "Executive KPI Scorecard","Supplier Quality Report"]

@st.cache_data(ttl=120)
def get_inventory_stats():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT source_table FROM data_lineage_map WHERE source_system LIKE 'SQLite%' OR source_system LIKE 'OLTP%' OR source_system LIKE 'ERP%'")
    src_db = [r[0] for r in cur.fetchall()]
    src = src_db if len(src_db) >= 5 else STATIC_SOURCES
    cur.execute("SELECT DISTINCT target_table FROM data_lineage_map")
    tgt = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT source_table FROM data_lineage_map WHERE source_system LIKE 'CSV%'")
    csv_db = [r[0] for r in cur.fetchall()]
    csv_f = [f"{t}.csv" for t in csv_db] if csv_db else STATIC_CSVS
    cur.execute("SELECT DISTINCT etl_pipeline FROM table_catalog WHERE etl_pipeline IS NOT NULL")
    etls = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT report_name FROM report_dependency")
    bi_db = [r[0] for r in cur.fetchall()]
    bi = bi_db if bi_db else STATIC_BIS
    conn.close()
    return src, tgt, csv_f, STATIC_APIS, etls, bi

# ========== AUTO IMPACT SUMMARY ==========
def render_auto_impact_summary(table_name: str, detailed: bool = False):
    """Render a risk grid for ALL possible operations on a table — no user selection needed."""
    conn = get_db_conn()
    try:
        df_reports   = pd.read_sql("SELECT DISTINCT report_name, dw_column, business_owner FROM report_dependency WHERE dw_table=?", conn, params=(table_name,))
        df_pipelines = pd.read_sql("SELECT DISTINCT etl_pipeline FROM table_catalog WHERE table_name=?", conn, params=(table_name,))
        df_linked    = pd.read_sql("SELECT DISTINCT linked_tables FROM table_catalog WHERE table_name=?", conn, params=(table_name,))
        cur = conn.cursor()
        cur.execute(f'PRAGMA table_info("{table_name}")')

        columns = [r[1] for r in cur.fetchall()]
    finally:
        conn.close()

    report_count   = len(df_reports)
    pipeline_count = len(df_pipelines)
    col_count      = len(columns)
    linked_tables  = df_linked.iloc[0, 0] if not df_linked.empty and df_linked.iloc[0, 0] else "None"

    def _risk(r, p):
        if r >= 3 or p >= 2: return "HIGH",   "risk-high",   "🔴"
        if r >= 1 or p >= 1: return "MEDIUM", "risk-medium", "🟡"
        return "LOW", "risk-low", "🟢"

    ops = [
        ("Drop Table",            _risk(report_count, pipeline_count),    f"{col_count} cols removed · {report_count} reports break · {pipeline_count} pipelines fail"),
        ("Alter Column Datatype", _risk(report_count, 1),                 f"Casting errors in {pipeline_count} pipeline(s) · incorrect data in {report_count} report(s)"),
        ("Add Column",            _risk(0, 0),                            "Additive — existing reports unaffected · pipelines may need schema refresh"),
        ("Rename Column",         _risk(report_count, pipeline_count),    f"{report_count} report(s) reference column by name · {pipeline_count} pipeline(s) need remapping"),
        ("Revoke Access",         _risk(min(report_count,2), 1),          "Teams with SELECT grants lose access · scheduled jobs may fail silently"),
        ("Nullability Change",    _risk(1 if report_count else 0, 0),     f"NULL constraints affect {report_count} downstream aggregations"),
    ]

    st.markdown(f"**Predictive Impact — All Possible Operations on `{table_name}`**")
    st.caption("Risk is calculated from downstream report dependencies, ETL pipelines, and linked tables.")
    cols_ui = st.columns(3)
    for i, (op_name, (risk, risk_class, emoji), desc) in enumerate(ops):
        with cols_ui[i % 3]:
            st.markdown(f"""
            <div class="{risk_class}" style="margin-bottom:8px;">
              <div style="font-size:0.78rem;font-weight:700;margin-bottom:3px;">{emoji} {op_name}</div>
              <div style="font-size:0.85rem;font-weight:800;">{risk} RISK</div>
              <div style="font-size:0.72rem;opacity:0.85;margin-top:3px;">{desc}</div>
            </div>""", unsafe_allow_html=True)

    if detailed:
        st.markdown("---")
        st.markdown("**Downstream Reports:**")
        if not df_reports.empty:
            st.dataframe(df_reports, use_container_width=True, hide_index=True)
        else:
            st.info("No report dependencies found.")
        st.markdown("**ETL Pipelines at Risk:**")
        if not df_pipelines.empty:
            for _, row in df_pipelines.iterrows():
                st.warning(f"⚙️ {row['etl_pipeline']}")
        st.markdown(f"**Linked Tables:** `{linked_tables}`")
        if columns:
            st.markdown(f"**Columns ({col_count}):** `{'` · `'.join(columns)}`")

# ========== HEADER ==========
st.markdown("""
<div class="header-bar">
  <p class="header-title">🔗 Enterprise Data Lineage & Impact Agent</p>
  <p class="header-sub">Automated transparency across all sources, pipelines, targets, and reporting layers</p>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<style>
[data-testid="column"]:nth-of-type(2) { position:fixed; bottom:20px; right:20px; width:440px !important; height:82vh; }
</style>
""", unsafe_allow_html=True)

left_col, right_col = st.columns([1.35, 0.65])

with left_col:
    # ---- LINEAGE VIEW FLOW ----
    st.markdown("### Lineage View")
    src, tgt, csv_f, apis, etls, bi = get_inventory_stats()

    # Create 4 columns representing Left-to-Right Flow
    col_src, col_etl, col_tgt, col_rep = st.columns(4)

    # Triggering action for click drill-down
    def _drill(entity_name, category_type):
         st.session_state.panel_entity = entity_name
         st.session_state.panel_flags  = {
             "show_etl": True, "show_audit": True, "show_versions": True, "show_reports": True
         }
         st.session_state.impact_detailed = True
         # Force graph update continuously on click
         try:
              from src.agent.agent_tools_ext import generate_e2e_lineage_graph
              focused = generate_e2e_lineage_graph.run(entity_name)
              if focused and ".html" in str(focused):
                  import re
                  m2 = re.search(r'([A-Za-z]:[/\\][\w\\/\-\.\s]+\.html)', str(focused))
                  if m2 and os.path.exists(m2.group(1).strip()):
                      st.session_state.current_graph = m2.group(1).strip()

         except Exception as e:
              st.session_state.drill_error = f"Drill Error: {type(e).__name__} - {str(e)}"


    # 1. SOURCES COLUMN
    with col_src:
        st.markdown("<div style='background:#F8FAFC;padding:12px;border-radius:12px;border:1px solid #E2E8F0;height:100%;'>", unsafe_allow_html=True)
        st.markdown("#### Sources")
        with st.expander("Tables (RDBMS)", expanded=True):
            for item in src:
                if st.button(item, key=f"btn_src_{item}", use_container_width=True):
                    _drill(item, "table")
                    st.rerun()
        with st.expander("APIs", expanded=False):
            for item in apis:
                clean_api = item.split("/")[-1]
                if st.button(clean_api, key=f"btn_api_{item}", use_container_width=True):
                     _drill(item, "api")
                     st.rerun()
        with st.expander("Files (CSV)", expanded=False):
            for item in csv_f:
                 if st.button(item, key=f"btn_csv_{item}", use_container_width=True):
                     _drill(item, "file")
                     st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # 2. ETL PIPELINES COLUMN
    with col_etl:
        st.markdown("<div style='background:#FEF3C7;padding:12px;border-radius:12px;border:1px solid #FDE68A;height:100%;'>", unsafe_allow_html=True)
        st.markdown("#### Pipelines")
        for item in etls:
             clean_pl = item.replace('oltp_to_dw_', '').replace('api_to_dw_', '').replace('flatfile_to_dw_', '')
             if st.button(clean_pl.upper(), key=f"btn_etl_{item}", use_container_width=True):
                 _drill(item, "pipeline")
                 st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # 3. TARGET TABLES COLUMN
    with col_tgt:
        st.markdown("<div style='background:#DCFCE7;padding:12px;border-radius:12px;border:1px solid #A7F3D0;height:100%;'>", unsafe_allow_html=True)
        st.markdown("#### Targets (DW)")
        for item in tgt:
             if st.button(item, key=f"btn_tgt_{item}", use_container_width=True):
                 _drill(item, "table")
                 st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    # 4. BI REPORTS COLUMN
    with col_rep:
        st.markdown("<div style='background:#E0E7FF;padding:12px;border-radius:12px;border:1px solid #C7D2FE;height:100%;'>", unsafe_allow_html=True)
        st.markdown("#### BI Reports")
        for item in bi:
             clean_bi = item.split("Dashboard")[0].split("Report")[0].strip()
             if st.button(clean_bi, key=f"btn_bi_{item}", use_container_width=True):
                 _drill(item, "report")
                 st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")

    # ---- DRILL-DOWN INTELLIGENT PANEL ----
    if st.session_state.get("panel_entity"):
        entity      = st.session_state.panel_entity
        panel_flags = st.session_state.get("panel_flags", {"show_etl": True})
        detailed    = st.session_state.get("impact_detailed", True)

        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;">
          <span style="background:#DBEAFE;color:#1E40AF;padding:4px 16px;border-radius:20px;font-size:0.85rem;font-weight:700;">{entity}</span>
          <span style="color:#64748B;font-size:0.82rem;">Ecosystem Context Walkthrough</span>
        </div>""", unsafe_allow_html=True)

        if "drill_error" in st.session_state and st.session_state.drill_error:
             st.error(st.session_state.drill_error)
             # Clear it after rendering so it doesn't linger
             st.session_state.drill_error = None


        # Tabs layout
        tab_labels = []
        if st.session_state.get("current_graph"):  tab_labels.append("Lineage Graph")
        tab_labels.extend(["ETL Logs", "DB Audit", "BI Reports"]) # Removed duplicate Impact analysis
        
        tabs    = st.tabs(tab_labels)
        tab_idx = 0
        conn    = get_db_conn()



        # TAB: Lineage Graph (entity-specific, focused)
        if "Lineage Graph" in tab_labels:
            with tabs[tab_idx]:
                tab_idx += 1
                graph_path = st.session_state.get("current_graph", "")
                if graph_path and os.path.exists(graph_path):
                    st.caption(f"Focused lineage for **{entity}** — sources, ETL pipeline, DW table, and downstream reports.")
                    with open(graph_path, 'r', encoding='utf-8') as fh:
                        components.html(fh.read(), height=500, scrolling=True)
                else:
                    st.info("Lineage graph calculation complete. Review tabs or invoke chatbot actions.")

        # Common supporting query: Get downstream DW/Target tables for the selected entity
        try:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT target_table FROM data_lineage_map WHERE LOWER(source_table) = LOWER(?)", (entity,))
            mappings = [r[0] for r in cur.fetchall() if r[0]]
        except Exception:
            mappings = []
        lookup_names = [entity] + mappings
        placeholders = ", ".join(["?"] * len(lookup_names))

        # TAB: ETL Logs
        if "ETL Logs" in tab_labels:
            with tabs[tab_idx]:
                tab_idx += 1
                try:
                    cur = conn.cursor()
                    cur.execute(f"SELECT etl_pipeline FROM table_catalog WHERE table_name IN ({placeholders})", lookup_names)
                    pipelines = [r[0] for r in cur.fetchall() if r[0]]
                    
                    if pipelines:
                        p_placeholders = ", ".join(["?"] * len(pipelines))
                        df_logs = pd.read_sql(
                            f"SELECT pipeline_name, start_time, end_time, status, records_inserted, error_message, retry_attempts FROM etl_execution_logs WHERE pipeline_name IN ({p_placeholders}) ORDER BY start_time DESC LIMIT 25",
                            conn, params=pipelines)
                    else:
                        df_logs = pd.read_sql(
                            "SELECT pipeline_name, start_time, end_time, status, records_inserted, error_message, retry_attempts FROM etl_execution_logs ORDER BY start_time DESC LIMIT 25",
                            conn)
                    if not df_logs.empty:
                        failed = len(df_logs[df_logs['status'] == 'FAILED'])
                        success = len(df_logs[df_logs['status'] != 'FAILED'])
                        m1, m2, m3 = st.columns(3)
                        m1.metric("Total Runs", len(df_logs))
                        m2.metric("Successful", success)
                        m3.metric("Failed", failed, delta=f"-{failed}" if failed else None, delta_color="inverse")

                        def _b(v): return f'<span class="badge-fail">{v}</span>' if v == 'FAILED' else f'<span class="badge-ok">{v}</span>'
                        df_d = df_logs.copy(); df_d['status'] = df_d['status'].apply(_b)
                        st.markdown(df_d.to_html(escape=False, index=False), unsafe_allow_html=True)
                    else:
                        st.info("No ETL logs found for this entity.")
                except Exception as e:
                    st.warning(f"ETL log query failed: {e}")

        # TAB: DB Audit
        if "DB Audit" in tab_labels:
            with tabs[tab_idx]:
                tab_idx += 1
                try:
                    df_audit = pd.read_sql(
                        f"SELECT event_time, event_type, target_object, changed_by_user, change_description, impact_score FROM db_audit_log WHERE target_object IN ({placeholders}) ORDER BY event_time DESC LIMIT 40",
                        conn, params=lookup_names)
                    if not df_audit.empty:
                        ddl = len(df_audit[df_audit['event_type'] == 'DDL'])
                        dcl = len(df_audit[df_audit['event_type'] == 'DCL'])
                        dml = len(df_audit[df_audit['event_type'] == 'DML'])
                        a1, a2, a3 = st.columns(3)
                        a1.metric("DDL Changes", ddl)
                        a2.metric("Security (DCL)", dcl)
                        a3.metric("Data (DML)", dml)
                        st.dataframe(df_audit, use_container_width=True, hide_index=True, height=320)
                    else:
                        st.info("No audit logs found for this object or mapped links.")
                except Exception: pass

        # TAB: BI Reports
        if "BI Reports" in tab_labels:
            with tabs[tab_idx]:
                 tab_idx += 1
                 try:
                     # Query condition spans both report name AND target tables connected to the entity
                     df_rep = pd.read_sql(
                         f"SELECT DISTINCT report_name, dw_table, business_owner FROM report_dependency WHERE dw_table IN ({placeholders}) OR report_name IN ({placeholders}) ORDER BY report_name",
                         conn, params=lookup_names * 2)
                     if not df_rep.empty:
                         for report in df_rep['report_name'].unique():
                             rows = df_rep[df_rep['report_name'] == report]
                             tables = " · ".join(rows['dw_table'].dropna().unique())
                             st.markdown(f"**{report}** (Tables: {tables})")
                     else:
                         st.info("No downstream BI reports connected to this object.")
                 except Exception: pass

                
        conn.close()




# ========== RIGHT PANEL: STATIC ADVISORY & CHATBOT ==========
with right_col:
    st.markdown("### Impact Intelligence")
    st.caption("Real-Time Advisory & Pre-Flight Risk Intelligence")

    active_ent = st.session_state.get("panel_entity")
    conn = get_db_conn()
    
    if active_ent:
        # Static adhesive advisory widget
        render_auto_impact_summary(active_ent, detailed=False)
        
        # Continuous History / Failure Alert Section
        st.markdown("<div style='margin-top:12px;'>", unsafe_allow_html=True)
        try:
             cur = conn.cursor()
             cur.execute("SELECT start_time, status, error_message FROM etl_execution_logs WHERE pipeline_name LIKE ? OR pipeline_name IN (SELECT etl_pipeline FROM table_catalog WHERE table_name = ?) ORDER BY start_time DESC LIMIT 2", (f"%{active_ent}%", active_ent))
             logs = cur.fetchall()
             if logs:
                 st.markdown("**Continuous Ops Advisor**")
                 for l in logs:
                     icon = "OK" if l[1] == "SUCCESS" else "FAIL"
                     st.markdown(f"""
                     <div style="background:#fff;border-left:4px solid {'#DC2626' if l[1]=='FAILED' else '#059669'};padding:8px 12px;border-radius:6px;font-size:0.79rem;margin-bottom:6px;box-shadow:0 1px 3px rgba(0,0,0,0.05);">
                       <strong>{icon} {l[1]}</strong> | <span style='color:#64748B;'>{l[0][:16]}</span><br>
                       {f"<span style='color:#DC2626;font-size:0.72rem;'>Error: {l[2][:45]}...</span>" if l[1]=='FAILED' else "No critical issue detected."}
                     </div>""", unsafe_allow_html=True)
        except Exception: pass
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("""
        <div style="background:#F0FDF4;border:1px solid #BBF7D0;border-radius:12px;padding:14px;margin-top:10px;box-shadow:0 2px 5px rgba(22,101,52,0.05);">
          <strong>AI Recommendation:</strong><br>
          <span style="font-size:0.82rem;color:#15803d;line-height:1.4;">Historical failures detected on upstream links. Adjusting casting parameters or approving audit reconciliation suggested.</span>
        </div>
        """, unsafe_allow_html=True)

    else:
        st.markdown("""
        <div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:12px;padding:24px;text-align:center;color:#64748B;">
          <div style="font-size:2rem;margin-bottom:8px;">⚖️</div>
          <strong>No entity selected for Analysis</strong><br>
          <span style="font-size:0.79rem;">Click on a Source, Pipeline, or Target node on the left to view real-time impact scoring & alerts.</span>
        </div>
        """, unsafe_allow_html=True)
    conn.close()

    st.markdown("<hr style='margin:18px 0;'>", unsafe_allow_html=True)
    st.markdown("<h4 style='margin-bottom:12px;color:#0F172A;font-family:Outfit,sans-serif;'>💬 AI Assistant Chat Bot</h4>", unsafe_allow_html=True)



    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "assistant", "content": (
            "👋 Hello! I'm your Enterprise Data Governance assistant.\n\n"
            "Ask me about:\n"
            "- **Lineage** of any table, source, file, or report\n"
            "- **ETL health**, failures, and execution history\n"
            "- **Impact** of proposed changes — drop, rename, type change, access revoke\n"
            "- **Audit history** of any data object\n\n"
            "Try: *\"Tell me about Fact_Sales\"* or *\"What happens if I drop Dim_Customer?\"*"
        )}]

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Ask about lineage, ETL, impact, audit logs..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Analyzing ecosystem..."):
                has_api_key = bool(os.getenv("GROQ_API_KEY"))
                response = run_real_agent(prompt) if has_api_key else run_mock_agent(prompt)

                text_response = response
                html_path = None

                if "[Agent Automated Action]:" in response:
                    parts = response.split("[Agent Automated Action]:")
                    text_response = parts[0].strip()
                    html_path = parts[1].strip().split("View it at: ")[-1].strip()
                if not (html_path and os.path.exists(str(html_path))):
                    m = re.search(r'([A-Za-z]:[/\\][\w\\/\-\.]+\.html)', response)
                    if m:
                        cand = m.group(1).strip()
                        if os.path.exists(cand):
                            html_path = cand

                st.markdown(text_response)
                st.session_state.messages.append({"role": "assistant", "content": text_response})
                if html_path and os.path.exists(html_path):
                    st.session_state.current_graph = html_path

                # ---- Smart Entity & Flag detection ----
                prompt_lower = prompt.lower()
                
                # Alias routing for common short names
                alias_map = {
                    "customer":  "Dim_Customer", "customers": "Dim_Customer",
                    "product":   "Dim_Product",  "products":  "Dim_Product",
                    "sales":     "Fact_Sales",
                    "inventory": "Fact_Inventory",
                    "campaign":  "Dim_Campaign", "campaigns": "Dim_Campaign",
                    "supplier":  "Dim_Supplier", "suppliers": "Dim_Supplier",
                    "employee":  "Dim_Employee", "employees": "Dim_Employee",
                    "store":     "Dim_Store",    "stores":    "Dim_Store",
                    "promotion": "Dim_Promotion","promotions":"Dim_Promotion",
                    "geography": "Dim_Geography",
                    "date":      "Dim_Date"
                }

                detected = None
                
                # 1. Try alias map matching on words in the prompt
                for keyword, target_entity in alias_map.items():
                    if f" {keyword} " in f" {prompt_lower} " or prompt_lower.startswith(keyword) or prompt_lower.endswith(keyword):
                        detected = target_entity
                        break

                # 2. Regex fallback: Look for Dim_ / Fact_ in the prompt or response
                if not detected:
                    regex_matches = re.findall(r'(?:Dim|Fact)_[A-Za-z0-9_]+', prompt + " " + response)
                    if regex_matches:
                        detected = regex_matches[0]

                # 3. Pipeline fallback: Look for OLTP_ / FLATFILE_
                if not detected:
                    pipe_matches = re.findall(r'(?:OLTP|FLATFILE|API)_TO_DW_[A-Za-z0-9_]+', (prompt + " " + response).upper())
                    if pipe_matches:
                        detected = pipe_matches[0]

                show_all = any(k in prompt_lower for k in [
                    'lineage','trace','flow','tell me','show me','what is',
                    'context','investigate','analyze','analysis','about','drop','impact','change'
                ])
                # Detect if user wants detailed impact drill-down
                wants_detail = any(k in prompt_lower for k in [
                    'detail', 'more', 'deeper', 'drill', 'full', 'all reports', 'all pipelines'
                ])

                st.session_state.panel_entity    = detected or st.session_state.get("panel_entity", "Fact_Sales")
                st.session_state.impact_detailed = wants_detail

                st.session_state.panel_flags = {
                    "show_etl":     show_all or any(k in prompt_lower for k in ['etl','pipeline','log','execution','failure','failed','run','health','reconcili']),
                    "show_audit":   show_all or any(k in prompt_lower for k in ['audit','change','history','who','ddl','dcl','dml','schema']),
                    "show_versions":show_all or any(k in prompt_lower for k in ['version','change','history','etl','pipeline']),
                    "show_reports": show_all or any(k in prompt_lower for k in ['report','dashboard','bi','downstream']),
                }

                # ---- Auto-generate focused entity lineage graph ----
                if not (html_path and os.path.exists(str(html_path))):
                    try:
                        focused = generate_e2e_lineage_graph.run(st.session_state.panel_entity)
                        if focused and ".html" in str(focused):
                            m2 = re.search(r'([A-Za-z]:[/\\][\w\\/\-\.]+\.html)', str(focused))
                            if m2 and os.path.exists(m2.group(1).strip()):
                                st.session_state.current_graph = m2.group(1).strip()
                        if not st.session_state.get("current_graph"):
                            import src.agent.agent_tools_ext as _ext
                            dp = os.path.join(_ext.OUTPUT_DIR, f"{st.session_state.panel_entity}_e2e_lineage.html")
                            if os.path.exists(dp):
                                st.session_state.current_graph = dp
                    except Exception:
                        pass

        st.rerun()
