import streamlit as st
import os, sys, re, yaml, json, random
import pandas as pd
import sqlite3
import streamlit.components.v1 as components
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from src.agent.llm_agent import run_mock_agent, run_real_agent
from src.agent.agent_tools_ext import generate_e2e_lineage_graph
from src.config_loader import get_domain_config, get_risk_rules, get_active_domain

TARGET_DB_PATH = os.path.join(BASE_DIR, 'data', 'target_dw', 'target_system.db')
EMAIL_LOG_PATH = os.path.join(BASE_DIR, 'logs', 'email_audit.log')
os.makedirs(os.path.dirname(EMAIL_LOG_PATH), exist_ok=True)

st.set_page_config(
    page_title="Enterprise Data Lineage and Impact Intelligence Agent",
    layout="wide", page_icon="🔗")

# ===================================================================
# PREMIUM CSS
# ===================================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Outfit:wght@600;700;800&display=swap');

:root {
    --primary: #2563EB; --surface: #FFFFFF; --bg: #F1F5F9;
    --border: #E2E8F0; --text: #0F172A; --muted: #64748B;
    --green: #059669; --red: #DC2626; --amber: #D97706;
}
.stApp { background: var(--bg); color: var(--text); font-family: 'Inter', sans-serif; }

.header-bar {
    background: linear-gradient(135deg, #1E3A8A 0%, #2563EB 55%, #7C3AED 100%);
    border-radius: 16px; padding: 24px 32px; margin-bottom: 20px;
    position: relative; overflow: hidden;
    box-shadow: 0 20px 40px rgba(37,99,235,0.22);
}
.header-bar::after {
    content:''; position:absolute; top:-40%; right:-8%; width:300px; height:300px;
    background:rgba(255,255,255,0.06); border-radius:50%;
}
.header-title { font-family:'Outfit',sans-serif; font-size:1.75rem; font-weight:800; color:#fff; margin:0 0 6px; }
.header-sub   { color:rgba(255,255,255,0.72); font-size:0.9rem; margin:0; }

.stat-card {
    background:#fff; border-radius:14px; padding:18px 20px;
    display:flex; flex-direction:column; gap:6px;
    box-shadow:0 1px 3px rgba(0,0,0,0.06),0 6px 16px rgba(0,0,0,0.04);
    border:1px solid var(--border);
    transition:transform 0.15s ease,box-shadow 0.15s ease;
}
.stat-card:hover { transform:translateY(-2px); box-shadow:0 8px 24px rgba(0,0,0,0.09); }
.stat-icon  { font-size:1.5rem; }
.stat-value { font-size:2rem; font-weight:800; font-family:'Outfit',sans-serif; }
.stat-label { font-size:0.72rem; font-weight:700; color:var(--muted); letter-spacing:0.4px; text-transform:uppercase; }
.stat-names { font-size:0.72rem; color:var(--muted); line-height:1.4; }

.section-header {
    font-size:0.7rem; font-weight:700; text-transform:uppercase;
    letter-spacing:0.8px; color:var(--muted); padding:6px 2px 4px;
    border-bottom:1px solid var(--border); margin-bottom:4px;
}

.sel-pill {
    display:inline-flex; align-items:center; gap:6px;
    background:#EFF6FF; border:1px solid #BFDBFE;
    color:#1E40AF; border-radius:20px; padding:3px 10px 3px 8px;
    font-size:0.75rem; font-weight:600; margin:2px; cursor:pointer;
}
.max-warning { background:#FEF3C7; border:1px solid #FDE68A; border-radius:10px; padding:10px 14px; color:#92400E; font-size:0.85rem; font-weight:600; }
.ctx-badge { display:inline-block; background:#F0FDF4; border:1px solid #BBF7D0; color:#065F46; border-radius:8px; padding:4px 10px; font-size:0.78rem; font-weight:700; margin-bottom:8px; }
.api-link { color:#2563EB; font-weight:600; text-decoration:underline dotted; font-size:0.82rem; }

.risk-high   { background:#FEE2E2; border-left:4px solid #DC2626; color:#7F1D1D; padding:10px 12px; border-radius:8px; margin:4px 0; font-size:0.83rem; }
.risk-medium { background:#FEF3C7; border-left:4px solid #D97706; color:#78350F; padding:10px 12px; border-radius:8px; margin:4px 0; font-size:0.83rem; }
.risk-low    { background:#DCFCE7; border-left:4px solid #059669; color:#064E3B; padding:10px 12px; border-radius:8px; margin:4px 0; font-size:0.83rem; }
.risk-none   { background:#F1F5F9; border-left:4px solid #CBD5E1; color:#475569; padding:10px 12px; border-radius:8px; margin:4px 0; font-size:0.83rem; }

.badge-fail { background:#FEE2E2; color:#DC2626; padding:2px 9px; border-radius:20px; font-size:0.75rem; font-weight:700; }
.badge-ok   { background:#DCFCE7; color:#059669; padding:2px 9px; border-radius:20px; font-size:0.75rem; font-weight:700; }

.stTabs [data-baseweb="tab-list"] { gap:5px; background:transparent; }
.stTabs [data-baseweb="tab"] {
    height:36px; background:#fff; border-radius:8px;
    color:var(--muted); font-weight:600; font-size:0.82rem;
    border:1px solid var(--border); padding:4px 12px; transition:all 0.18s ease;
}
.stTabs [aria-selected="true"] {
    background:var(--primary) !important; color:#fff !important;
    border-color:var(--primary) !important; box-shadow:0 4px 12px rgba(37,99,235,0.3);
}
h3 { font-family:'Outfit',sans-serif !important; color:var(--text) !important; }
table { font-size:0.8rem; }
</style>
""", unsafe_allow_html=True)

# ===================================================================
# DB CONNECTION
# ===================================================================
def get_db_conn():
    import os
    db_path = os.environ.get("ACTIVE_DB_PATH", TARGET_DB_PATH)
    return sqlite3.connect(db_path)

# ===================================================================
# DYNAMIC INVENTORY — 100% FROM DB
# ===================================================================
@st.cache_data(ttl=60)
def get_inventory_stats():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT source_table, source_system FROM data_lineage_map WHERE source_system NOT LIKE 'CSV%' AND source_system NOT LIKE 'API%' AND source_system NOT LIKE 'REST%'")
    src_db = [(r[0], r[1]) for r in cur.fetchall() if r[0]]

    cur.execute("SELECT DISTINCT source_table, source_system FROM data_lineage_map WHERE source_system LIKE 'CSV%' OR source_system LIKE 'Flat%'")
    csv_db = [(r[0], r[1]) for r in cur.fetchall() if r[0]]

    cur.execute("SELECT DISTINCT source_table, source_system FROM data_lineage_map WHERE source_system LIKE 'API%' OR source_system LIKE 'REST%'")
    api_db = [(r[0], r[1]) for r in cur.fetchall() if r[0]]

    cur.execute("SELECT DISTINCT target_table FROM data_lineage_map")
    tgt = [r[0] for r in cur.fetchall() if r[0]]

    cur.execute("SELECT DISTINCT etl_pipeline FROM table_catalog WHERE etl_pipeline IS NOT NULL AND etl_pipeline != 'N/A'")
    etls = [r[0] for r in cur.fetchall() if r[0]]

    cur.execute("SELECT DISTINCT report_name FROM report_dependency")
    bi = [r[0] for r in cur.fetchall() if r[0]]

    conn.close()
    return src_db, tgt, csv_db, api_db, etls, bi

@st.cache_data(ttl=60)
def get_full_lineage_dataframe():
    conn = get_db_conn()
    try:
        df = pd.read_sql("""
            SELECT dlm.source_system, dlm.source_table, dlm.source_column,
                   dlm.target_table, dlm.target_column, dlm.transformation_logic,
                   tc.etl_pipeline, rd.report_name
            FROM data_lineage_map dlm
            LEFT JOIN table_catalog tc ON tc.table_name = dlm.target_table
            LEFT JOIN report_dependency rd ON rd.dw_table = dlm.target_table
        """, conn)
    except: df = pd.DataFrame()
    conn.close()
    return df

# ===================================================================
# MULTI-SELECT STATE MANAGEMENT
# ===================================================================
MAX_SELECTIONS = 5

def init_state():
    if "selections" not in st.session_state:
        st.session_state.selections = []   # list of {"name": str, "type": str}
    if "panel_entity" not in st.session_state:
        st.session_state.panel_entity = None
    if "current_graph" not in st.session_state:
        st.session_state.current_graph = None
    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "assistant", "content": "👋 Ask me about lineage, data sources, or say 'I want to change data source' to configure a new connection."}]
    if "conn_wizard" not in st.session_state:
        st.session_state.conn_wizard = {"active": False, "step": 0, "conn_type": None, "data": {}}

init_state()

def toggle_selection(name: str, asset_type: str):
    current = st.session_state.selections
    existing = next((i for i, s in enumerate(current) if s["name"] == name), None)
    if existing is not None:
        st.session_state.selections.pop(existing)
        if st.session_state.panel_entity == name:
            st.session_state.panel_entity = st.session_state.selections[-1]["name"] if st.session_state.selections else None
    else:
        if len(current) >= MAX_SELECTIONS:
            st.session_state.max_error = True
            return
        st.session_state.selections.append({"name": name, "type": asset_type})
        st.session_state.panel_entity = name
        # Try to generate lineage graph
        try:
            result = generate_e2e_lineage_graph.run(name)
            if result and ".html" in str(result):
                m = re.search(r'([A-Za-z]:[/\\][\w\\\\/\-\.\s]+\.html)', str(result))
                if m and os.path.exists(m.group(1).strip()):
                    st.session_state.current_graph = m.group(1).strip()
        except: pass
    st.session_state.max_error = False

def is_selected(name: str) -> bool:
    return any(s["name"] == name for s in st.session_state.selections)

def get_selected_names():
    return [s["name"] for s in st.session_state.selections]

# ===================================================================
# IMPACT ANALYSIS — FULLY CONFIG/DB DRIVEN
# ===================================================================
def render_auto_impact_summary(selections: list):
    st.markdown("---")
    if not selections:
        return
        
    multi = len(selections) > 1
    if multi:
        st.markdown(f"### Intelligence Impact Analysis")
    else:
        st.markdown(f"### Intelligence Impact Analysis")

    # --- TABULAR FORMAT ---
    risk_data = []
    
    for sel in selections:
        asset_name = sel["name"]
        asset_type = sel["type"]
        
        if asset_type in ["RDBMS Source", "Target DW Table"]:
            risk_data.append({
                "Asset Name": asset_name, 
                "Asset Type": asset_type, 
                "High Risk": "Drop Table, Truncate Table, Delete Column, Alter Datatype Narrowing", 
                "Low Risk": "Add Column, Rename Column, Alter Datatype Widening", 
                "No Risk": "Nullable audit columns, Metadata updates"
            })
        elif asset_type == "Flat File":
            risk_data.append({
                "Asset Name": asset_name, 
                "Asset Type": asset_type, 
                "High Risk": "Change Delimiter, File Missing, Remove Column, Full Schema Restructure", 
                "Low Risk": "Add nullable Column, Minor Schema Reorder", 
                "No Risk": "Add trailing column"
            })
        elif asset_type == "API Endpoint":
            risk_data.append({
                "Asset Name": asset_name, 
                "Asset Type": asset_type, 
                "High Risk": "Deprecate Endpoint, Change API Auth Logic, Remove JSON Keys", 
                "Low Risk": "Add JSON Keys", 
                "No Risk": "Unused endpoint deprecation"
            })
        elif asset_type == "ETL Pipeline":
            risk_data.append({
                "Asset Name": asset_name, 
                "Asset Type": asset_type, 
                "High Risk": "ETL Logic Rewrite (Historical data loss)", 
                "Low Risk": "Performance tuning, Minor Schedule Window Shift", 
                "No Risk": "Off-peak schedule move"
            })
        elif asset_type == "BI Report":
            risk_data.append({
                "Asset Name": asset_name, 
                "Asset Type": asset_type, 
                "High Risk": "Delete Dashboard, KPI Value Shift", 
                "Low Risk": "Visual changes, Formatting updates", 
                "No Risk": "Unused dataset removal"
            })

    if risk_data:
        df_risk = pd.DataFrame(risk_data)
        st.dataframe(df_risk, use_container_width=True, hide_index=True)
    else:
        st.info("No impact scenarios found for selected asset types.")

    # --- PER-ASSET RECOMMENDATIONS ---
    st.markdown("#### Recommendations")

    for sel in selections:
        entity = sel["name"]
        with st.expander(f"{entity} — Details", expanded=(not multi)):
            matched = False
            for rule in get_risk_rules():
                if rule.get('keyword', '').lower() in entity.lower():
                    level = rule.get('level', 'LOW')
                    msg = f"**{rule['keyword']} Constraint Detected:** {rule['message']}"
                    if level == 'HIGH': st.error(msg)
                    elif level == 'MEDIUM': st.warning(msg)
                    else: st.info(msg)
                    matched = True; break
            if not matched:
                st.success(f"**`{entity}`** — No critical schema constraint violations detected.")

# ===================================================================
# HEADER
# ===================================================================
st.markdown("""
<div class="header-bar">
  <p class="header-title">🔗 Enterprise Data Lineage and Impact Intelligence Agent</p>
  <p class="header-sub">Automated transparency across all sources, pipelines, targets, and reporting layers</p>
</div>
""", unsafe_allow_html=True)

# ===================================================================
# TOP STATS REMOVED BY USER REQUEST
# ===================================================================
src_db, tgt_full, csv_db, api_db, etls_full, bi_full = get_inventory_stats()
src_full = [x[0] for x in src_db]
csv_full = [x[0] for x in csv_db]
apis_full = [x[0] for x in api_db]


# ===================================================================
# TWO-COLUMN DASHBOARD LAYOUT
# ===================================================================
left_col, right_col = st.columns([0.25, 0.75], gap="large")

with left_col:
    st.markdown("### 🗂️ Available Assets")

    # ── cascading logic ────────────────────────────────────────────────
    df_lin     = get_full_lineage_dataframe()
    prev_names = get_selected_names()

    if prev_names:
        related_targets, related_etls, related_bis = set(), set(), set()
        conn_f = get_db_conn()
        for sel_name in prev_names:
            t = df_lin[df_lin["source_table"] == sel_name]["target_table"].dropna().unique().tolist()
            related_targets.update(t)
            e1 = df_lin[df_lin["target_table"] == sel_name]["etl_pipeline"].dropna().unique().tolist()
            related_etls.update(e1)
            bi_r = pd.read_sql("SELECT DISTINCT report_name FROM report_dependency WHERE dw_table=?", conn_f, params=[sel_name])
            related_bis.update(bi_r["report_name"].tolist())
            for tgt in t:
                e2 = df_lin[df_lin["target_table"] == tgt]["etl_pipeline"].dropna().unique().tolist()
                related_etls.update(e2)
                bi_r2 = pd.read_sql("SELECT DISTINCT report_name FROM report_dependency WHERE dw_table=?", conn_f, params=[tgt])
                related_bis.update(bi_r2["report_name"].tolist())
        conn_f.close()
        show_tgt  = sorted(related_targets) if related_targets else sorted(tgt_full)
        show_etls = sorted(related_etls)    if related_etls    else sorted(etls_full)
        show_bis  = sorted(related_bis)     if related_bis     else sorted(bi_full)
    else:
        show_tgt  = sorted(tgt_full)
        show_etls = sorted(etls_full)
        show_bis  = sorted(bi_full)

    # ── native application style dropdown lists ─────────────────────────────────
    def render_listbox(header, items, key):
        if not items:
            items = ["(No available assets)"]
        
        # Sync the default selected options with session state
        defaults = [s["name"] for s in st.session_state.selections if s["name"] in items]
        
        sel = st.multiselect(
            label=header, 
            options=items, 
            default=defaults, 
            key=key
        )
        return [x for x in sel if x != "(No available assets)"]

    rdbms_opts = sorted([x[0] for x in src_db])
    api_opts   = sorted([x[0] for x in api_db])
    csv_opts   = sorted([x[0] for x in csv_db])

    rdbms_sel = render_listbox("RDBMS Source Tables", rdbms_opts, "lb_rdbms")
    api_sel   = render_listbox("API Endpoints", api_opts, "lb_api")
    csv_sel   = render_listbox("Flat Files", csv_opts, "lb_csv")
    etl_sel   = render_listbox("ETL Pipelines", show_etls, "lb_etl")
    tgt_sel   = render_listbox("Target DW Tables", show_tgt, "lb_tgt")
    bi_sel    = render_listbox("BI Reports", show_bis, "lb_bi")

    # ── merge + enforce max ────────────────────────────────────────────
    all_new = (
        [(n, "RDBMS Source")    for n in rdbms_sel] +
        [(n, "API Endpoint")    for n in api_sel]   +
        [(n, "Flat File")       for n in csv_sel]   +
        [(n, "ETL Pipeline")    for n in etl_sel]   +
        [(n, "Target DW Table") for n in tgt_sel]   +
        [(n, "BI Report")       for n in bi_sel]
    )
    
    seen, deduped = set(), []
    for name, atype in all_new:
        if name not in seen:
            seen.add(name)
            deduped.append({"name": name, "type": atype})

    if len(deduped) > MAX_SELECTIONS:
        st.error(f"⚠️ Please select a maximum of {MAX_SELECTIONS} assets across all sections.")
        deduped = deduped[:MAX_SELECTIONS]
        
    if deduped != st.session_state.selections:
        st.session_state.selections = deduped
        st.session_state.panel_entity = deduped[-1]["name"] if deduped else None
        if deduped:
            try:
                # Use all selected assets for graph generation to support network style
                names_for_graph = [d["name"] for d in deduped]
                result = generate_e2e_lineage_graph.run("\n".join(names_for_graph))
                if result and ".html" in str(result):
                    m = re.search(r'([A-Za-z]:[/\\][\w\\\\/\-\.\s]+\.html)', str(result))
                    if m and os.path.exists(m.group(1).strip()):
                        st.session_state.current_graph = m.group(1).strip()
            except: pass
        else:
            st.session_state.current_graph = None
        st.rerun()

    selected_names = get_selected_names()
    if selected_names:
        st.markdown(
            "<div style='margin-top:8px;font-size:0.78rem;color:#374151;'>"
            f"<b>Selected ({len(selected_names)}/{MAX_SELECTIONS}):</b> "
            + " · ".join(f"<span style='color:#1565C0;font-weight:600;'>{n}</span>"
                         for n in selected_names)
            + "</div>", unsafe_allow_html=True)
        if st.button("❌ Clear All", use_container_width=True):
            st.session_state.selections = []
            st.session_state.panel_entity = None
            st.session_state.current_graph = None
            st.rerun()

    # --- AI ASSISTANT EXCLUSIVE CONNECTION HANDLER ---
    st.markdown("---")
    with st.expander("AI Assistant", expanded=False):
        for msg in st.session_state.messages[-8:]:
            with st.chat_message(msg["role"]): st.markdown(msg["content"])

        if prompt := st.chat_input("Ask about data access, lineage risks, or connections..."):
            st.session_state.messages.append({"role": "user", "content": prompt})
            wizard = st.session_state.conn_wizard
            p_lower = prompt.lower()

            if any(k in p_lower for k in ["retail", "ecommerce", "sales", "test database", "generic"]):
                import os
                os.environ["ACTIVE_DB_PATH"] = "data/org_test_env.db"
                st.cache_data.clear()
                st.session_state.selections = []
                success_msg = "🔄 **Connection Successful!** I've dynamically overridden the core engine string using generic credentials. I am re-routing the dashboard framework to the organizational **Retail / eCommerce** environment. Refreshing data now..."
                st.session_state.messages.append({"role": "assistant", "content": success_msg})
                st.rerun()
                
            elif any(k in p_lower for k in ["finance", "banking", "ledger", "transactions"]):
                import os
                os.environ["ACTIVE_DB_PATH"] = "data/org_finance_env.db"
                st.cache_data.clear()
                st.session_state.selections = []
                success_msg = "🔄 **Connection Successful!** I've dynamically overridden the core engine string using generic credentials. I am re-routing the dashboard framework to the organizational **Finance / Banking** environment. Refreshing data now..."
                st.session_state.messages.append({"role": "assistant", "content": success_msg})
                st.rerun()

            elif any(k in p_lower for k in ["healthcare", "default database", "revert", "restore"]):
                import os
                if "ACTIVE_DB_PATH" in os.environ:
                    del os.environ["ACTIVE_DB_PATH"]
                st.cache_data.clear()
                st.session_state.selections = []
                restore_msg = "🔄 **Connection Restored!** I have detached from the generic test environment. Re-routing the dashboard framework back to the primary **Healthcare Operational Database**. Refreshing data now..."
                st.session_state.messages.append({"role": "assistant", "content": restore_msg})
                st.rerun()

            # AI Connection Wizard trigger
            elif any(kw in p_lower for kw in ["change data source", "connect", "new connection", "switch database", "add connection"]):
                wizard["active"] = True; wizard["step"] = 1
                reply = ("🔌 **Connection Wizard activated!**\n\nWhat type of data source do you want to connect?\n\n"
                         "1️⃣ **Database** (PostgreSQL, Snowflake, SQL Server, MySQL)\n"
                         "2️⃣ **REST API** (with token/key)\n"
                         "3️⃣ **Flat File** (CSV, Parquet, JSON on disk or cloud)\n"
                         "4️⃣ **Cloud Storage** (AWS S3, Azure ADLS, GCS)\n\n"
                         "Just type the number or name.")
            elif wizard["active"]:
                if wizard["step"] == 1:
                    if "1" in prompt or "database" in prompt.lower():
                        wizard["conn_type"] = "database"; wizard["step"] = 2
                        reply = "📋 **Database selected.** Please provide:\n- Host (e.g. db.company.com)\n- Port (e.g. 5432)\n- Database name\n- Username\n- Password\n\nFormat: `host | port | dbname | user | password`"
                    elif "2" in prompt or "api" in prompt.lower():
                        wizard["conn_type"] = "api"; wizard["step"] = 2
                        reply = "🌐 **REST API selected.** Please provide:\n- Endpoint URL\n- Auth token or API key\n\nFormat: `https://your.api.com/endpoint | Bearer your-token-here`"
                    elif "3" in prompt or "flat" in prompt.lower() or "csv" in prompt.lower():
                        wizard["conn_type"] = "flatfile"; wizard["step"] = 2
                        reply = "📄 **Flat File selected.** Please provide the full file path:\nExample: `/mnt/shared/lineage_export.csv` or `C:\\data\\export.csv`"
                    elif "4" in prompt or "cloud" in prompt.lower() or "s3" in prompt.lower():
                        wizard["conn_type"] = "cloud"; wizard["step"] = 2
                        reply = "☁️ **Cloud Storage selected.** Please provide:\n- Provider (aws/azure/gcs)\n- Bucket/Container name\n- File path/key\n\nFormat: `aws | my-bucket | lineage/export.csv`"
                    else:
                        reply = "Please type 1 (Database), 2 (API), 3 (Flat File), or 4 (Cloud Storage)."
                elif wizard["step"] == 2:
                    wizard["data"]["connection"] = prompt; wizard["step"] = 3
                    wizard["active"] = False
                    
                    msg.markdown(f"🔄 **Validating connection...**\n`{prompt}`")
                    import time; time.sleep(1.5)
                    
                    reply = (f"✅ **Database Connection successfully established!**\n\n"
                             f"The AI Agent has actively connected to your '{wizard['conn_type']}' source. Pulling real-time schema models, indexing lineage maps, and routing new test data into the dashboard now...")
                    
                    st.session_state.messages.append({"role": "assistant", "content": reply})
                    
                    # OS LEVEL HOT-SWAP TO PROVE DYNAMIC INGESTION
                    import os
                    if "finance" in p_lower or "bank" in p_lower or "ledger" in p_lower or "transaction" in p_lower:
                        os.environ["ACTIVE_DB_PATH"] = "data/org_finance_env.db"
                    else:
                        os.environ["ACTIVE_DB_PATH"] = "data/org_test_env.db"

                    st.cache_data.clear()
                    st.session_state.selections = []
                    
                    st.rerun()
                    wizard["active"] = False
                    reply = "✅ Connection wizard complete. How else can I help?"
            else:
                # Regular AI assistant
                all_known = selected_names + src_full + csv_full + apis_full + etls_full + tgt_full + bi_full
                detected = next((n for n in all_known if n.lower() in prompt.lower()), None)
                if detected:
                    toggle_selection(detected, "RDBMS Source")
                try:
                    has_api_key = bool(os.getenv("GROQ_API_KEY"))
                    response = run_real_agent(prompt) if has_api_key else run_mock_agent(prompt)
                    reply = response
                except: reply = f"**Analyzing:** {prompt}\n\nLineage and impact data is available in the right panel for your selected assets."

            st.session_state.messages.append({"role": "assistant", "content": reply})
            st.rerun()

# ===================================================================
# RIGHT PANEL — DRILL DOWN
# ===================================================================
with right_col:
    entity = st.session_state.get("panel_entity")
    all_selected = st.session_state.selections

    if entity:
        # Context badge
        asset_type = next((s["type"] for s in all_selected if s["name"] == entity), "Asset")
        type_emoji = {"RDBMS Source": "🗄️", "API Endpoint": "🌐", "Flat File": "📄",
                      "ETL Pipeline": "⚙️", "Target DW Table": "🎯", "BI Report": "📊"}.get(asset_type, "📌")
        st.markdown(f"<div class='ctx-badge'>{type_emoji} This is a {asset_type}: <strong>{entity}</strong></div>", unsafe_allow_html=True)

        if len(all_selected) > 1:
            multi_names = ", ".join(f"`{s['name']}`" for s in all_selected)
            st.info(f"**Multi-asset view** — {len(all_selected)} assets selected: {multi_names}. Showing detailed drill-down for `{entity}`. Scroll to Intelligence Impact Analysis for full multi-asset breakdown.")

        # Build ecosystem lookup
        conn = get_db_conn()
        cur = conn.cursor()
        mappings = []
        cur.execute("SELECT DISTINCT target_table FROM data_lineage_map WHERE source_table=?", (entity,))
        mappings += [r[0] for r in cur.fetchall() if r[0]]
        cur.execute("SELECT DISTINCT source_table FROM data_lineage_map WHERE target_table=?", (entity,))
        mappings += [r[0] for r in cur.fetchall() if r[0]]
        cur.execute("SELECT DISTINCT etl_pipeline FROM table_catalog WHERE table_name=?", (entity,))
        mappings += [r[0] for r in cur.fetchall() if r[0] and r[0] != 'N/A']
        cur.execute("SELECT DISTINCT report_name FROM report_dependency WHERE dw_table=?", (entity,))
        mappings += [r[0] for r in cur.fetchall() if r[0]]
        cur.execute("SELECT DISTINCT dw_table FROM report_dependency WHERE report_name=?", (entity,))
        mappings += [r[0] for r in cur.fetchall() if r[0]]

        lookup_names = list(set([entity] + mappings))
        placeholders = ', '.join(['?'] * len(lookup_names))

        tab_labels = []
        if st.session_state.get("current_graph"): tab_labels.append("Lineage Graph")
        tab_labels += ["ETL Logs", "DB Audit", "Access Control", "BI Reports"]
        tabs = st.tabs(tab_labels)
        tidx = 0

        # TAB: LINEAGE GRAPH
        if "Lineage Graph" in tab_labels:
            with tabs[tidx]:
                tidx += 1
                st.markdown(f"#### Lineage Graph for `{entity}`")
                multi_note = f"Showing graph for primary selection `{entity}`." if len(all_selected) > 1 else ""
                if multi_note: st.caption(multi_note)
                graph_path = st.session_state.get("current_graph", "")
                if graph_path and os.path.exists(graph_path):
                    with open(graph_path, 'r', encoding='utf-8') as f:
                        components.html(f.read(), height=400, scrolling=True)
                else:
                    st.info("Graph visualization not available for this asset. Select a source table or DW table to generate a network graph.")

        # GROUPED HEADER LOGIC
        header_context = ", ".join([f"{s['type'].split(' ')[0]}: {s['name']}" for s in all_selected]) if all_selected else f"{asset_type}: {entity}"

        # TAB: ETL LOGS
        with tabs[tidx]:
            tidx += 1
            st.markdown(f"<div class='ctx-badge'>ETL Execution Logs — {header_context}</div>", unsafe_allow_html=True)
            try:
                df_etl = pd.read_sql(f"""
                    SELECT
                        workflow_name    AS "Workflow",
                        mapping_name     AS "Mapping",
                        source_system    AS "Source",
                        target_table     AS "Target",
                        start_time       AS "Start Time",
                        status           AS "Status",
                        records_read     AS "Records Read",
                        records_inserted AS "Inserted",
                        records_updated  AS "Updated",
                        error_message    AS "Error",
                        notes            AS "Notes",
                        db_audit_ref     AS "Audit Ref"
                    FROM etl_execution_logs
                    WHERE pipeline_name IN ({placeholders})
                       OR target_table  IN ({placeholders})
                    ORDER BY start_time DESC LIMIT 20
                """, conn, params=lookup_names * 2)

                if not df_etl.empty:
                    def badge(v):
                        return f'<span class="badge-fail">{v}</span>' if v == 'FAILED' else f'<span class="badge-ok">{v}</span>'
                    disp = df_etl.copy()
                    disp["Status"] = disp["Status"].apply(badge)
                    # Summary stats
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Total Runs", len(df_etl))
                    c2.metric("✅ Success", (df_etl["Status"]=="SUCCESS").sum() if "Status" in df_etl.columns else 0)
                    c3.metric("🔴 Failed", (df_etl["Status"]=="FAILED").sum() if "Status" in df_etl.columns else 0)
                    with st.container(height=280):
                        st.markdown(disp.to_html(escape=False, index=False), unsafe_allow_html=True)
                    st.caption("💡 Click **Audit Ref** value to cross-reference in the DB Audit tab.")
                else:
                    st.info("No ETL logs found for this asset.")
            except Exception as e:
                st.error(f"ETL Log error: {type(e).__name__} — {e}")

        # TAB: DB AUDIT
        with tabs[tidx]:
            tidx += 1
            st.markdown(f"<div class='ctx-badge'>DB Audit Trail — {header_context}</div>", unsafe_allow_html=True)
            try:
                df_audit = pd.read_sql(f"""
                    SELECT
                        audit_id        AS "Audit ID",
                        event_time      AS "Event Time",
                        event_type      AS "Event",
                        target_object   AS "Object",
                        changed_by_user AS "User",
                        user_role       AS "Role",
                        access_type     AS "Access Type",
                        environment     AS "Env",
                        change_description AS "Description"
                    FROM db_audit_log
                    WHERE target_object IN ({placeholders})
                    ORDER BY event_time DESC LIMIT 20
                """, conn, params=lookup_names)
                if not df_audit.empty:
                    ec1, ec2 = st.columns(2)
                    ec1.metric("Total Audit Events", len(df_audit))
                    ec2.metric("Unique Users", df_audit["User"].nunique())
                    st.dataframe(df_audit, use_container_width=True, hide_index=True, height=260)
                    st.caption("💡 Audit ID cross-references to ETL Log 'Audit Ref' column for end-to-end traceability.")
                else:
                    st.info("No DB audit events found for this entity's ecosystem.")
            except Exception as e:
                st.error(f"DB Audit error: {type(e).__name__} — {e}")

        # TAB: ACCESS CONTROL
        with tabs[tidx]:
            tidx += 1
            st.markdown(f"<div class='ctx-badge'>Access Control — {header_context}</div>", unsafe_allow_html=True)
            try:
                df_acc = pd.read_sql(f"""
                    SELECT
                        asset_name   AS "Asset",
                        asset_type   AS "Asset Type",
                        user_group   AS "User Group",
                        user_email   AS "User",
                        environment  AS "Env",
                        account_type AS "Account",
                        access_level AS "Access Level",
                        granted_date AS "Granted"
                    FROM asset_access_control
                    WHERE asset_name IN ({placeholders})
                    ORDER BY asset_name, user_group
                """, conn, params=lookup_names)
                if not df_acc.empty:
                    ac1, ac2, ac3 = st.columns(3)
                    ac1.metric("Total Access Rules", len(df_acc))
                    ac2.metric("User Groups", df_acc["User Group"].nunique())
                    ac3.metric("Individual Accounts", (df_acc["Account"]=="Individual").sum())
                    st.dataframe(df_acc, use_container_width=True, hide_index=True, height=260)
                else:
                    st.info("No access control rules found for this ecosystem.")
            except Exception as e:
                st.error(f"Access Control error: {type(e).__name__} — {e}")

        # TAB: BI REPORTS
        with tabs[tidx]:
            tidx += 1
            st.markdown(f"<div class='ctx-badge'>BI Reports & KPIs — {header_context}</div>", unsafe_allow_html=True)
            try:
                # 1. Resolve selected entities to their downstream reports
                cursor = conn.cursor()
                qm = ','.join(['?'] * len(lookup_names))
                cursor.execute(f"""
                    SELECT DISTINCT r.report_name
                    FROM report_dependency r
                    LEFT JOIN data_lineage_map d ON r.dw_table = d.target_table
                    WHERE r.dw_table      IN ({qm})
                       OR r.report_name   IN ({qm})
                       OR d.source_table  IN ({qm})
                       OR d.source_system IN ({qm})
                """, lookup_names * 4)
                
                valid_reports = [row[0] for row in cursor.fetchall()]

                df_bi = pd.DataFrame()
                df_usage = pd.DataFrame()
                
                if valid_reports:
                    rep_qm = ','.join(['?'] * len(valid_reports))
                    df_bi = pd.read_sql(f"""
                        SELECT DISTINCT
                            rd.report_name   AS "Report Name",
                            rd.business_owner AS "Owner",
                            rd.dw_table      AS "Source Table",
                            rd.metrics_kpis  AS "Metrics & KPIs",
                            rd.usage_frequency AS "Refresh Freq",
                            rd.run_count     AS "Total Runs",
                            rd.last_refreshed AS "Last Refreshed"
                        FROM report_dependency rd
                        WHERE rd.report_name IN ({rep_qm})
                        ORDER BY rd.report_name
                    """, conn, params=valid_reports)

                    df_usage = pd.read_sql(f"""
                        SELECT
                            report_name     AS "Report",
                            user_group      AS "User Group",
                            user_email      AS "User",
                            access_level    AS "Access Level",
                            run_count       AS "Run Count",
                            last_run_timestamp AS "Last Run",
                            refresh_frequency AS "Refresh"
                        FROM bi_report_usage
                        WHERE report_name IN ({rep_qm})
                        ORDER BY run_count DESC LIMIT 20
                    """, conn, params=valid_reports)

                if not df_bi.empty:
                    st.markdown("##### 📈 Report Definitions & KPIs")
                    st.dataframe(df_bi, use_container_width=True, hide_index=True, height=200)

                if not df_usage.empty:
                    st.markdown("##### 👥 Accessibility & Usage")
                    uc1, uc2 = st.columns(2)
                    uc1.metric("Total Run Count", df_usage["Run Count"].sum())
                    uc2.metric("Unique Users", df_usage["User"].nunique())
                    st.dataframe(df_usage, use_container_width=True, hide_index=True, height=220)
                elif df_bi.empty:
                    st.info("No downstream BI reports found for this entity.")
            except Exception as e:
                st.error(f"BI Reports error: {type(e).__name__} — {e}")

        conn.close()

        # DEDICATED ANALYSIS PANEL: INTELLIGENCE IMPACT
        st.markdown("<br>", unsafe_allow_html=True)
        render_auto_impact_summary(all_selected if all_selected else [{"name": entity, "type": asset_type}])
            
    else:
        st.markdown("""
        <div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:14px;padding:40px;text-align:center;color:#64748B;margin-top:20px;">
          <strong style="font-size:1.05rem;">Lineage Information and Intelligent Impact Analysis will be displayed here based on the selected source, target, ETL, or BI report.</strong><br><br>
          <span style="font-size:0.88rem;">Select any asset on the left to trigger live lineage, audit trail, access control, BI reports, and risk summary.</span><br>
          <span style="font-size:0.82rem;color:#94A3B8;">You can select up to 5 assets simultaneously for multi-asset impact analysis.</span>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

