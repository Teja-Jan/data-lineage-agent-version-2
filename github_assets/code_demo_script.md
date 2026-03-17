# 📖 Live Demo Script: The Code Walkthrough

Use this guide to **read from** during your technical demonstration. It breaks down exactly what to **Show** and what to **Say** to survive any code-level scrutiny.

---

## 🛠️ Step 0: Dashboard Pre-Set (Before you start)
Open these GitHub tabs in your browser for fast switching:
1. `src/app.py`
2. `src/agent/llm_agent.py`
3. `src/agent/agent_tools_ext.py`

---

## 🟢 Phase 1: The Entry Point (`app.py`)

**👉 SHOW:** open `src/app.py` in your browser.

**🗣️ SAY:**
> *"To build our dashboard, we use **Streamlit**. Traditional UI code takes weeks, but Streamlit lets us build a dynamic, high-density visualization in Python."*
> *"Notice the left column represents a **Timeline Flow** from Left-to-Right layout (Sources ➡️ Pipelines ➡️ Targets ➡️ BI Reports). Clicking any node fires our state session `_drill()` function to generate a focused lineage visual in high performance."*

**🛡️ SCRUTINY DEFENSE (If asked):**
- **Q:** *"Is this just static HTML?"*
- **A:** *"No. We render interactive nodes on-the-fly using `generate_e2e_lineage_graph` (which uses NetworkX) based on dynamic user selection."*

---

## 🔵 Phase 2: The Orchestrator (`llm_agent.py`)

**👉 SHOW:** open `src/agent/llm_agent.py` in your browser. Point to the **`run_real_agent()`** function (Line 18).

**🗣️ SAY:**
> *"When a user interacts with the Chatbot, we don't just send text back to the LLM and hope it doesn't hallucinate. We use **Tool Calling** or Function Dispatching."*
> *"Inside `llm_agent.py`, we define structured functions like `get_full_impact_analysis` and explicitly register them with the Llama 3.3 model as JSON descriptors."*
> *"The backend reads the user prompt, determines that they are asking about dropping a column, and automatically outputs a **Tool Call wrapper wrapper** targeting our Python scripts instead of answering blindly."*

**🛡️ SCRUTINY DEFENSE (If asked):**
- **Q:** *"How do you prevent the AI from making up lineage?"*
- **A:** *"We force deterministic lookups. The AI does not 'recall' lineage; it **requests** lineage from our tools. It acts simply as a reasoning core to summarize the live DB rows."*

---

## 🔴 Phase 3: The Engine (`agent_tools_ext.py`)

**👉 SHOW:** open `src/agent/agent_tools_ext.py` in your browser. Find **`get_full_impact_analysis()`** (Line 107).

**🗣️ SAY:**
> *"This is the execution layer. When the AI calls `get_full_impact_analysis()`, the Python function connects strictly to our SQLite Data Warehouse."*
> *"It queries the `data_lineage_map` and `report_dependency` mapping tables to trace transitive dependencies. To calculate risk for a column drop, it traverses the graph in SQL to see which pipelines use that lookup and which downstream dashboards reference it."*
> *"It packages the matches back into structured JSON payloads, giving the AI 100% accurate data to finalize the response back to the UI interface."*

**🛡️ SCRUTINY DEFENSE (If asked):**
- **Q:** *"How does it handle scalability?"*
- **A:** *"Because the tools are querying relational index indices (`PRIMARY KEY` lookups on `data_lineage_map`), response times remain sub-millisecond even with thousands of assets."*

---

## 🟡 Phase 4: The Foundation (`src/data_generation/`)

**👉 SHOW:** open **`src/data_generation/`** folder on GitHub.

**🗣️ SAY:**
> *"Before the AI can read lineage, the data ecosystem must exist. We have a set of data-seeder scripts that initialize our simulated Enterprise environment."*
> *"For example, `setup_target_dw.py` builds the Dimension and Fact tables to form the Star Schema structure. `generate_sources.py` simulates receiving daily flatfiles uploads."*
> *"Finally, `run_etl.py` executes the actual pipeline that pulls data from those staging files and pushes them strictly into loaded targets. This simulates a real production cycle that our AI then monitors."*

**🛡️ SCRUTINY DEFENSE (If asked):**
- **Q:** *"Is this live ETL or mock data?"*
- **A:** *"It executes live Python ETL logic querying files and writing directly to our SQLite DB coordinates, mirroring a modular Production ETL tool framework pipeline."*

---

## 🔬 General Defensibility Cheat Sheet

| Scrutiny Question | The Answer to Give |
| :--- | :--- |
| **Is it inside a Docker contain?** | It runs fully containerized or on a virtual environment grid (`venv`) independently with local DB mapping caches. |
| **What happens if a source DB goes offline?** | We wouldn't know at query time unless we run live pings; however, our sub-system execution logs (`etl_execution_logs`) will flag **Failed runs** and display Connection Errors automatically on the sidebar panels without fully locking out the UI flow. |
| **How do you keep the DAG updated?** | The audit log triggers can be tied to DB hooks or webhooks, ensuring any schema modification appends onto `db_audit_log` continuously keeping the visual map strictly synchronous. |
