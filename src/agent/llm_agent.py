import os
import sys
from dotenv import load_dotenv

# Import core tools
from .agent_tools import (
    get_table_lineage, generate_lineage_graph, get_column_impact, 
    get_pipeline_history, get_table_access, get_table_details, 
    get_schema_evolution, get_metadata_inventory
)
# Import enterprise extension tools
from .agent_tools_ext import (
    get_data_model_description, get_full_impact_analysis, generate_e2e_lineage_graph, get_business_context, get_holistic_entity_context
)

load_dotenv()

def run_real_agent(user_prompt: str):
    """Direct Groq API agent with robust error handling for malformed tool calls."""
    import groq as groq_lib
    import json

    client = groq_lib.Groq()  # reads GROQ_API_KEY from env automatically

    tools_map = {
        "get_table_lineage":           get_table_lineage,
        "generate_lineage_graph":      generate_lineage_graph,
        "get_column_impact":           get_column_impact,
        "get_pipeline_history":        get_pipeline_history,
        "get_table_access":            get_table_access,
        "get_table_details":           get_table_details,
        "get_schema_evolution":        get_schema_evolution,
        "get_metadata_inventory":      get_metadata_inventory,
        "get_data_model_description":  get_data_model_description,
        "get_full_impact_analysis":    get_full_impact_analysis,
        "generate_e2e_lineage_graph":  generate_e2e_lineage_graph,
        "get_business_context":        get_business_context,
        "get_holistic_entity_context": get_holistic_entity_context,
    }

    # Keep schema lean — Llama 3.3 generates malformed JSON with >5 complex tools
    groq_tools = [
        {"type": "function", "function": {
            "name": "get_holistic_entity_context",
            "description": "Get complete lineage, ETL status, audit history, and impact for any table, pipeline, source, or report. Call this first for any entity question.",
            "parameters": {"type": "object", "properties": {
                "entity_name": {"type": "string", "description": "Table, source, pipeline, or report name"}
            }, "required": ["entity_name"]}
        }},
        {"type": "function", "function": {
            "name": "generate_e2e_lineage_graph",
            "description": "Generate a focused, interactive lineage graph for a specific table showing sources, ETL pipelines, DW tables, and BI reports.",
            "parameters": {"type": "object", "properties": {
                "table_name": {"type": "string"}
            }, "required": ["table_name"]}
        }},
        {"type": "function", "function": {
            "name": "get_full_impact_analysis",
            "description": "Predict the impact of a proposed change (drop, rename, datatype change) on a table or column.",
            "parameters": {"type": "object", "properties": {
                "table_name":  {"type": "string"},
                "column_name": {"type": "string"},
                "change_type": {"type": "string", "enum": ["drop", "rename", "datatype"]}
            }, "required": ["table_name", "column_name"]}
        }},
        {"type": "function", "function": {
            "name": "get_pipeline_history",
            "description": "Get ETL pipeline execution history, run counts, failures, and retry details.",
            "parameters": {"type": "object", "properties": {
                "pipeline_name": {"type": "string"}
            }, "required": ["pipeline_name"]}
        }},
    ]

    system_msg = (
        "You are an Enterprise Data Lineage & Governance Specialist. "
        "For ANY question about a table, source, pipeline, or report — call `get_holistic_entity_context` first. "
        "Use `generate_e2e_lineage_graph` when asked to visualize. "
        "\n\nCRITICAL OUTPUT REQUIREMENT:\n"
        "When finalizing your text response AFTER tools have run, YOU MUST EXPLICITLY TRACE THE PATH in your text. "
        "Do NOT just say 'lineage was retrieved'. Instead, write a detailed walk: "
        "'[Source System] -> [Staging/File] -> [ETL Pipeline] -> [DW Table] -> [Downstream BI Reports]'. "
        "Explicitly list any ETL failure reasons, specific audit details, or broke down reports found in the tool output. "
        "Always structure your answer clearly into Upstream and Downstream stages for the user."
    )

    messages = [
        {"role": "system",  "content": system_msg},
        {"role": "user",    "content": user_prompt},
    ]


    def _invoke_tool(fn_name: str, args: dict) -> str:
        """Invoke a LangChain tool safely, handling both dict and string inputs."""
        tool = tools_map.get(fn_name)
        if not tool:
            return f"Unknown tool: {fn_name}"
        try:
            # Single-argument tools (e.g. entity_name, table_name) — pass the first value as string
            if len(args) == 1:
                return str(tool.run(list(args.values())[0]))
            elif args:
                return str(tool.run(args))
            else:
                return str(tool.run({}))
        except Exception as e:
            return f"Tool execution error [{fn_name}]: {e}"

    # Agentic loop — max 5 iterations
    for _ in range(5):
        try:
            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=messages,
                tools=groq_tools,
                tool_choice="auto",
                temperature=0,
            )
        except groq_lib.BadRequestError as e:
            # Llama produced a malformed tool call — retry as plain text (no tools)
            try:
                fallback = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=messages,
                    temperature=0.1,
                )
                text = fallback.choices[0].message.content or ""
                return text if text else run_mock_agent(user_prompt)
            except Exception:
                return run_mock_agent(user_prompt)
        except Exception as e:
            return f"Agent error: {e}\n\n" + run_mock_agent(user_prompt)

        msg = response.choices[0].message

        if not msg.tool_calls:
            return msg.content or "No response generated."

        # Append assistant turn with tool calls
        messages.append({
            "role": "assistant",
            "content": msg.content or "",
            "tool_calls": [
                {"id": tc.id, "type": "function",
                 "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                for tc in msg.tool_calls
            ]
        })

        # Execute each tool call
        for tc in msg.tool_calls:
            fn_name = tc.function.name
            try:
                args = json.loads(tc.function.arguments) if tc.function.arguments else {}
            except json.JSONDecodeError:
                args = {}
            result = _invoke_tool(fn_name, args)
            messages.append({"role": "tool", "content": result, "tool_call_id": tc.id})

    return "Agent completed analysis."





def run_mock_agent(user_prompt: str) -> str:
    """Enhanced mock agent router for demonstration purposes."""
    prompt_lower = user_prompt.lower()
    
    # Handle common typos from demo users
    prompt_lower = prompt_lower.replace("porducts", "product").replace("lienage", "lineage").replace("lieage", "lineage").replace("products", "product")
    
    # Tables in catalog
    target_tables = ["Dim_Customer", "Dim_Product", "Dim_Date", "Dim_Campaign", "Fact_Sales", "Dim_Store", "Dim_Employee", "Dim_Promotion", "Dim_Supplier", "Dim_Geography", "Fact_Inventory"]
    
    # Smart asset detection
    detected_asset = None
    if "product" in prompt_lower: detected_asset = "Dim_Product"
    elif "customer" in prompt_lower: detected_asset = "Dim_Customer"
    elif "sales" in prompt_lower: detected_asset = "Fact_Sales"
    elif "campaign" in prompt_lower: detected_asset = "Dim_Campaign"
    elif "store" in prompt_lower: detected_asset = "Dim_Store"
    elif "inventory" in prompt_lower: detected_asset = "Fact_Inventory"
    elif "supplier" in prompt_lower: detected_asset = "Dim_Supplier"
    elif "employee" in prompt_lower: detected_asset = "Dim_Employee"
    elif "promotion" in prompt_lower: detected_asset = "Dim_Promotion"
    elif "geography" in prompt_lower: detected_asset = "Dim_Geography"
    elif "date" in prompt_lower: detected_asset = "Dim_Date"

    # 2. Intent Routing
    if any(k in prompt_lower for k in ["tables", "catalog", "what is present", "what tables", "available", "inventory"]):
        if "target" in prompt_lower or "dw" in prompt_lower:
            return f"The Target Data Warehouse contains the following {len(target_tables)} tables: {', '.join(target_tables)}."
        return get_metadata_inventory.run({})

    elif any(k in prompt_lower for k in ["data model", "explain", "fact table", "dimension", "describe"]):
        target = detected_asset or "Fact_Sales"
        return get_data_model_description.run(target)

    elif any(k in prompt_lower for k in ["end to end", "e2e", "full flow", "source to target", "pipeline flow"]):
        target = detected_asset or "Fact_Sales"
        res = generate_e2e_lineage_graph.run(target)
        path = res.split("View it at: ")[1].strip() if "View it at: " in res else None
        tag = f"\n\n[Agent Automated Action]: {res}" if path else f"\n\n[Error]: {res}"
        return f"Generating end-to-end lineage for {target}...{tag}"

    elif any(k in prompt_lower for k in ["lineage", "trace", "flow", "reconciliation", "tell me about"]):
        target = detected_asset or "Fact_Sales"
        if "reconciliation" in prompt_lower or "delay" in prompt_lower or "failed" in prompt_lower:
            target = "OLTP_TO_DW_CUSTOMER"
        return get_holistic_entity_context.run(target)
        
    elif any(k in prompt_lower for k in ["details", "structure", "columns"]):
        if detected_asset:
            return get_table_details.run(detected_asset)
        return "Please specify a table (e.g., 'show columns for Dim_Product') to see structural details."
        
    elif any(k in prompt_lower for k in ["full impact", "report impact", "dashboard impact"]):
        ctype = "datatype" if any(x in prompt_lower for x in ["type", "datatype", "cast"]) else "drop"
        col = "price" if "price" in prompt_lower else "email" if "email" in prompt_lower else "quantity"
        tbl = detected_asset or "Dim_Product"
        return get_full_impact_analysis.run({"table_name": tbl, "column_name": col, "change_type": ctype})

    elif any(k in prompt_lower for k in ["impact", "drop", "delete", "change", "datatype", "modify"]):
        ctype = "datatype" if any(x in prompt_lower for x in ["type", "datatype", "cast"]) else "drop"
        if "price" in prompt_lower:
            return get_full_impact_analysis.run({"table_name": "Dim_Product", "column_name": "price", "change_type": ctype})
        elif "email" in prompt_lower:
            return get_full_impact_analysis.run({"table_name": "Dim_Customer", "column_name": "email", "change_type": ctype})
        return f"Please specify a column to analyze, e.g., 'What is the full impact of dropping a column in {detected_asset or 'Dim_Product'}?'"
        
    elif any(k in prompt_lower for k in ["access", "who", "audit", "permissions"]):
        target = detected_asset or "Fact_Sales"
        return get_table_access.run(target)
        
    elif any(k in prompt_lower for k in ["history", "pipeline", "etl", "results", "version"]):
        pl = "FLATFILE_TO_DW_SALES"
        if "customer" in prompt_lower: pl = "OLTP_TO_DW_CUSTOMER"
        return get_pipeline_history.run(pl)
        
    elif any(k in prompt_lower for k in ["schema", "evolution", "changes", "ddl"]):
        return get_schema_evolution.run({})
        
    elif any(k in prompt_lower for k in ["go ahead", "do this", "fix it", "execute"]):
        return "✅ **Action Simulated:** I have requested immediate execution from the Governance Engine. Because modifying ETL logic carries a Moderate/High impact score, an approval email has been routed to the Data Owner (`teja.jan220@gmail.com`). Once approved, the reconciliation job will restart dynamically."
        
    elif any(k in prompt_lower for k in ["context", "issues", "why", "past", "reason", "historical"]):
        return get_business_context.run(user_prompt)
        
    else:
        return ("I can help with: 📊 Data Model exploration, 🗺️ Lineage tracing, ⚙️ E2E pipeline flow, "
                "🔴 Full impact analysis (tables, ETL, reports), 📋 ETL history, 🕵️ Audit trails. "
                "Try: 'Explain the data model for Fact_Sales' or 'Show full impact of dropping price.'")

def main():
    print("=====================================================")
    print(" Data Lineage & Impact Intelligence Agent Started ")
    print("=====================================================\n")
    
    has_api_key = bool(os.getenv("GROQ_API_KEY"))
    
    if not has_api_key:
        print(">> Warning: GROQ_API_KEY not found in environment (.env). Using Mock Agent Router for Demo. <<\n")
        
    while True:
        try:
            prompt = input("\nAgent Prompt > ")
            if prompt.strip().lower() in ['exit', 'quit']:
                break
                
            if not prompt.strip():
                continue
                
            if has_api_key:
                response = run_real_agent(prompt)
            else:
                response = run_mock_agent(prompt)
                
            print("\nAgent Response:\n")
            print(response)
            print("-" * 50)
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error executing agent: {str(e)}")

if __name__ == "__main__":
    main()
