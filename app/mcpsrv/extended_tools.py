"""
Extended MCP tools for 1c-mcp-metacode.

Five new tools that complement the typed_tools.py inventory:
  P1 cypher_query               — gated raw Cypher
  P2 batch_dependency_resolve   — high-level JOIN across all categories
  P3 routine_subgraph           — routine call graph with regex filters
  P4 reverse_callers            — who calls a routine, with owner filter
  P5 form_binding_summary       — per-form bound/unbound control counts

All five work across all 42 metadata categories and across multiple 1C
projects (parameterised through env vars PROJECT_NAME / NEO4J_HTTP_URL
/ NEO4J_PASSWORD / CONFIG_NAME, never hard-coded).

Entry point: register_extended_tools(mcp). Called from server._register_tools().
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

from config import settings

from .queries import _run_query as _run_query_loader
from .resolvers import resolve_object_ref
from .typed_tools import _init_loader, _resolve_project

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers (env-driven, never hard-coded per-project)
# ---------------------------------------------------------------------------

def _project_name() -> str:
    """Current project name. Reads PROJECT_NAME env var (set by docker-compose)."""
    return os.environ.get("PROJECT_NAME", "unf")


def _neo4j_http_url() -> str:
    """HTTP endpoint of Neo4j for raw Cypher (P1).

    Inside Docker: defaults to http://neo4j:7474 (the in-network service).
    On the host:    set NEO4J_HTTP_URL=http://localhost:6004 (mapped port).

    P2-P5 use the in-process GraphDatabaseLoader (bolt), not HTTP, so they
    don't care about this URL.
    """
    env_url = os.environ.get("NEO4J_HTTP_URL")
    if env_url:
        return env_url
    # Inside Docker: resolve NEO4J_URI's hostname to its HTTP sibling.
    uri = getattr(settings, "neo4j_uri", "") or ""
    if uri.startswith("bolt://"):
        host = uri[len("bolt://"):].split(":", 1)[0]
        if host and host not in ("", "localhost", "127.0.0.1"):
            return f"http://{host}:7474"
    return "http://localhost:7474"


def _neo4j_password() -> str:
    return os.environ.get("NEO4J_PASSWORD", "") or getattr(settings, "neo4j_password", "") or ""


def _config_prefix(project_name: Optional[str] = None) -> str:
    """Build qualified_name prefix like 'unf/УправлениеНебольшойФирмой/'.

    Uses env PROJECT_NAME (or override) and CONFIG_NAME (default for УНФ).
    Every Cypher filter that needs to scope by project should use this via
    the $config_prefix parameter — never literal 'unf/' or 'erp/' strings.
    """
    pn = project_name or _project_name()
    cfg = os.environ.get("CONFIG_NAME", "УправлениеНебольшойФирмой")
    return f"{pn}/{cfg}/"


def _safe_filename(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", name)[:80]


# ---------------------------------------------------------------------------
# P1 — gated raw Cypher
# ---------------------------------------------------------------------------

FORBIDDEN_PATTERNS = re.compile(
    r"\b("
    r"CREATE|MERGE|DELETE|SET|REMOVE|DROP|"
    r"CALL\s+apoc\.(?:periodic|schema|refactor|graph)\b"
    r")\b",
    re.IGNORECASE,
)

MAX_LIMIT = 1000


def _http_run_cypher(query: str, params: Dict[str, Any], timeout_sec: int) -> Dict[str, Any]:
    """Run a read-only Cypher query via Neo4j HTTP API. Used by P1 only.

    P2-P5 use _run_query(loader, ...) which goes through the existing
    GraphDatabaseLoader path (bolts, with project_name injection).
    """
    url = f"{_neo4j_http_url()}/db/neo4j/tx/commit"
    creds = base64.b64encode(("neo4j:" + _neo4j_password()).encode()).decode()
    body = json.dumps(
        {"statements": [{"statement": query, "parameters": params}]}
    ).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {creds}",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if payload.get("errors"):
        errs = payload["errors"]
        msg = "; ".join(e.get("message", str(e)) for e in errs)
        raise RuntimeError(f"Neo4j error: {msg}")
    results = payload.get("results", [])
    if not results:
        return {"columns": [], "rows": [], "row_count": 0}
    columns = results[0].get("columns", [])
    rows = [row.get("row", []) for row in results[0].get("data", [])]
    return {"columns": columns, "rows": rows, "row_count": len(rows)}


def _register_cypher_query(mcp):
    def cypher_query(
        query: str,
        params: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        timeout_sec: int = 30,
        project_name: Optional[str] = None,
    ) -> str:
        """Execute a READ-ONLY Cypher query against Neo4j.

        Write operations are blocked (CREATE / MERGE / DELETE / SET /
        REMOVE / DROP / apoc.write). If a query has no LIMIT clause,
        one is appended automatically up to `limit` (default 100, max 1000).

        Pass `$project_name` inside the query and it will be filled in
        from the current PROJECT_NAME env var (or the override you pass).

        Returns JSON: {"columns":[...], "rows":[[...]], "row_count": N,
                       "truncated": bool, "execution_ms": int}.
        """
        params = dict(params or {})
        try:
            if FORBIDDEN_PATTERNS.search(query):
                return json.dumps(
                    {
                        "error": "Write operations are blocked in cypher_query. "
                        "Allowed: MATCH/RETURN/WITH/UNWIND/OPTIONAL/CALL (non-write apoc). "
                        "Use the dedicated write endpoints or direct Neo4j access for mutations."
                    },
                    ensure_ascii=False,
                )
            try:
                lim = max(1, min(MAX_LIMIT, int(limit)))
            except Exception:
                lim = 100
            # Only append LIMIT if the query has at least one RETURN clause
            # and none of the RETURN branches already has LIMIT.
            if "RETURN" in query.upper():
                parts = query.upper().split("RETURN")
                last_part = parts[-1].strip()
                if "LIMIT" not in last_part:
                    query = query.rstrip(";").rstrip() + f" LIMIT {lim}"
            if "$project_name" in query and "project_name" not in params:
                params["project_name"] = project_name or _project_name()
            try:
                timeout_eff = max(1, min(120, int(timeout_sec)))
            except Exception:
                timeout_eff = 30
            t0 = time.time()
            out = _http_run_cypher(query, params, timeout_eff)
            out["truncated"] = out["row_count"] >= lim
            out["execution_ms"] = int((time.time() - t0) * 1000)
            return json.dumps(out, ensure_ascii=False, default=str)
        except urllib.error.URLError as e:
            return json.dumps({"error": f"Neo4j HTTP error: {e}"}, ensure_ascii=False)
        except Exception as e:
            logger.exception("cypher_query failed")
            return json.dumps({"error": f"cypher_query failed: {e}"}, ensure_ascii=False)

    cypher_query.__doc__ = """Execute a READ-ONLY Cypher query against Neo4j. Write ops are blocked. Pass `$project_name` to scope by current PROJECT_NAME env."""
    mcp.tool()(cypher_query)


# ---------------------------------------------------------------------------
# P2 — batch_dependency_resolve
# ---------------------------------------------------------------------------

def _register_batch_dependency_resolve(mcp):
    def batch_dependency_resolve(
        attribute_name: Optional[str] = None,
        object_filter: Optional[str] = None,
        routine_filter: Optional[str] = None,
        relationship: str = "BINDS_TO",
        from_side: str = "FormControl",
        to_side: str = "Attribute",
        depth: int = 1,
        limit: int = 100,
        project_name: Optional[str] = None,
    ) -> str:
        """Resolve a batch of graph joins in one call.

        Default: find all FormControl -[BINDS_TO]-> Attribute/FormAttribute
        pairs (works for any category that has attributes — that's all 42).

        Filters:
          - attribute_name : exact match on target attribute name
          - object_filter  : regex on owner qualified_name (e.g. '^.*Расходная.*$')
          - routine_filter : regex on routine name (when routine/routine-side used)
          - relationship   : any relationship type (BINDS_TO, CALLS, USED_IN, ...)
          - from_side / to_side : any node label (FormControl, Attribute, Routine...)
          - depth          : 1 hop (default) or more for multi-step joins
          - limit          : safety cap (default 100, max 1000)

        Returns JSON array of {from_name, from_qn, to_name, to_qn, via_relationship}.
        """
        loader = _init_loader()
        if loader is None:
            return json.dumps({"error": "Neo4j connection not available."}, ensure_ascii=False)
        try:
            pn = _resolve_project(project_name)
        except Exception as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)
        try:
            lim = max(1, min(MAX_LIMIT, int(limit)))
        except Exception:
            lim = 100
        
        # Auto-reduce limit for broad queries (no object_filter) to prevent
        # overwhelming the response with thousands of rows.
        broad_query = (not attribute_name and not object_filter)
        if broad_query and lim > 20:
            lim = 20
        try:
            depth_eff = max(1, min(3, int(depth)))
        except Exception:
            depth_eff = 1

        cypher = """
MATCH (from:""" + from_side + """)-[rels:""" + relationship + """*1..""" + str(depth_eff) + """]->(to:""" + to_side + """)
WHERE (from.qualified_name STARTS WITH $config_prefix OR from.owner_qn STARTS WITH $config_prefix)
  AND (to.qualified_name STARTS WITH $config_prefix OR to.owner_qn STARTS WITH $config_prefix
       OR to.project_name = $project_name)
  AND ($attr_name IS NULL OR to.name = $attr_name)
  AND ($obj_filter IS NULL OR from.owner_qn =~ $obj_filter OR from.qualified_name =~ $obj_filter)
  AND ($rt_filter  IS NULL OR to.name =~ $rt_filter)
WITH from, to, rels
UNWIND rels AS r
RETURN from.name AS from_name, from.qualified_name AS from_qn,
       to.name AS to_name, to.qualified_name AS to_qn,
       type(r) AS via_relationship
LIMIT $limit
"""
        try:
            rows = _run_query_loader(
                loader,
                cypher,
                {
                    "config_prefix": _config_prefix(pn),
                    "project_name": pn,
                    "attr_name": attribute_name,
                    "obj_filter": object_filter,
                    "rt_filter": routine_filter,
                    "limit": lim,
                },
                pn,
            ) or []
            out = {
                "count": len(rows),
                "truncated": len(rows) >= lim,
                "relationship": relationship,
                "from_side": from_side,
                "to_side": to_side,
                "depth": depth_eff,
                "rows": rows,
            }
            # Add user guidance for broad or truncated results
            notes = []
            if broad_query:
                notes.append(
                    "Broad query (no attribute_name, no object_filter). "
                    "Pass attribute_name= or object_filter= to narrow results."
                )
            if out["truncated"]:
                notes.append(
                    f"Result truncated at {lim} rows. "
                    "Add object_filter= to drill into a specific object or category."
                )
            if notes:
                out["note"] = " | ".join(notes)
            # When 0 rows and both sides are non-FormControl, hint about available edge types.
            if len(rows) == 0 and from_side != "FormControl":
                zero_note = (
                    "Zero rows. The relationship '"
                    + relationship + "' may not exist between " + from_side
                    + " and " + to_side + " in this graph. "
                    "Try relationship='USED_IN', 'CALLS', or 'DO_MOVEMENTS_IN'."
                )
                if "note" in out:
                    out["note"] += " | " + zero_note
                else:
                    out["note"] = zero_note
            return json.dumps(out, ensure_ascii=False, default=str)
        except Exception as e:
            logger.exception("batch_dependency_resolve failed")
            return json.dumps({"error": str(e)}, ensure_ascii=False)

    batch_dependency_resolve.__doc__ = """Resolve a batch of graph joins in one call. Works on all 42 categories. Default = FormControl→Attribute BINDS_TO. Filters: attribute_name (exact), object_filter / routine_filter (regex)."""
    mcp.tool()(batch_dependency_resolve)


# ---------------------------------------------------------------------------
# P3 — routine_subgraph
# ---------------------------------------------------------------------------

def _register_routine_subgraph(mcp):
    def routine_subgraph(
        routine_id: str,
        callee_name_filter: Optional[str] = None,
        callee_owner_filter: Optional[str] = None,
        direction: str = "callees",
        depth: int = 1,
        limit: int = 50,
        project_name: Optional[str] = None,
    ) -> str:
        """Routine call subgraph with regex filters.

        direction ∈ {callees, callers, both}. Filters apply to the
        far end of the relationship. depth=1 is default (immediate
        callers/callees); raise to 2-3 for broader subgraphs but be
        mindful of result-set size (298K routines in unf).

        Returns JSON: {nodes:[{id,name,owner_qn}], edges:[{from,to,rel}], count}.
        """
        loader = _init_loader()
        if loader is None:
            return json.dumps({"error": "Neo4j connection not available."}, ensure_ascii=False)
        try:
            pn = _resolve_project(project_name)
        except Exception as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)
        try:
            lim = max(1, min(MAX_LIMIT, int(limit)))
        except Exception:
            lim = 50
        try:
            depth_eff = max(1, min(3, int(depth)))
        except Exception:
            depth_eff = 1
        if direction not in ("callees", "callers", "both"):
            return json.dumps(
                {"error": f"direction must be one of callers/callees/both, got '{direction}'"},
                ensure_ascii=False,
            )

        # Validate routine_id: Neo4j requires literal interpolation here because
        # parameter maps in MATCH path patterns are not supported. We whitelist
        # SHA-1 hex IDs (40 characters) to prevent Cypher injection.
        _SHA_RE = re.compile(r"^[0-9a-f]{40}$", re.I)
        if not _SHA_RE.match(routine_id):
            return json.dumps(
                {"error": f"routine_id must be a 40-character SHA-1 hex string, got '{routine_id[:20]}...'"},
                ensure_ascii=False,
            )

        # direction = forward (callees) or backward (callers). For both we
        # run two queries and merge; Cypher's variadic path patterns don't
        # trivially support both without union, so keep it explicit.
        edges = []
        nodes = []
        try:
            if direction in ("callees", "both"):
                cypher = """\
MATCH path = (start:Routine {id:'""" + routine_id + """'})-[:CALLS*1..""" + str(depth_eff) + """\](end:Routine)
WHERE (start.owner_qn STARTS WITH $config_prefix OR $config_prefix = '')
  AND ($name_filter IS NULL OR end.name =~ $name_filter)
  AND ($owner_filter IS NULL OR end.owner_qn =~ $owner_filter)
WITH collect(DISTINCT path) AS paths
UNWIND paths AS p
WITH p,
     [n IN nodes(p) | {id:n.id, name:n.name, owner_qn:n.owner_qn}] AS ns_list,
     [r IN relationships(p) | {from: startNode(r).name, to: endNode(r).name, rel: type(r)}] AS rs_list
RETURN ns_list, rs_list
LIMIT $limit
"""
                rows = _run_query_loader(
                    loader,
                    cypher,
                    {
                        "config_prefix": _config_prefix(pn),
                        "name_filter": callee_name_filter,
                        "owner_filter": callee_owner_filter,
                        "depth": depth_eff,
                        "limit": lim,
                    },
                    pn,
                ) or []
                for r in rows:
                    edges.extend(r.get("rs_list") or [])
                    nodes.extend(r.get("ns_list") or [])
            if direction in ("callers", "both"):
                cypher_back = """\
MATCH path = (start:Routine {id:'""" + routine_id + """'})<-[:CALLS*1..""" + str(depth_eff) + """\](end:Routine)
WHERE (start.owner_qn STARTS WITH $config_prefix OR $config_prefix = '')
  AND ($name_filter IS NULL OR end.name =~ $name_filter)
  AND ($owner_filter IS NULL OR end.owner_qn =~ $owner_filter)
WITH collect(DISTINCT path) AS paths
UNWIND paths AS p
WITH p,
     [n IN nodes(p) | {id:n.id, name:n.name, owner_qn:n.owner_qn}] AS ns_list,
     [r IN relationships(p) | {from: startNode(r).name, to: endNode(r).name, rel: type(r)}] AS rs_list
RETURN ns_list, rs_list
LIMIT $limit
"""
                rows = _run_query_loader(
                    loader,
                    cypher_back,
                    {
                        "config_prefix": _config_prefix(pn),
                        "name_filter": callee_name_filter,
                        "owner_filter": callee_owner_filter,
                        "depth": depth_eff,
                        "limit": lim,
                    },
                    pn,
                ) or []
                for r in rows:
                    edges.extend(r.get("rs_list") or [])
                    nodes.extend(r.get("ns_list") or [])
            # Dedup by id / (from,to,rel)
            seen_n = set()
            unique_nodes = []
            for n in nodes:
                if n.get("id") and n["id"] not in seen_n:
                    seen_n.add(n["id"])
                    unique_nodes.append(n)
            seen_e = set()
            unique_edges = []
            for e in edges:
                key = (e.get("from"), e.get("to"), e.get("rel"))
                if key not in seen_e:
                    seen_e.add(key)
                    unique_edges.append(e)
            out = {
                "count_nodes": len(unique_nodes),
                "count_edges": len(unique_edges),
                "direction": direction,
                "depth": depth_eff,
                "nodes": unique_nodes[:lim],
                "edges": unique_edges[:lim],
            }
            return json.dumps(out, ensure_ascii=False, default=str)
        except Exception as e:
            logger.exception("routine_subgraph failed")
            return json.dumps({"error": str(e)}, ensure_ascii=False)

    routine_subgraph.__doc__ = """Routine call subgraph with regex filters on callee/caller name and owner_qn. direction=callees|callers|both, depth=1..3."""
    mcp.tool()(routine_subgraph)


# ---------------------------------------------------------------------------
# P4 — reverse_callers
# ---------------------------------------------------------------------------

def _register_reverse_callers(mcp):
    def reverse_callers(
        routine_name: str,
        routine_owner_filter: Optional[str] = None,
        caller_owner_filter: Optional[str] = None,
        limit: int = 50,
        project_name: Optional[str] = None,
    ) -> str:
        """Find callers of routines named `routine_name`, optionally filtered
        by routine owner_qn (e.g. '^.*РасходнаяНакладная$') and caller
        owner_qn (regex).

        Note: routine handlers like `ОбработкаПроведения` are wired through
        event subscriptions, not CALLS — so this tool returns 0 rows for
        them by design. Use find_dependency_paths for those cases.

        Returns JSON array of {caller_name, caller_owner, caller_id, called_name, called_owner}.
        """
        loader = _init_loader()
        if loader is None:
            return json.dumps({"error": "Neo4j connection not available."}, ensure_ascii=False)
        try:
            pn = _resolve_project(project_name)
        except Exception as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)
        try:
            lim = max(1, min(MAX_LIMIT, int(limit)))
        except Exception:
            lim = 50

        cypher = """
MATCH (called:Routine)
WHERE called.name = $routine_name
  AND (called.owner_qn STARTS WITH $config_prefix OR $config_prefix = '')
  AND ($routine_owner IS NULL OR called.owner_qn =~ $routine_owner)
MATCH (called)<-[:CALLS]-(caller:Routine)
WHERE ($caller_owner IS NULL OR caller.owner_qn =~ $caller_owner)
RETURN caller.id AS caller_id,
       caller.name AS caller_name,
       caller.owner_qn AS caller_owner,
       called.name AS called_name,
       called.owner_qn AS called_owner
LIMIT $limit
"""
        try:
            rows = _run_query_loader(
                loader,
                cypher,
                {
                    "config_prefix": _config_prefix(pn),
                    "routine_name": routine_name,
                    "routine_owner": routine_owner_filter,
                    "caller_owner": caller_owner_filter,
                    "limit": lim,
                },
                pn,
            ) or []
            out = {
                "count": len(rows),
                "routine_name": routine_name,
                "rows": rows,
            }
            # Hint when zero rows: event-handler routines are wired through
            # subscriptions, not CALLS edges, so they naturally return 0.
            if len(rows) == 0:
                out["note"] = (
                    "Zero rows returned. Event-handler routines (ОбработкаПроведения, "
                    "ПередЗаписью, etc.) are wired through event subscriptions, not CALLS "
                    "edges — this is expected. Use get_event_subscriptions or "
                    "find_dependency_paths to trace their callers."
                )
            return json.dumps(out, ensure_ascii=False, default=str)
        except Exception as e:
            logger.exception("reverse_callers failed")
            return json.dumps({"error": str(e)}, ensure_ascii=False)

    reverse_callers.__doc__ = """Find routines that CALL routines named `routine_name`. Filters by owner_qn regex (e.g. '^.*РасходнаяНакладная$'). Returns 0 for event-handler routines (by design)."""
    mcp.tool()(reverse_callers)


# ---------------------------------------------------------------------------
# P5 — form_binding_summary
# ---------------------------------------------------------------------------

def _register_form_binding_summary(mcp):
    def form_binding_summary(
        object_name: str,
        form_name: Optional[str] = None,
        project_name: Optional[str] = None,
    ) -> str:
        """Per-form aggregate: total controls, bound, unbound, bound %.

        `object_name` accepts any of the 42 metadata categories:
          'Документы.РасходнаяНакладная'
          'Справочники.Контрагенты'
          'Константы.АвтоПодборНомеровГТД'   (no form, returns empty + note)
          'Подсистемы.Продажи'                (no form, returns empty + note)
          'MCP_Сервер$ext$.Обработки.<X>'     (extension objects)

        Categories without Form return a valid empty result with an
        explanatory note — no exception.

        Returns JSON: {object_name, category, forms:[{form_name, total, bound, unbound, bound_pct}], note?}.
        """
        loader = _init_loader()
        if loader is None:
            return json.dumps({"error": "Neo4j connection not available."}, ensure_ascii=False)
        try:
            pn = _resolve_project(project_name)
        except Exception as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        try:
            resolved = resolve_object_ref(loader, object_name, pn, None)
        except ValueError as e:
            return json.dumps({"error": f"object not found: {e}"}, ensure_ascii=False)

        category = resolved.get("category_name", "")
        qn = resolved.get("qualified_name", "")
        if not qn:
            return json.dumps(
                {"error": f"Could not resolve '{object_name}' to a qualified_name"},
                ensure_ascii=False,
            )

        # Step 1: does this object have any Form nodes?
        cypher_forms = """
MATCH (f:Form)
WHERE f.qualified_name STARTS WITH $qn_prefix
  AND ($form_name IS NULL OR f.name = $form_name)
RETURN f.name AS form_name, f.qualified_name AS form_qn
ORDER BY f.name
"""
        try:
            forms = _run_query_loader(
                loader,
                cypher_forms,
                {"qn_prefix": qn + "/Form/", "form_name": form_name},
                pn,
            ) or []
        except Exception as e:
            logger.exception("form_binding_summary form lookup failed")
            return json.dumps({"error": str(e)}, ensure_ascii=False)

        if not forms:
            # Contextual suggestions based on category
            cat_lower = category.lower()
            suggestions = {
                "общиемодули": "Use get_bsl_modules / search_bsl_routines to inspect routines.",
                "подсистемы": "Use get_metadata_object_structure to inspect subsystem contents.",
                "константы": "Use batch_dependency_resolve (P2) to find attribute usages.",
                "регламентныезадания": "Use get_event_subscriptions to find handled events.",
                "роли": "Use get_access_rights to inspect role permissions.",
            }
            hint = suggestions.get(cat_lower.replace(" ", ""),
                "For non-form analytics use batch_dependency_resolve (P2) "
                "or routine_subgraph (P3).")
            return json.dumps(
                {
                    "object_name": object_name,
                    "category": category,
                    "forms": [],
                    "note": (
                        f"Category '{category}' has no Form nodes for object "
                        f"'{resolved.get('name', object_name)}'. "
                        + hint
                    ),
                },
                ensure_ascii=False,
            )

        # Step 2: per-form aggregate controls and bindings.
        cypher_aggregate = """
MATCH (f:Form {qualified_name:$form_qn})
MATCH (f)-[:HAS_CONTROL|HAS_CHILD*0..]->(ctrl:FormControl)
OPTIONAL MATCH (ctrl)-[:BINDS_TO]->(attr)
WITH f,
     count(DISTINCT ctrl) AS total,
     count(DISTINCT CASE WHEN attr IS NOT NULL THEN ctrl END) AS bound
RETURN f.name AS form_name, total, bound,
       total - bound AS unbound,
       CASE WHEN total > 0 THEN round(100.0 * bound / total, 1) ELSE 0 END AS bound_pct
"""
        out_forms = []
        for fr in forms:
            try:
                agg = _run_query_loader(
                    loader,
                    cypher_aggregate,
                    {"form_qn": fr["form_qn"]},
                    pn,
                ) or []
                if agg:
                    out_forms.append(agg[0])
                else:
                    out_forms.append(
                        {
                            "form_name": fr["form_name"],
                            "form_qn": fr["form_qn"],
                            "total": 0,
                            "bound": 0,
                            "unbound": 0,
                            "bound_pct": 0,
                        }
                    )
            except Exception as e:
                logger.exception("form_binding_summary aggregate failed for %s", fr["form_qn"])
                out_forms.append(
                    {
                        "form_name": fr["form_name"],
                        "form_qn": fr["form_qn"],
                        "error": str(e),
                    }
                )

        return json.dumps(
            {
                "object_name": object_name,
                "category": category,
                "qualified_name": qn,
                "forms": out_forms,
            },
            ensure_ascii=False,
            default=str,
        )

    form_binding_summary.__doc__ = """Per-form bound/unbound control counts. Works for all 42 categories; categories without Form return valid empty result with note. object_name accepts 'Category.Name' for any of the 42 categories."""
    mcp.tool()(form_binding_summary)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def register_extended_tools(mcp) -> None:
    """Register all 5 extended tools with the FastMCP server instance.

    Called from app/mcpsrv/server.py:_register_tools() once at startup.
    """
    _register_cypher_query(mcp)
    _register_batch_dependency_resolve(mcp)
    _register_routine_subgraph(mcp)
    _register_reverse_callers(mcp)
    _register_form_binding_summary(mcp)
    logger.info(
        "Extended MCP tools registered (project=%s, neo4j_http=%s)",
        _project_name(),
        _neo4j_http_url(),
    )
