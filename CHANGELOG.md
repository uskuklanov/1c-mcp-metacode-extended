# Changelog

##### v2.1.0-extended - 2026-07-24
**Fork of [ROCTUP/1c-mcp-metacode](https://github.com/ROCTUP/1c-mcp-metacode) maintained by [uskuklanov](https://github.com/uskuklanov).**

5 new MCP tools added in `app/mcpsrv/extended_tools.py`:

- **`cypher_query(query, params?, limit?, timeout_sec?, project_name?)`** — gated raw Cypher execution against Neo4j HTTP API. Write operations (CREATE/MERGE/DELETE/SET/REMOVE/DROP/apoc.write) are blocked by regex filter. Auto-appends LIMIT if missing. Pass `$project_name` and the tool fills it in from the current PROJECT_NAME env var. Returns `{columns, rows, row_count, truncated, execution_ms}`. Works across all 42 metadata categories and any 1C project.

- **`batch_dependency_resolve(attribute_name?, object_filter?, routine_filter?, relationship?, from_side?, to_side?, depth?, limit?, project_name?)`** — high-level graph JOIN with regex filters. Default is `FormControl -[BINDS_TO]-> Attribute` (works for any category that has attributes, all 42). Variadic path `*1..depth` (depth=1..3). For categories without FormControl (Константы, РегламентныеЗадания) use `from_side='Attribute'`. Returns `{count, truncated, relationship, from_side, to_side, depth, rows: [{from_name, from_qn, to_name, to_qn, via_relationship}]}`.

- **`routine_subgraph(routine_id, callee_name_filter?, callee_owner_filter?, direction?, depth?, limit?, project_name?)`** — routine call subgraph with regex filters on callee/caller name and owner_qn. direction=callees|callers|both, depth=1..3. routine_id is 40-char SHA. Returns `{count_nodes, count_edges, direction, depth, nodes, edges}`.

- **`reverse_callers(routine_name, routine_owner_filter?, caller_owner_filter?, limit?, project_name?)`** — find callers of a routine by name, with owner_qn regex filters. Returns 0 rows for event-handler routines like `ОбработкаПроведения` (they are wired through event subscriptions, not CALLS) — by design.

- **`form_binding_summary(object_name, form_name?, project_name?)`** — per-form aggregate: total controls, bound, unbound, bound %. Works for any of the 42 metadata categories via `'<Категория>.<Имя>'`. Categories without Form return a valid empty result with an explanatory note (NOT an error). Uses `[:HAS_CONTROL|HAS_CHILD*0..]` to walk the entire control tree. Returns `{object_name, category, qualified_name, forms: [{form_name, total, bound, unbound, bound_pct}], note?}`.

**Multi-project portability (env-driven, no code changes):**
- `PROJECT_NAME` (env) — required, used as prefix in Cypher filters via `$config_prefix`.
- `NEO4J_HTTP_URL` (env, optional) — defaults to derivation from `NEO4J_URI` (e.g. `bolt://neo4j:7687` → `http://neo4j:7474`).
- `NEO4J_PASSWORD` (env or `.env`) — for P1 basic auth.
- `CONFIG_NAME` (env, optional) — defaults to `УправлениеНебольшойФирмой` (matches УНФ; override for other projects).

**Other changes:**
- `app/mcpsrv/server.py`: 3-line patch in `_register_tools()` to import and call `extended_tools.register_extended_tools(mcp)`.
- `app/graphdb/bsl_code_split.py`: bake the Phase A infinite-loop fix into the image (was a volume-overlay fix in unf-metacode's docker-compose.yml).

**Build / deploy:**
```bash
# In your 1C project (e.g. unf):
cd tools/1c-mcp-metacode-fork
docker compose -f ../1c-mcp-metacode/docker-compose.yml -p <project>-metacode build metacode
docker compose -f ../1c-mcp-metacode/docker-compose.yml -p <project>-metacode up -d metacode
```

**Verified on unf (2026-07-24):** all 5 tools return real data. P5 tested across 22 categories (11 with forms, 11 without) — all return valid responses. P1 write-block tested with 5 dangerous queries — all blocked.

##### v2.1.1-extended - 2026-07-25
**Bugfixes and refinements (8 items from report §5.1 + P3 Cypher fix):**

- **P1 `cypher_query`**: LIMIT auto-append now only fires when the query has at least one `RETURN` clause — no more false appends on `WITH`-only or CALL subqueries.
- **P2 `batch_dependency_resolve`**: auto-reduces limit to 20 when neither `attribute_name` nor `object_filter` is set (prevents 16K-row surprises). Adds contextual `note` on broad or truncated results. Returns `note` with relationship hints when 0 rows for non-FormControl pairs.
- **P3 `routine_subgraph`**: `routine_id` is now validated as a 40-character SHA-1 hex string before interpolation — eliminates Cypher injection risk and redundant escaping. Fixed `->` and `-` after `]` in relationship patterns that were broken during the refactor.
- **P4 `reverse_callers`**: adds explanatory `note` when 0 rows are returned, directing users to `get_event_subscriptions` / `find_dependency_paths` for event-handler routines.
- **P5 `form_binding_summary`**: contextual notes for categories like ОбщиеМодули, Подсистемы, Константы, РегламентныеЗадания, Роли with tool-specific suggestions.

##### v2.2.0-extended - 2026-07-25
**LLM-friendly tool signatures (Annotated + dict returns):**

- **All 5 extended tools**: return `Dict[str, Any]` instead of `json.dumps(str)` — structured content arrives as native dict, no manual JSON parsing needed. The LLM sees `structuredContent` fields directly.
- **All 5 extended tools**: every parameter now has `Annotated[type, "description"]` — the description is visible in `tools/list` `inputSchema`, helping the LLM understand parameter format (what regex syntax, what Category.Name looks like, etc.).
- **All 22 typed tools**: ~150 parameters now have `Annotated[type, "description"]` annotations covering format, mode options, comparison types, and concrete examples.
- **P3 `direction`**: enum-like documentation in parameter description (callees/callers/both).
- **P5 `form_binding_summary`**: rich example in parameter docs for `object_name`.
- **Tool docstrings in extended_tools**: expanded with `USE WHEN` sections and concrete examples.

##### v2.0.0 - 2026-07-12
- Инструменты поиска метаданных полностью переработаны: вместо трёх инструментов
  (`search_metadata` со свободным запросом и генерацией Cypher по шаблону или через LLM,
  `search_metadata_by_description`, `search_code`) — 22 типизированных инструмента
  с явными JSON Schema параметрами. Режим генерации Cypher через LLM и шаблонный
  payload-режим убраны.
- Появилась возможность искать не только прямые связи объекта, а многошаговые пути зависимостей
  между произвольными узлами графа — метаданными, элементами, формами и вызовами кода BSL
  (`find_dependency_paths`).
- Добавлена поддержка расширений 1С: объекты расширений загружаются в общий граф проекта вместе с
  базовой конфигурацией, со связями base ↔ extension, и сравнением объектов (`get_extension_object_diff`).
- Добавлен семантический поиск по телу кода BSL (`search_bsl_code`) с двухфазной индексацией и
  лексическим (RLM) режимом.
- Добавлен слой AI-сводок объектов (object summary) и поиск объектов по сводкам
  (`find_objects_by_summary`).
- Добавлена веб-консоль (просмотр метаданных, форм, кода, статистики, управление видимостью MCP-tools)
  и встроенный AI агент с профилями моделей и подключением внешних MCP-серверов.
- Добавлена инкрементальная загрузка с планировщиком и full reconcile (обновление только изменений
  вместо полной перезагрузки).
- Добавлена загрузка метаданных напрямую из XML-дампа (`METADATA_SOURCE=xml`).
- Добавлена компактизация ответов: повторяющиеся значения (qn, config_name, категории, типы,
  права) заменяются символьными ссылками (@qn:N, @config:N, ...) для экономии токенов
  (`response_compact_refs`).
- Добавлена возможность подключить внешнюю rerank-модель для переранжирования результатов
  семантического поиска (по описаниям, коду и сводкам) — независимо включается для каждого вида поиска.
- Добавлен формат ответа toon (наряду с существовавшими text/json), выбран форматом по умолчанию.
- Добавлена полная документация в каталоге `docs/`.

##### v1.3.0 - 2025-10-29
- Добавлена поддержка загрузки процедур/функций из модуля формы обычных форм (файлы Form.bin).
- Улучшена скорость загрузки (примерно в 2 раза) путем распараллеливания процесса загрузки на несколько ядер.
- Добавлена векторная индексация описаний процедур/функций, описаний метаданных, а так же гибридный поиск по этим описаниям.

##### v1.2.0 - 2025-10-20
- Добавлена загрузка тела процедур и функций, а также их описаний (комментарии над сигнатурой).
- Возможность полнотекстового поиска процедур и функций по описанию.
- Возможность получения тела процедуры/функции прямо из графовой базы.
- Добавлена поддержка загрузки справки по объектам метаданным с возможностью полнотекстового поиска объектов метаданных по этой справке, а так же по другим описательным полям.

##### v1.1.0 - 2025-10-07
- Добавлена поддержка загрузки подписок на события
- Добавлена поддержка загрузки модулей с процедурами/функциями и формирования графа вызовов

##### v1.0.0 - 2025-09-30
- Первоначальный релиз с загрузкой метаданных 1С в Neo4j
- Поддержка MCP инструментов для поиска метаданных
