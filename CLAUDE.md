# CodeGraph Usage Guide for AI Assistants

This project uses CodeGraph for intelligent code navigation and analysis. CodeGraph maintains a knowledge graph of the codebase that helps understand code structure, dependencies, and relationships.

## Available Commands

All commands use the installed codegraph at `~/.codegraph/current/bin/codegraph`

### Core Operations

#### `codegraph status`
**When to use:** Before any analysis to check if the index is up to date
```bash
~/.codegraph/current/bin/codegraph status
```
Shows index statistics, file counts, and whether the index needs updating.

#### `codegraph sync`
**When to use:** After code changes to update the index incrementally
```bash
~/.codegraph/current/bin/codegraph sync
```
Fast incremental update - only re-indexes changed files since last index/sync.

#### `codegraph index`
**When to use:** For full re-indexing (rarely needed, prefer `sync`)
```bash
~/.codegraph/current/bin/codegraph index
```
Full project re-index - slower but ensures complete accuracy.

---

## Search & Query Operations

### `codegraph query <search>`
**When to use:** To find symbols, functions, types, or any code entities
```bash
# Find a function
~/.codegraph/current/bin/codegraph query "sendMessage"

# Find a type or struct
~/.codegraph/current/bin/codegraph query "WebSocketHandler"

# Search with wildcards
~/.codegraph/current/bin/codegraph query "handle*"
```

**Returns:** File paths, line numbers, and context for matching symbols

### `codegraph files`
**When to use:** To understand project structure from the index
```bash
~/.codegraph/current/bin/codegraph files
```

**Options:**
- `--tree` - Show as tree structure
- `--lang <language>` - Filter by language (rust, typescript, javascript, etc.)

---

## Dependency & Impact Analysis

### `codegraph callers <symbol>`
**When to use:** To find what calls a specific function/method
```bash
~/.codegraph/current/bin/codegraph callers "broadcast"
```

**Use cases:**
- Understanding usage of a function before modifying it
- Finding all entry points for a feature
- Impact analysis before refactoring

### `codegraph callees <symbol>`
**When to use:** To find what a function/method calls
```bash
~/.codegraph/current/bin/codegraph callees "handleConnection"
```

**Use cases:**
- Understanding function dependencies
- Mapping execution flow
- Identifying which components a function depends on

### `codegraph impact <symbol>`
**When to use:** To analyze full impact of changing a symbol
```bash
~/.codegraph/current/bin/codegraph impact "ConnectionRegistry"
```

**Returns:** Complete transitive impact - everything that could be affected by changing this symbol

**Critical for:**
- Breaking changes assessment
- Refactoring planning
- Risk analysis before modifications

### `codegraph affected [files...]`
**When to use:** To find test files affected by source file changes
```bash
# Check what tests are affected by changes to specific files
~/.codegraph/current/bin/codegraph affected crates/core/src/broadcast.rs

# Check currently modified files (git)
~/.codegraph/current/bin/codegraph affected --git-diff
```

**Use cases:**
- Determining which tests to run
- PR validation
- Continuous integration optimization

---

## Context Building

### `codegraph context <task>`
**When to use:** To gather relevant code context for a task
```bash
~/.codegraph/current/bin/codegraph context "add rate limiting to websocket connections"
```

**Returns:** Markdown with relevant files, symbols, and context for the task

**Use cases:**
- Starting work on a feature
- Understanding how to implement something
- Gathering context before making changes

---

## Workflow Guidelines

### 🔄 When to Update the Index

**Always sync after:**
1. Creating new files
2. Modifying function signatures
3. Adding/removing imports
4. Refactoring code structure
5. Switching git branches with different code

**Command:**
```bash
~/.codegraph/current/bin/codegraph sync
```

### 🔍 Before Making Changes

1. **Check status** to ensure index is current
2. **Query** for relevant symbols/functions
3. **Analyze impact** of planned changes
4. **Find callers** to understand usage patterns
5. **Build context** for complex features

### 📝 During Development

1. Make code changes
2. **Sync** the index immediately
3. **Check affected tests** to know what to run
4. Run appropriate tests
5. **Re-check impact** if making additional changes

### ✅ Before Committing

1. **Sync** to capture all changes
2. **Check affected** tests have been run
3. **Verify impact** analysis matches expectations
4. Review the changes one more time

---

## Project-Specific Notes

This is a **BeamSocket** project with:
- **33 Rust files** (core WebSocket functionality)
- **26 JavaScript files** (benchmarks, examples, build scripts)
- **10 TypeScript files** (Node.js bindings and types)

### Key Areas

**Core Rust Implementation:**
- `crates/core/src/` - Main WebSocket engine
- `crates/node/src/` - Node.js N-API bindings

**Node.js Package:**
- `packages/beamsocket/src/` - TypeScript API surface

**Tests:**
- `crates/core/tests/` - Rust integration tests
- `packages/beamsocket/__tests__/` - JavaScript integration tests

### Common Queries for This Project

```bash
# Find all broadcast-related code
~/.codegraph/current/bin/codegraph query "broadcast"

# Find connection handling
~/.codegraph/current/bin/codegraph query "Connection"

# Check what calls the engine
~/.codegraph/current/bin/codegraph callers "Engine"

# Impact of changing rooms implementation
~/.codegraph/current/bin/codegraph impact "RoomManager"
```

---

## Troubleshooting

### Stale Lock File
If indexing is blocked:
```bash
~/.codegraph/current/bin/codegraph unlock
```

### Out of Sync Index
If results seem wrong:
```bash
~/.codegraph/current/bin/codegraph index  # Full re-index
```

### Check Health
Always start with:
```bash
~/.codegraph/current/bin/codegraph status
```

---

## Best Practices for AI Assistants

1. **Check status first** - Always verify index is up to date before queries
2. **Sync after changes** - Update the index immediately after file modifications  
3. **Use impact analysis** - Before suggesting breaking changes
4. **Find callers** - Understand usage before refactoring public APIs
5. **Build context** - For complex features, use the context command
6. **Check affected tests** - Know what needs testing after changes
7. **Query before implementing** - See if similar code already exists

---

## Summary Decision Tree

```
Starting a task?
  └─> codegraph status → codegraph context "<task>"

Making changes?
  └─> codegraph query → codegraph callers → codegraph impact → Make changes → codegraph sync

After changes?
  └─> codegraph affected --git-diff → Run tests

Unknown code?
  └─> codegraph query → codegraph callees (to understand what it does)
      └─> codegraph callers (to understand where it's used)
```
