# Plan: Convert Bootstrapper from Recursive to Iterative

## Context

The `Bootstrapper` class in `src/fromager/bootstrapper.py` currently uses recursive calls to process dependency trees. When bootstrapping packages with very deep or wide dependency graphs (especially with `--multiple-versions` mode enabled), this hits Python's recursion depth limit and causes stack overflow errors.

The recursion occurs at two critical points:

- **Line 521**: Install dependencies (processed AFTER building the package)
- **Line 708**: Build dependencies (BUILD_SYSTEM, BUILD_BACKEND, BUILD_SDIST - processed BEFORE building)

This refactoring converts the recursive depth-first traversal to an iterative approach using an explicit work stack, similar to existing patterns in `dependency_graph.py` (lines 217-255) and `commands/graph.py` (lines 136-179).

## Recommended Approach

**Use an explicit work stack with work items that represent "stack frames"**

This approach:

- Preserves exact execution order (depth-first traversal)
- Maintains all existing behavior (test_mode, multiple_versions, sdist_only)
- Matches proven patterns already in the codebase
- Provides clear state management and debuggability
- Eliminates recursion depth limits entirely

## Implementation Design

### 1. Work Item Structure

Add a new dataclass to represent each package version to bootstrap:

```python
@dataclasses.dataclass
class BootstrapWorkItem:
    """Represents a single bootstrap work item (stack frame).

    Each work item processes one package version through multiple phases.
    """
    # Core requirement info
    req: Requirement
    req_type: RequirementType
    source_url: str
    resolved_version: Version
    build_sdist_only: bool

    # Parent context (for graph edges)
    parent: tuple[Requirement, Version] | None

    # Dependency chain (snapshot of self.why at creation)
    why_snapshot: list[tuple[RequirementType, Requirement, Version]]

    # Processing phase
    phase: BootstrapPhase = BootstrapPhase.GRAPH_UPDATE

    # Phase-specific state
    build_result: SourceBuildResult | None = None
    install_dependencies: list[Requirement] = dataclasses.field(default_factory=list)
    install_dep_index: int = 0
```

### 2. Processing Phases

Add an enum for the distinct processing phases:

```python
class BootstrapPhase(StrEnum):
    """Processing phases for a bootstrap work item."""
    GRAPH_UPDATE = "graph_update"         # Add to dependency graph
    SEEN_CHECK = "seen_check"             # Check if already processed
    BUILD = "build"                       # Build package (handles build deps)
    EXTRACT_DEPS = "extract_deps"         # Extract install dependencies
    BUILD_ORDER = "build_order"           # Record in build-order.json
    PROCESS_INSTALL = "process_install"   # Process install dependencies
    CLEANUP = "cleanup"                   # Clean build directories
    COMPLETE = "complete"                 # Done
```

### 3. Main Iterative Loop

Replace recursive `bootstrap()` with iterative loop:

```python
def bootstrap(self, req: Requirement, req_type: RequirementType) -> None:
    """Bootstrap a package and its dependencies (iterative)."""
    # Resolve versions (same as current)
    resolved_versions = self.resolve_versions(
        req=req, req_type=req_type,
        return_all_versions=self.multiple_versions
    )

    # Create initial work items (reverse order for LIFO stack)
    work_stack: list[BootstrapWorkItem] = []
    for source_url, resolved_version in reversed(resolved_versions):
        work_stack.append(BootstrapWorkItem(
            req=req,
            req_type=req_type,
            source_url=source_url,
            resolved_version=resolved_version,
            parent=self._get_current_parent(),
            why_snapshot=self.why.copy(),
            build_sdist_only=self._should_build_sdist_only(req_type),
            phase=BootstrapPhase.GRAPH_UPDATE,
        ))

    # Process work stack (LIFO = depth-first)
    while work_stack:
        item = work_stack.pop()

        # Restore dependency chain context
        self.why = item.why_snapshot.copy()

        try:
            # Process current phase
            self._process_phase(item, work_stack)
        except Exception as err:
            self._handle_bootstrap_error(item, err, work_stack)
```

### 4. Phase Processing

Implement phase handlers that process one phase and advance to the next:

- `_phase_graph_update()`: Add to graph → advance to SEEN_CHECK
- `_phase_seen_check()`: Check if seen → if new, mark seen and advance to BUILD; if seen, stop
- `_phase_build()`: Build package (including build deps) → advance to EXTRACT_DEPS
- `_phase_extract_deps()`: Extract install deps → advance to BUILD_ORDER
- `_phase_build_order()`: Record in build-order.json → advance to PROCESS_INSTALL
- `_phase_process_install()`: Push install deps to stack → advance to CLEANUP
- `_phase_cleanup()`: Clean build dirs → done (don't push back)

### 5. Build Dependencies Handling

**Critical**: Build dependencies must complete BEFORE building the package.

In `_phase_build()`:

1. Push current item onto `self.why` stack
2. Call `_prepare_build_dependencies_iterative()` which:
   - Collects BUILD_SYSTEM, BUILD_BACKEND, BUILD_SDIST dependencies
   - Pushes them to work stack in reverse order (SDIST, BACKEND, SYSTEM)
   - They pop in correct order (SYSTEM, BACKEND, SDIST) and process depth-first
3. After build deps complete, build the package
4. Pop from `self.why` stack

The work stack LIFO order ensures build deps are processed to completion before the parent package continues.

### 6. Install Dependencies Handling

In `_phase_process_install()`:

1. If more install deps remain:
   - Push current item back with incremented `install_dep_index`
   - Resolve next install dep
   - Push install dep work items to stack (in reverse order)
2. If all install deps processed:
   - Advance to CLEANUP phase

The LIFO stack ensures depth-first processing: each install dep and its transitive deps complete before the next install dep.

### 7. Progress Bar Updates

**Current behavior** (lines 516, 522, 702, 709):

- `update_total()` increases total count when dependencies are discovered
- `update()` increments progress after EACH direct dependency completes (including all its transitive deps)

**Challenge**: In recursive code, `update()` is called when a child's recursive call returns. In iterative code, we don't have a return point—we just push work items to the stack.

**Solution**: Use progress update marker work items

Add a simple marker class:

```python
@dataclasses.dataclass
class ProgressUpdateMarker:
    """Marker work item that updates progress bar when popped from stack."""
    pass
```

When pushing dependencies to the stack, insert markers **before** the dependency work items (so they pop **after** the deps complete):

```python
def _phase_process_install_deps(self, item, stack):
    if item.install_dep_index < len(item.install_dependencies):
        # Push current item back for next install dep
        next_item = dataclasses.replace(item, install_dep_index=item.install_dep_index + 1)
        stack.append(next_item)

        # Push progress marker FIRST (pops LAST, after dep completes)
        stack.append(ProgressUpdateMarker())

        # Push install dependency work items
        dep = item.install_dependencies[item.install_dep_index]
        dep_versions = self.resolve_versions(...)
        for source_url, version in reversed(dep_versions):
            stack.append(BootstrapWorkItem(...))
```

In the main loop, detect markers and call `update()`:

```python
while work_stack:
    item = work_stack.pop()

    if isinstance(item, ProgressUpdateMarker):
        self.progressbar.update()
        continue

    # ... rest of processing
```

**Processing order** (LIFO):

1. Pop and process install dep work item → entire dependency subtree completes
2. Pop ProgressUpdateMarker → call `update()` ✓
3. Pop parent item → continue to next install dep

Same approach for build dependencies in `_prepare_build_dependencies_iterative()`.

**Result**: Progress bar behaves identically to recursive version—updates after each direct dependency (and its transitive deps) completes.

### 8. Error Handling Preservation

All error handling modes work identically:

**Test mode**: Catch exceptions per work item, record failure, don't push back (skip package)

**Multiple versions mode**: Catch exceptions per version, record failure, remove from graph, don't push back (continue to other versions)

**Normal mode**: Raise exceptions immediately (fail-fast)

Non-fatal errors (hooks, dependency extraction) are caught within phase handlers, exactly as in current code.

## Critical Files to Modify

**Primary file:**

- `src/fromager/bootstrapper.py` (lines 270-526)
  - Add `BootstrapWorkItem` dataclass, `ProgressUpdateMarker` dataclass, and `BootstrapPhase` enum
  - Replace `bootstrap()` method with iterative version
  - Replace `_bootstrap_single_version()` (lines 322-390) - logic moves to work item creation
  - Replace `_bootstrap_impl()` (lines 392-526) - logic splits into phase handlers
  - Replace `_handle_build_requirements()` (lines 696-709) - logic moves to `_prepare_build_dependencies_iterative()`
  - Add 7 new phase handler methods
  - Add build dependency helpers
  - Add centralized error handler
  - Update main loop to handle progress markers

**Reference files (read-only, for patterns):**

- `src/fromager/dependency_graph.py` (lines 217-255) - Stack-based DFS traversal pattern
- `src/fromager/commands/graph.py` (lines 136-179) - Stack with context preservation
- `src/fromager/requirements_file.py` (lines 14-29) - RequirementType enum

**Test files (for verification):**

- `tests/test_bootstrapper.py` - All existing tests must pass unchanged

## Implementation Steps

### Step 1: Add Data Structures (Low Risk)

- Add `BootstrapPhase` enum
- Add `BootstrapWorkItem` dataclass
- Add `ProgressUpdateMarker` dataclass
- No behavior changes, not called yet

### Step 2: Add Phase Handler Methods (Low Risk)

- Implement all `_phase_*()` methods by extracting logic from `_bootstrap_impl()`
- Implement `_build_package()` to orchestrate building with build deps
- Implement `_prepare_build_dependencies_iterative()` to push build deps
- Implement `_handle_bootstrap_error()` for centralized error handling
- Not called yet, no behavior changes

### Step 3: Add Feature Flag (Low Risk)

- Add `use_iterative: bool = False` parameter to `__init__()`
- Keep `_bootstrap_impl()` as `_bootstrap_recursive()`
- Add new `_bootstrap_iterative()` with work loop
- Route through feature flag in `bootstrap()`

### Step 4: Test with Flag Enabled (Medium Risk)

- Run full test suite with `use_iterative=True`
- Compare outputs (build-order.json, graph.json) with recursive version
- Fix any issues found

### Step 5: Enable by Default (Medium Risk)

- Change default to `use_iterative=True`
- Run full test suite and e2e tests
- Monitor for any issues

### Step 6: Remove Recursive Code (Low Risk)

- Remove `_bootstrap_recursive()`, `_bootstrap_single_version()`, `_handle_build_requirements()`
- Remove feature flag
- Rename `_bootstrap_iterative()` to `bootstrap()`

## Self.why Stack Management

**Challenge**: `self.why` is a shared stack tracking the current dependency chain. The recursive version manages it via the call stack.

**Solution**: Snapshot and restore approach

1. Each work item captures `why_snapshot = self.why.copy()` at creation time
2. When processing a work item, restore: `self.why = item.why_snapshot.copy()`
3. For build phase, temporarily push current requirement onto `self.why` (for build dep context)
4. After build completes, pop from `self.why`

This preserves exact behavior for:

- `_processing_build_requirement()` (line 244-268) - checks `self.why` for build deps
- `_explain` property (lines 579-585) - formats `self.why` for logging
- Parent context extraction (lines 335-337) - reads `self.why[-1]`

## Verification Strategy

### Functional Testing

1. Run full test suite: `hatch run test:test`
2. Run e2e tests: `hatch run test:test e2e/`
3. All tests must pass unchanged

### Equivalence Testing

1. Bootstrap same package set with recursive (flag off) and iterative (flag on)
2. Compare `build-order.json` - must be identical
3. Compare `dependency-graph.json` - must be identical
4. Compare log output - order should match

### Stress Testing

1. Bootstrap package with 1000+ transitive dependencies
2. Bootstrap package with 500-level deep dependency chain
3. Verify no stack overflow
4. Verify reasonable memory usage

### Performance Testing

1. Compare execution time (should be similar, maybe slightly slower)
2. Measure memory usage (should be similar or better)
3. Profile for bottlenecks if needed

## Edge Cases Handled

1. **Cyclic dependencies**: Prevented by `_seen_requirements` set (unchanged behavior)
2. **Multiple versions mode**: Each version is independent work item with own error handling
3. **Test mode fallback**: Build failures trigger prebuilt fallback within `_build_package()`
4. **Git URL requirements**: Resolution unchanged, happens before work item creation
5. **Deep chains**: Work stack on heap, no recursion limit
6. **Progress bar**: Uses marker work items to update after each dependency completes, matching recursive behavior exactly
7. **Build/install dep ordering**: LIFO stack with correct push order ensures correct processing order

## Rollback Plan

Feature flag approach allows easy rollback:

- If issues found, default `use_iterative=False`
- Keep both paths until iterative version proven stable
- Can A/B compare outputs to debug discrepancies
- Eventually remove recursive path once confident

## Success Criteria

1. All existing tests pass unchanged
2. Bootstrap same packages produces identical output files
3. Can bootstrap packages with arbitrarily deep dependency graphs
4. No regressions in test_mode, multiple_versions, or sdist_only modes
5. Performance within 10% of recursive version
