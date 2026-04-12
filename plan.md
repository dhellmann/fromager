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

## Key Refinements in This Plan

This refined plan clarifies several critical details:

01. **9 phases total** (not 8): BUILD split into EXTRACT_BUILD_DEPS and BUILD_PACKAGE for clarity
02. **Phase naming consistency**: EXTRACT_BUILD_DEPS and EXTRACT_INSTALL_DEPS for parallel structure
03. **Linear phase progression**: Every work item flows through all 9 phases sequentially; phases check `skip_processing` flag internally to decide whether to do work
04. **self.why management**: Main loop handles ALL push/pop, phase handlers MUST NOT touch it
05. **Phase handler details**: Explicit function signatures, behavior, and state transitions
06. **Seen requirements**: Detailed key format and marking logic
07. **Multiple versions handling**: Concrete example of how dependencies with multiple versions are processed depth-first
08. **Progress bar semantics**: When update_total() vs update() are called
09. **Error handling specifics**: Which errors are fatal vs non-fatal in test mode
10. **Helper methods**: \_get_current_parent(), \_process_phase(), \_handle_bootstrap_error()
11. **Memory usage note**: Linear with dependency width due to why_snapshot copies

## Implementation Design

### 1. Work Item Structure

Add a new dataclass to represent each package version to bootstrap:

```python
@dataclasses.dataclass
class BootstrapWorkItem:
    """Represents a single bootstrap work item (stack frame).

    Each work item processes one package version through multiple phases.
    All items flow through all 9 phases linearly.
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

    # Processing control
    skip_processing: bool = False  # Set by SEEN_CHECK if already processed

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
    EXTRACT_BUILD_DEPS = "build_deps"             # Collect and push build dependencies
    BUILD_PACKAGE = "build_package"       # Actually build the package
    EXTRACT_INSTALL_DEPS = "extract_deps"         # Extract install dependencies
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

**Linear progression approach:** Every work item flows through all 9 phases sequentially. Each phase checks `item.skip_processing` flag and either does work or becomes a no-op, then always advances to the next phase.

**Function signature:** `def _phase_X(self, item: BootstrapWorkItem, work_stack: list[BootstrapWorkItem]) -> None:`

Phase handlers modify `item` in place and append to `work_stack` directly. They return `None`.

**All phase handlers follow this pattern:**

1. Check if work should be skipped (`item.skip_processing` or other settings)
2. Do work if not skipping
3. Advance `item.phase` to next phase
4. Push `item` back to `work_stack` (except COMPLETE)

**Exception: PROCESS_INSTALL** may loop back to itself (increments `install_dep_index` instead of advancing phase) until all dependencies processed.

#### Phase Handlers (Linear Progression):

- **`_phase_graph_update()`**: Add dependency graph edge

  - **Work:** Add edge from parent to this item in dependency graph (always do this)
  - **Next:** Advance to SEEN_CHECK, push back

- **`_phase_seen_check()`**: Check if already processed

  - **Key:** `(canonicalize_name(item.req.name), tuple(sorted(item.req.extras)), str(item.resolved_version), "sdist"/"wheel")`
  - **If seen:** Set `item.skip_processing = True`
  - **If not seen:** Mark as seen in `self._seen_requirements`
  - **Next:** Advance to EXTRACT_BUILD_DEPS, push back

- **`_phase_extract_build_deps()`**: Collect build dependencies

  - **Skip if:** `item.skip_processing == True`
  - **Work:**
    - Extract BUILD_SYSTEM, BUILD_BACKEND, BUILD_SDIST dependencies (3 calls)
    - Call `update_total(len(build_dependencies))` for progress tracking
    - Push build deps to work stack in reverse order (SDIST, BACKEND, SYSTEM)
    - Build deps pop in correct order (SYSTEM, BACKEND, SDIST) and complete before parent continues
  - **Next:** Advance to BUILD_PACKAGE, push back

- **`_phase_build_package()`**: Build the package

  - **Skip if:** `item.skip_processing == True`
  - **Work:**
    - Call `_build_package()` to build sdist and/or wheel
    - Respects `item.build_sdist_only` flag (skip wheel if True)
    - Store result in `item.build_result`
  - **Next:** Advance to EXTRACT_INSTALL_DEPS, push back

- **`_phase_extract_install_deps()`**: Extract install dependencies

  - **Skip if:** `item.skip_processing == True`
  - **Work:**
    - Call `_get_install_dependencies()` to extract from wheel/sdist
    - Handle exceptions in test_mode (non-fatal, use empty list)
    - Call `update_total(len(install_dependencies))` for progress tracking
    - Store in `item.install_dependencies`
  - **Next:** Advance to BUILD_ORDER, push back

- **`_phase_build_order()`**: Record in build-order.json

  - **Skip if:** `item.skip_processing == True`
  - **Work:** Call `self.build_order.append()`
  - **Next:** Advance to PROCESS_INSTALL, push back

- **`_phase_process_install()`**: Process install dependencies depth-first

  - **Skip if:** `item.skip_processing == True`
  - **Work:**
    - **If more deps remain** (`item.install_dep_index < len(item.install_dependencies)`):
      - Get next dependency
      - Resolve versions for that dependency
      - Increment `item.install_dep_index`
      - Push current item back (**same phase**, updated index)
      - Push resolved dependency work items in reverse order
      - Return (don't advance phase yet)
    - **If all deps processed:** Continue to next phase
  - **Next:** Advance to CLEANUP, push back

- **`_phase_cleanup()`**: Clean build directories

  - **Skip if:** `item.skip_processing == True`
  - **Work:** Call cleanup hooks/methods
  - **Next:** Advance to COMPLETE, push back

- **`_phase_complete()`**: Mark package complete

  - **Work:** Call `self.progressbar.update()` to increment progress (always do this)
  - **Next:** Done, don't push back (processing complete)

### 5. Build Dependencies Handling

**Critical**: Build dependencies must complete BEFORE building the package.

**Two-phase approach (within linear progression):**

1. **EXTRACT_BUILD_DEPS phase**: Collect and push build dependencies to stack

   - Skip if `item.skip_processing == True`
   - Extract BUILD_SYSTEM, BUILD_BACKEND, BUILD_SDIST dependencies (3 separate calls)
   - Push build deps to work stack in reverse order: SDIST, BACKEND, SYSTEM
   - Push current item back with `phase = BUILD_PACKAGE`
   - Return (let stack process build deps)

2. **BUILD_PACKAGE phase**: Build the package (all build deps now complete)

   - Skip if `item.skip_processing == True`
   - At this point, all build dependencies have been processed to completion
   - Call `_build_package()` to build sdist and/or wheel
   - Respects `item.build_sdist_only` flag
   - Advance to EXTRACT_INSTALL_DEPS

**Stack ordering ensures correctness:**

- LIFO stack pops SYSTEM first → processes through all 9 phases → completes
- Then pops BACKEND → processes through all 9 phases → completes
- Then pops SDIST → processes through all 9 phases → completes
- Finally pops parent item in BUILD_PACKAGE phase → all build deps are ready

**Example stack flow:**

```
Initial: [parent@EXTRACT_BUILD_DEPS]
After EXTRACT_BUILD_DEPS: [parent@BUILD_PACKAGE, SDIST@GRAPH_UPDATE, BACKEND@GRAPH_UPDATE, SYSTEM@GRAPH_UPDATE]
Pop SYSTEM → flows through all 9 phases → completes
Pop BACKEND → flows through all 9 phases → completes
Pop SDIST → flows through all 9 phases → completes
Pop parent@BUILD_PACKAGE → build with all deps available → continues to EXTRACT_INSTALL_DEPS → ... → COMPLETE
```

### 6. Install Dependencies Handling

In `_phase_process_install()`:

1. If more install deps remain (`item.install_dep_index < len(item.install_dependencies)`):
   - Get next dependency: `dep = item.install_dependencies[item.install_dep_index]`
   - Resolve versions: `resolved_versions = self.resolve_versions(req=dep, req_type=RequirementType.INSTALL, return_all_versions=self.multiple_versions)`
   - Increment index: `item.install_dep_index += 1`
   - Push current item back (same phase, updated index)
   - **Create and push dependency work items in reverse order:**
     - For each `(source_url, resolved_version)` in `reversed(resolved_versions)`:
       - Create work item with `parent=(item.req, item.resolved_version)`, `why_snapshot=self.why.copy()`, `phase=GRAPH_UPDATE`
       - Append to work_stack
2. If all deps processed:
   - Advance to CLEANUP phase
   - Push current item back

**Depth-first processing with multiple versions:**

Example: If dependency B resolves to versions [1.0, 2.0] and dependency C to \[3.0\]:

```
install_dependencies = [B, C]  # Process B first, then C

Processing B (index 0):
  resolved_versions = [(url_B_2.0, 2.0), (url_B_1.0, 1.0)]
  Push: [parent@PROCESS_INSTALL(index=1), B-1.0@GRAPH_UPDATE, B-2.0@GRAPH_UPDATE]

Stack pops: B-2.0 processes fully → B-1.0 processes fully → parent continues

Processing C (index 1):
  resolved_versions = [(url_C_3.0, 3.0)]
  Push: [parent@PROCESS_INSTALL(index=2), C-3.0@GRAPH_UPDATE]

Stack pops: C-3.0 processes fully → parent continues

Index 2 >= len(install_dependencies) → advance to CLEANUP
```

The LIFO stack ensures depth-first processing: each install dep (all its versions) and transitive deps complete before the next install dep.

### 7. Progress Bar Updates

**Current behavior** (lines 516, 522, 702, 709):

- `update_total()` increases total count when dependencies are discovered
- `update()` increments progress after EACH package completes (including building the package and processing all its transitive deps)

**Challenge**: In recursive code, `update()` is called when a child's recursive call returns. In iterative code, we don't have a return point—we just push work items to the stack.

**Solution**: Use COMPLETE phase

The COMPLETE phase is the final phase for each work item. It:

1. Calls `self.progressbar.update()` to increment progress
2. Doesn't push the item back (processing complete)

**Phase flow:**

- GRAPH_UPDATE → SEEN_CHECK → BUILD → EXTRACT_INSTALL_DEPS → BUILD_ORDER → PROCESS_INSTALL → CLEANUP → **COMPLETE**
- COMPLETE phase calls `update()` and finishes

**Update totals:**

- Call `update_total(len(install_dependencies))` in EXTRACT_INSTALL_DEPS phase when install deps are discovered
- Call `update_total(len(build_dependencies))` in BUILD phase when build deps are discovered

**Result**: Progress bar behaves identically to recursive version—updates after each package completes (including all its work and transitive dependencies).

### 8. Error Handling Preservation

All error handling modes work identically, but the mechanism changes from call-stack-based to work-stack-based.

#### Main Loop Error Handling

```python
while work_stack:
    item = work_stack.pop()

    # Restore parent context
    self.why = item.why_snapshot.copy()

    # Push current item for this phase
    self.why.append((item.req_type, item.req, item.resolved_version))

    try:
        self._process_phase(item, work_stack)
    except Exception as err:
        # Pop current item from self.why
        self.why.pop()

        # Handle based on mode
        if self.test_mode:
            self._record_test_mode_failure(item.req, str(item.resolved_version), err, "bootstrap")
            continue  # Skip to next work item

        if self.multiple_versions:
            pkg_name = canonicalize_name(item.req.name)
            self._failed_versions.append((pkg_name, str(item.resolved_version), err))
            self.ctx.dependency_graph.remove_dependency(pkg_name, item.resolved_version)
            self.ctx.write_to_graph_to_file()
            continue  # Skip to next work item (next version)

        raise  # Fail-fast in normal mode
    else:
        # Success - pop current item from self.why
        self.why.pop()
```

**Test mode**: Catch exceptions per work item, record failure, continue to next item

**Multiple versions mode**: Catch exceptions per version, record failure, remove from graph, continue to next version

**Normal mode**: Raise exceptions immediately (fail-fast)

#### Phase Handler Error Handling

Non-fatal errors (hooks, dependency extraction) are caught within phase handlers:

```python
def _phase_extract_install_deps(self, item, work_stack):
    try:
        install_dependencies = self._get_install_dependencies(...)
    except Exception as dep_error:
        if not self.test_mode:
            raise
        self._record_test_mode_failure(
            item.req, str(item.resolved_version), dep_error,
            "dependency_extraction", "warning"
        )
        install_dependencies = []

    item.install_dependencies = install_dependencies
    self.progressbar.update_total(len(install_dependencies))
    item.phase = BootstrapPhase.BUILD_ORDER
    work_stack.append(item)
```

#### Failed Dependency Handling - Lazy Cleanup

When a package fails, its dependencies may already be on the work stack. Use **lazy cleanup**:

- Let dependency work items process normally
- They will hit the `_seen_requirements` check and skip (already processed)
- Or their build will fail naturally (missing dependencies in build environment)
- This preserves exact recursive behavior without complex stack manipulation

**Advantages:**

- Simple - no need to scan/modify work_stack
- Correct - `_seen_requirements` already prevents reprocessing
- Matches recursive behavior - failed package's deps would have failed anyway

## Critical Files to Modify

**Primary file:**

- `src/fromager/bootstrapper.py` (lines 270-526)
  - Add `BootstrapWorkItem` dataclass and `BootstrapPhase` enum (9 phases)
  - Replace `bootstrap()` method with iterative version
  - Replace `_bootstrap_single_version()` (lines 322-390) - logic moves to work item creation
  - Replace `_bootstrap_impl()` (lines 392-526) - logic splits into phase handlers
  - Replace `_handle_build_requirements()` (lines 696-709) - logic moves to EXTRACT_BUILD_DEPS phase
  - Add 9 new phase handler methods:
    - `_phase_graph_update()`
    - `_phase_seen_check()`
    - `_phase_extract_build_deps()`
    - `_phase_build_package()`
    - `_phase_extract_install_deps()`
    - `_phase_build_order()`
    - `_phase_process_install()`
    - `_phase_cleanup()`
    - `_phase_complete()`
  - Add `_process_phase()` dispatcher method
  - Add `_handle_bootstrap_error()` error handling method
  - Add `_get_current_parent()` helper (if doesn't exist) for extracting parent from `self.why`
  - Update main loop with error handling and self.why management

**Reference files (read-only, for patterns):**

- `src/fromager/dependency_graph.py` (lines 217-255) - Stack-based DFS traversal pattern
- `src/fromager/commands/graph.py` (lines 136-179) - Stack with context preservation
- `src/fromager/requirements_file.py` (lines 14-29) - RequirementType enum

**Test files (for verification):**

- `tests/test_bootstrapper.py` - All existing tests must pass unchanged

## Implementation Steps

### Step 1: Add Data Structures (Low Risk)

- Add `BootstrapPhase` enum (9 phases including COMPLETE)
- Add `BootstrapWorkItem` dataclass
- No behavior changes, not called yet

### Step 2: Add Phase Handler Methods (Low Risk)

- Implement all 9 `_phase_*()` methods by extracting logic from `_bootstrap_impl()`
- Implement `_process_phase()` dispatcher and `_handle_bootstrap_error()` helper
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

**Solution**: Snapshot and restore approach with main loop push/pop

1. Each work item captures `why_snapshot = self.why.copy()` at creation time (preserves parent context)
2. Main loop restores parent context: `self.why = item.why_snapshot.copy()`
3. Main loop pushes current item: `self.why.append((item.req_type, item.req, item.resolved_version))`
4. Main loop pops current item in try/except/else for error safety
5. **Phase handlers MUST NOT manipulate `self.why`** - they see current item already on the stack

**Main loop handles ALL push/pop:**

```python
while work_stack:
    item = work_stack.pop()

    # Restore parent context from snapshot
    self.why = item.why_snapshot.copy()

    # Push current item (main loop responsibility, not phase handler)
    self.why.append((item.req_type, item.req, item.resolved_version))

    try:
        self._process_phase(item, work_stack)
    except Exception as err:
        self.why.pop()  # Cleanup on error
        # ... handle error based on mode
    else:
        self.why.pop()  # Cleanup on success
```

**When creating child work items (in phase handlers):**

Phase handlers create child work items with `why_snapshot=self.why.copy()`. At that point, `self.why` contains the current item (pushed by main loop), so children get correct parent context.

**Example:** When PROCESS_INSTALL creates work item for dependency B:

```python
# self.why = [A, parent_item] (pushed by main loop)
child_item = BootstrapWorkItem(
    req=B,
    why_snapshot=self.why.copy(),  # Captures [A, parent_item]
    parent=(item.req, item.resolved_version),  # parent_item
    ...
)
```

When child processes later, main loop restores `self.why = [A, parent_item]` and pushes `[A, parent_item, B]`.

**Helper method for creating parent tuple:**

`_get_current_parent()` is called when creating work items:

- Returns `None` if `self.why` is empty (root package)
- Returns `(self.why[-1][1], self.why[-1][2])` if non-empty (parent req, parent version)

For root packages in `bootstrap()`, `self.why` is empty before entering work loop → `parent=None`.
For child work items in phase handlers, `self.why` contains current item → `parent=(current_req, current_version)`.

This preserves exact behavior for:

- `_processing_build_requirement()` (line 261-266) - walks `self.why` to check if current requirement is a build dependency
- `_explain` property (lines 580-585) - formats `self.why` for logging
- Parent context extraction - reads `self.why[-1]` when needed

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
5. Compare failure reports in test mode
6. Compare failed versions list in multiple-versions mode

### Error Mode Testing

1. **Normal mode:** Verify fail-fast still works (exception propagates immediately)
2. **Test mode:**
   - Verify prebuilt fallback works for build failures
   - Verify non-fatal errors (hooks, deps) are recorded but continue
   - Verify failure report JSON is written
3. **Multiple versions mode:**
   - Verify all matching versions are processed
   - Verify failures are recorded per-version
   - Verify failed nodes are removed from graph
   - Verify other versions continue after one fails

### Stress Testing

1. Bootstrap package with 1000+ transitive dependencies
2. Bootstrap package with 500-level deep dependency chain
3. Verify no stack overflow
4. Verify reasonable memory usage

### Performance Testing

1. Compare execution time (should be similar, maybe slightly slower)
2. Measure memory usage (should be similar or better)
3. Profile for bottlenecks if needed

## Work Item Lifecycle Example

**Example 1: New package (not seen before)**

```
Item: requests-2.31.0, skip_processing=False

GRAPH_UPDATE:     Add edge to graph → advance to SEEN_CHECK
SEEN_CHECK:       Not in _seen_requirements → mark as seen → skip_processing=False → advance to EXTRACT_BUILD_DEPS
EXTRACT_BUILD_DEPS: Extract build deps → push deps to stack → advance to BUILD_PACKAGE
BUILD_PACKAGE:    Build sdist and wheel → advance to EXTRACT_INSTALL_DEPS
EXTRACT_INSTALL_DEPS: Extract install deps → advance to BUILD_ORDER
BUILD_ORDER:      Add to build-order.json → advance to PROCESS_INSTALL
PROCESS_INSTALL:  Push install deps to stack (loop if multiple) → advance to CLEANUP
CLEANUP:          Clean build dirs → advance to COMPLETE
COMPLETE:         Update progress bar → done
```

**Example 2: Already seen package (duplicate dependency)**

```
Item: urllib3-2.0.0, skip_processing=False

GRAPH_UPDATE:     Add edge to graph → advance to SEEN_CHECK
SEEN_CHECK:       Found in _seen_requirements → skip_processing=True → advance to EXTRACT_BUILD_DEPS
EXTRACT_BUILD_DEPS: skip_processing=True → do nothing → advance to BUILD_PACKAGE
BUILD_PACKAGE:    skip_processing=True → do nothing → advance to EXTRACT_INSTALL_DEPS
EXTRACT_INSTALL_DEPS: skip_processing=True → do nothing → advance to BUILD_ORDER
BUILD_ORDER:      skip_processing=True → do nothing → advance to PROCESS_INSTALL
PROCESS_INSTALL:  skip_processing=True → do nothing → advance to CLEANUP
CLEANUP:          skip_processing=True → do nothing → advance to COMPLETE
COMPLETE:         Update progress bar → done
```

**Example 3: sdist_only mode**

```
Item: numpy-1.24.0, skip_processing=False, build_sdist_only=True

GRAPH_UPDATE:     Add edge to graph → advance to SEEN_CHECK
SEEN_CHECK:       Not in _seen_requirements → mark as seen (sdist only) → skip_processing=False → advance to EXTRACT_BUILD_DEPS
EXTRACT_BUILD_DEPS: Extract build deps → push deps to stack → advance to BUILD_PACKAGE
BUILD_PACKAGE:    build_sdist_only=True → build only sdist, skip wheel → advance to EXTRACT_INSTALL_DEPS
EXTRACT_INSTALL_DEPS: Extract install deps from sdist → advance to BUILD_ORDER
BUILD_ORDER:      Add to build-order.json → advance to PROCESS_INSTALL
PROCESS_INSTALL:  Push install deps to stack → advance to CLEANUP
CLEANUP:          Clean build dirs → advance to COMPLETE
COMPLETE:         Update progress bar → done
```

## Additional Implementation Details

### Seen Requirements Tracking

**Key format** (defined at line 43 in bootstrapper.py):

```python
SeenKey = tuple[NormalizedName, tuple[str, ...], str, typing.Literal["sdist", "wheel"]]
```

**Components:**

1. `canonicalize_name(req.name)` — normalized package name
2. `tuple(sorted(req.extras))` — sorted extras tuple (e.g., `("dev", "test")`)
3. `str(version)` — version string
4. `"sdist"` or `"wheel"` — build type

**Marking logic** (lines 1383-1400):

- When building wheel: Mark both `(..., "sdist")` and `(..., "wheel")` as seen (wheel implies sdist exists)
- When building sdist-only: Mark only `(..., "sdist")` as seen

**Used in SEEN_CHECK phase** to break cycles and avoid redundant work.

### Progress Bar Behavior

**Initialization:** If not provided to constructor, creates `progress.Progressbar(None)` (line 100)

**Total tracking:** Cumulative count of all discovered dependencies

- EXTRACT_BUILD_DEPS: `update_total(len(build_dependencies))` per build type (3 calls)
- EXTRACT_INSTALL_DEPS: `update_total(len(install_dependencies))`

**Progress tracking:** Incremented after each package fully completes

- COMPLETE phase: `update()` called once per package

**Result:** Progress bar shows "N of M packages processed" where M grows as dependencies are discovered.

### Build Result Storage

`item.build_result: SourceBuildResult | None` stores the result from `_build_package()`. Currently not used for control flow, but available for:

- Error reporting
- Future optimizations
- Debugging

### Linear Phase Progression Benefits

**Design choice:** Every work item flows through all 9 phases sequentially, even if some phases become no-ops.

**Benefits:**

1. **Simplicity**: No complex conditional phase transitions to reason about
2. **Predictability**: Every item follows the same path (GRAPH_UPDATE → ... → COMPLETE)
3. **Debugging**: Can trace any item through standard flow
4. **Settings handling**: Each phase checks `item.skip_processing` or `item.build_sdist_only` internally

**How modes are handled:**

- **Already seen packages**: `skip_processing = True` in SEEN_CHECK, later phases become no-ops
- **sdist_only mode**: BUILD_PACKAGE checks `item.build_sdist_only` flag and skips wheel build
- **Test mode errors**: EXTRACT_INSTALL_DEPS catches exceptions, uses empty list, continues to next phase
- **Multiple versions**: Each version is independent work item flowing through same phases

**No special-case transitions needed** - the linear flow handles all modes cleanly.

### Test Mode Error Handling

**Multiple error types** recorded in `self.failed_packages`:

1. Resolution failures (line 297): Can't resolve requirement
2. Bootstrap failures (lines 369-373): Exception during bootstrap
3. Post-hook failures (lines 473-477): Non-fatal warning
4. Dependency extraction failures (lines 490-500): Non-fatal warning, use empty list
5. Build failures with fallback (lines 969-983): Try prebuilt, record if that fails too

**Non-fatal vs fatal:** Only dependency extraction and post-hook failures are non-fatal in test mode. Other failures terminate that package's processing but continue to next package.

## Edge Cases Handled

01. **Cyclic dependencies**: Prevented by `_seen_requirements` set (unchanged behavior)
02. **Multiple versions mode**: Each version is independent work item with own error handling
03. **Test mode fallback**: Build failures trigger prebuilt fallback within `_build_package()`
04. **sdist_only mode**: BUILD_PACKAGE phase checks `item.build_sdist_only` flag and skips wheel build; linear progression continues normally
05. **Already seen packages**: SEEN_CHECK sets `skip_processing = True`, subsequent phases become no-ops but item still flows through all phases
06. **Git URL requirements**: Resolution unchanged, happens before work item creation
07. **Deep chains**: Work stack on heap, no recursion limit
08. **Progress bar**: Uses COMPLETE phase to update after each package completes, matching recursive behavior exactly
09. **Build/install dep ordering**: LIFO stack with correct push order ensures correct processing order
10. **Self.why management**: Main loop handles push/pop with error safety, preserves dependency chain tracking
11. **Memory usage**: Each work item stores `why_snapshot` copy. For very wide trees (thousands of parallel deps), memory usage increases linearly with width, but should be acceptable for typical Python packages

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
