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

01. **5 phases total**: Optimized from 9 to 5 by combining related operations (GRAPH_UPDATE+SEEN_CHECK→START, BUILD_PACKAGE+BUILD_ORDER→BUILD_PACKAGE, CLEANUP+COMPLETE→COMPLETE) and eliminating unnecessary PROCESS_INSTALL phase (stack handles depth-first automatically)
02. **Phase naming consistency**: EXTRACT_BUILD_DEPS and EXTRACT_INSTALL_DEPS for parallel structure
03. **Linear progression with early exit**: START acts as filter - already-seen packages return `[]` and stop flowing; main loop updates progress bar when it sees `[]` (no skip_processing flag needed)
04. **Functional phase handlers**: Phase handlers return lists of work items instead of modifying the stack directly; main loop extends stack with returned items
05. **self.why management**: Main loop handles ALL push/pop, phase handlers MUST NOT touch it
06. **Phase handler details**: Explicit function signatures, behavior, and return values
07. **Seen requirements**: Detailed key format and marking logic
08. **Multiple versions handling**: Concrete example of how dependencies with multiple versions are processed depth-first
09. **Progress bar correctness**: Main loop calls `update()` when phase returns `[]` (work done); handles both already-seen (START→`[]`) and fully-processed (COMPLETE→`[]`)
10. **Error handling specifics**: Which errors are fatal vs non-fatal in test mode
11. **Helper methods**: \_get_current_parent(), \_process_phase(), \_handle_bootstrap_error()
12. **Memory usage note**: Linear with dependency width due to why_snapshot copies

## Implementation Design

### 1. Work Item Structure

Add a new dataclass to represent each package version to bootstrap:

```python
@dataclasses.dataclass
class BootstrapWorkItem:
    """Represents a single bootstrap work item (stack frame).

    Each work item processes one package version through multiple phases.
    Items flow through phases linearly until SEEN_CHECK determines if
    processing should continue or stop.
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
    phase: BootstrapPhase = BootstrapPhase.START

    # Phase-specific state
    build_result: SourceBuildResult | None = None
```

### 2. Processing Phases

Add an enum for the distinct processing phases:

```python
class BootstrapPhase(StrEnum):
    """Processing phases for a bootstrap work item."""
    START = "start"                       # Add to graph and check if already seen
    EXTRACT_BUILD_DEPS = "build_deps"     # Collect and push build dependencies
    BUILD_PACKAGE = "build_package"       # Build package and record in build-order.json
    EXTRACT_INSTALL_DEPS = "extract_deps" # Extract and push install dependencies
    COMPLETE = "complete"                 # Clean up and update progress bar
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
            phase=BootstrapPhase.START,
        ))

    # Process work stack (LIFO = depth-first)
    while work_stack:
        item = work_stack.pop()

        # Restore dependency chain context
        self.why = item.why_snapshot.copy()
        self.why.append((item.req_type, item.req, item.resolved_version))

        try:
            # Process current phase - returns items to add to stack
            new_items = self._process_phase(item)
            work_stack.extend(new_items)

            # Update progress bar when work item completes (returns [])
            # This handles both already-seen (START returns []) and
            # finished processing (COMPLETE returns [])
            if not new_items:
                self.progressbar.update()
        except Exception as err:
            # Handle error - may return items to continue processing
            new_items = self._handle_bootstrap_error(item, err)
            work_stack.extend(new_items)

            # Update progress if error terminates this work item
            if not new_items:
                self.progressbar.update()
        finally:
            self.why.pop()
```

### 4. Phase Processing

**Linear progression with early exit:** Work items flow through phases linearly. START acts as a filter - if a package has already been processed, it returns `[]` and the item stops flowing through the pipeline.

**Function signature:** `def _phase_X(self, item: BootstrapWorkItem) -> list[BootstrapWorkItem]:`

Phase handlers modify `item` in place and return a list of work items to add to the stack. The main loop extends the work stack with the returned items.

**Phase handler patterns:**

1. Do work for this phase (may create dependency work items)
2. Check if processing should stop:
   - START: If already seen, return `[]` (stop processing)
   - COMPLETE: Always return `[]` (end of pipeline)
3. Otherwise, advance `item.phase` to next phase
4. Return list of items to add to stack:
   - Usually `[item]` (current item with next phase)
   - May include dependency items: `[item, dep1, dep2, ...]` (in reverse order for LIFO)
   - Return `[]` to stop processing this item

**Exception: PROCESS_INSTALL** may loop back to itself (increments `install_dep_index` instead of advancing phase) until all dependencies processed.

#### Phase Handlers (Linear Progression with Early Exit):

- **`_phase_start()`**: Add to graph and check if already processed (filter/gatekeeper phase)

  - **Work:**
    - Add edge from parent to this item in dependency graph (always do this)
    - Check seen key: `(canonicalize_name(item.req.name), tuple(sorted(item.req.extras)), str(item.resolved_version), "sdist"/"wheel")`
  - **If already seen:**
    - Return `[]` ← **Early exit! Item stops here, doesn't flow through remaining phases**
    - Main loop sees `[]` and calls `progressbar.update()` (work item done)
  - **If not seen:**
    - Mark as seen in `self._seen_requirements`
    - Advance to EXTRACT_BUILD_DEPS
    - Return `[item]`

- **`_phase_extract_build_deps()`**: Collect build dependencies

  - **Work:**
    - Extract BUILD_SYSTEM, BUILD_BACKEND, BUILD_SDIST dependencies (3 calls)
    - Call `update_total(len(build_dependencies))` for progress tracking
    - Create work items for build deps in reverse order (SDIST, BACKEND, SYSTEM)
    - Build deps pop in correct order (SYSTEM, BACKEND, SDIST) and complete before parent continues
  - **Next:** Advance to BUILD_PACKAGE
  - **Return:** `[item] + build_dep_items`

- **`_phase_build_package()`**: Build the package and record in build order

  - **Work:**
    - Call `_build_package()` to build sdist and/or wheel
    - Respects `item.build_sdist_only` flag (skip wheel if True)
    - Store result in `item.build_result`
    - Call `self.build_order.append()` to record in build-order.json
  - **Next:** Advance to EXTRACT_INSTALL_DEPS
  - **Return:** `[item]`

- **`_phase_extract_install_deps()`**: Extract install dependencies and create work items

  - **Work:**
    - Call `_get_install_dependencies()` to extract from wheel/sdist
    - Handle exceptions in test_mode (non-fatal, use empty list)
    - Call `update_total(len(install_dependencies))` for progress tracking
    - For each dependency (in reverse order for depth-first):
      - Resolve versions for that dependency
      - Create work items for dependency versions in reverse order
      - Stack handles depth-first processing automatically
  - **Next:** Advance to COMPLETE
  - **Return:** `[item] + dep_items` (all dependency work items at once)

- **`_phase_complete()`**: Clean up and mark package complete

  - **Work:**
    - Call cleanup hooks/methods
    - Clean build directories
  - **Next:** Done
  - **Return:** `[]` ← **End of pipeline! Don't add back to stack**
  - Main loop sees `[]` and calls `progressbar.update()` (work item done)

### 5. Build Dependencies Handling

**Critical**: Build dependencies must complete BEFORE building the package.

**Two-phase approach (within linear progression):**

1. **EXTRACT_BUILD_DEPS phase**: Collect and return build dependencies

   - Extract BUILD_SYSTEM, BUILD_BACKEND, BUILD_SDIST dependencies (3 separate calls)
   - Create work items for build deps in reverse order: SDIST, BACKEND, SYSTEM
   - Advance current item to `phase = BUILD_PACKAGE`
   - Return `[item] + build_dep_items` (main loop adds to stack)
   - Stack processes build deps before continuing with parent

2. **BUILD_PACKAGE phase**: Build the package (all build deps now complete)

   - At this point, all build dependencies have been processed to completion
   - Call `_build_package()` to build sdist and/or wheel
   - Respects `item.build_sdist_only` flag
   - Advance to EXTRACT_INSTALL_DEPS
   - Return `[item]`

**Stack ordering ensures correctness:**

- LIFO stack pops SYSTEM first → processes through all 5 phases → completes
- Then pops BACKEND → processes through all 5 phases → completes
- Then pops SDIST → processes through all 5 phases → completes
- Finally pops parent item in BUILD_PACKAGE phase → all build deps are ready

**Example stack flow:**

```
Initial: [parent@EXTRACT_BUILD_DEPS]
After EXTRACT_BUILD_DEPS: [parent@BUILD_PACKAGE, SDIST@GRAPH_UPDATE, BACKEND@GRAPH_UPDATE, SYSTEM@GRAPH_UPDATE]
Pop SYSTEM → flows through all 5 phases → completes
Pop BACKEND → flows through all 5 phases → completes
Pop SDIST → flows through all 5 phases → completes
Pop parent@BUILD_PACKAGE → build with all deps available → continues to EXTRACT_INSTALL_DEPS → ... → COMPLETE
```

### 6. Install Dependencies Handling

In `_phase_extract_install_deps()`:

1. Extract install dependencies from wheel/sdist
2. For each dependency, resolve versions and create work items
3. Return all work items at once - stack handles depth-first automatically

**Simplified approach - no looping needed:**

```python
def _phase_extract_install_deps(self, item):
    # Extract dependencies
    install_dependencies = self._get_install_dependencies(...)
    self.progressbar.update_total(len(install_dependencies))

    # Create ALL dependency work items at once
    dep_items = []
    for dep in reversed(install_dependencies):  # Reverse for depth-first
        resolved_versions = self.resolve_versions(
            req=dep, req_type=RequirementType.INSTALL,
            return_all_versions=self.multiple_versions
        )
        for source_url, resolved_version in reversed(resolved_versions):
            dep_items.append(BootstrapWorkItem(
                req=dep,
                resolved_version=resolved_version,
                parent=(item.req, item.resolved_version),
                why_snapshot=self.why.copy(),
                phase=BootstrapPhase.START,
                ...
            ))

    item.phase = BootstrapPhase.CLEANUP
    return [item] + dep_items  # Stack handles depth-first
```

**Depth-first processing with multiple versions:**

Example: If dependency B resolves to versions [1.0, 2.0] and dependency C to \[3.0\]:

```
install_dependencies = [B, C]  # Process B first, then C

EXTRACT_INSTALL_DEPS creates ALL work items at once:
  For B: resolved_versions = [(2.0, url), (1.0, url)]
    Create: B-1.0@START, B-2.0@START (reversed)
  For C: resolved_versions = [(3.0, url)]
    Create: C-3.0@START

  Return: [parent@CLEANUP, C-3.0@START, B-2.0@START, B-1.0@START]
  (Note: reversed order for LIFO depth-first)

Stack pops and processes:
  B-1.0 → processes fully → complete
  B-2.0 → processes fully → complete
  C-3.0 → processes fully → complete
  parent@CLEANUP → continues

```

The LIFO stack ensures depth-first processing: each install dep (all its versions) and transitive deps complete before the next install dep. No phase looping needed!

### 7. Progress Bar Updates

**Current behavior** (lines 516, 522, 702, 709):

- `update_total()` increases total count when dependencies are discovered
- `update()` increments progress after EACH package completes (including building the package and processing all its transitive deps)

**Challenge**: In recursive code, `update()` is called when a child's recursive call returns. In iterative code, we don't have a return point—we just return work items from phases.

**Critical bug to avoid**: If phases call `update()`, already-seen packages get counted in `update_total()` but never reach the phase that calls `update()`, causing progress to get stuck.

**Solution**: Main loop calls `update()` when work item completes

The main loop is the only place that knows when a work item is done (phase returns `[]`):

```python
new_items = self._process_phase(item)
work_stack.extend(new_items)

# Update progress when work item completes (returns [])
if not new_items:
    self.progressbar.update()
```

**Two completion scenarios:**

1. **Already seen**: START returns `[]` → main loop calls `update()` → done
2. **Fully processed**: COMPLETE returns `[]` → main loop calls `update()` → done

**Update totals (unchanged):**

- Call `update_total(len(install_dependencies))` in EXTRACT_INSTALL_DEPS phase when install deps are discovered
- Call `update_total(len(build_dependencies))` in EXTRACT_BUILD_DEPS phase when build deps are discovered

**Why this works:**

Every work item created gets counted (via `update_total()`). Every work item eventually returns `[]` (either at START if already seen, or at COMPLETE if processed). Main loop calls `update()` for each `[]`, ensuring counts match.

**Example:**

```
Parent extracts deps [A, B, C] → calls update_total(3) → total = 3
A processes → COMPLETE returns [] → main loop calls update() → progress = 1
B already seen → START returns [] → main loop calls update() → progress = 2
C processes → COMPLETE returns [] → main loop calls update() → progress = 3
Result: 100% complete ✓
```

**Result**: Progress bar behaves identically to recursive version—updates after each package completes (including all its work and transitive dependencies).

### 8. Error Handling Preservation

All error handling modes work identically, but the mechanism changes from call-stack-based to work-stack-based.

#### Main Loop Error Handling

```python
while work_stack:
    item = work_stack.pop()

    # Restore parent context and push current item
    self.why = item.why_snapshot.copy()
    self.why.append((item.req_type, item.req, item.resolved_version))

    try:
        # Process phase - returns list of items to add to stack
        new_items = self._process_phase(item)
        work_stack.extend(new_items)
    except Exception as err:
        # Handle based on mode
        if self.test_mode:
            self._record_test_mode_failure(item.req, str(item.resolved_version), err, "bootstrap")
            # Continue to next work item (don't add anything to stack)
        elif self.multiple_versions:
            pkg_name = canonicalize_name(item.req.name)
            self._failed_versions.append((pkg_name, str(item.resolved_version), err))
            self.ctx.dependency_graph.remove_dependency(pkg_name, item.resolved_version)
            self.ctx.write_to_graph_to_file()
            # Continue to next work item (don't add anything to stack)
        else:
            raise  # Fail-fast in normal mode
    finally:
        # Always pop current item from self.why
        self.why.pop()
```

**Test mode**: Catch exceptions per work item, record failure, continue to next item (don't add items to stack)

**Multiple versions mode**: Catch exceptions per version, record failure, remove from graph, continue to next version (don't add items to stack)

**Normal mode**: Raise exceptions immediately (fail-fast)

#### Phase Handler Error Handling

Non-fatal errors (hooks, dependency extraction) are caught within phase handlers:

```python
def _phase_extract_install_deps(self, item):
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
    return [item]
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
  - Add `BootstrapWorkItem` dataclass and `BootstrapPhase` enum (5 phases)
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

- Add `BootstrapPhase` enum (5 phases including COMPLETE)
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
Item: requests-2.31.0

START:               Add edge to graph → not in _seen_requirements → mark as seen → advance to EXTRACT_BUILD_DEPS → return [item]
EXTRACT_BUILD_DEPS:  Extract build deps → advance to BUILD_PACKAGE → return [item] + build_dep_items
BUILD_PACKAGE:       Build sdist and wheel → record in build-order.json → advance to EXTRACT_INSTALL_DEPS → return [item]
EXTRACT_INSTALL_DEPS: Extract install deps → create work items for all deps → advance to COMPLETE → return [item] + install_dep_items
COMPLETE:            Clean build dirs → return []
Main loop:           Sees [] → calls progressbar.update() → done
```

**Example 2: Already seen package (duplicate dependency)**

```
Item: urllib3-2.0.0

START:      Add edge to graph → found in _seen_requirements → return []
Main loop:  Sees [] → calls progressbar.update() ← STOPS HERE!

(Item removed from pipeline - doesn't flow through remaining phases)
(Progress bar updated for duplicates - they were counted in update_total())
```

**Example 3: sdist_only mode**

```
Item: numpy-1.24.0, build_sdist_only=True

START:               Add edge to graph → not in _seen_requirements → mark as seen (sdist only) → advance to EXTRACT_BUILD_DEPS → return [item]
EXTRACT_BUILD_DEPS:  Extract build deps → advance to BUILD_PACKAGE → return [item] + build_dep_items
BUILD_PACKAGE:       build_sdist_only=True → build only sdist, skip wheel → record in build-order.json → advance to EXTRACT_INSTALL_DEPS → return [item]
EXTRACT_INSTALL_DEPS: Extract install deps from sdist → create work items for all deps → advance to COMPLETE → return [item] + install_dep_items
COMPLETE:            Clean build dirs → return []
Main loop:           Sees [] → calls progressbar.update() → done
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

### Linear Phase Progression with Early Exit

**Design choice:** Work items flow through phases linearly with START acting as a filter/gatekeeper.

**Benefits:**

1. **Optimized**: Reduced from 9 to 5 phases by:
   - Combining related operations (GRAPH_UPDATE+SEEN_CHECK→START, BUILD_PACKAGE+BUILD_ORDER→BUILD_PACKAGE, CLEANUP+COMPLETE→COMPLETE)
   - Eliminating PROCESS_INSTALL (stack handles depth-first automatically)
2. **Simplicity**: Straightforward pipeline with one decision point (START)
3. **Efficiency**: Already-seen packages stop at START (no flowing through 4 no-op phases)
4. **Correctness**: Matches recursive behavior - duplicates don't update progress bar
5. **No flag needed**: Eliminated `skip_processing` flag complexity

**How modes are handled:**

- **Already seen packages**: START returns `[]`, item stops flowing (early exit)
- **sdist_only mode**: BUILD_PACKAGE checks `item.build_sdist_only` flag and skips wheel build
- **Test mode errors**: EXTRACT_INSTALL_DEPS catches exceptions, uses empty list, continues to next phase
- **Multiple versions**: Each version is independent work item flowing through same phases

**Simple pipeline**: START (filter) → work phases → COMPLETE

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
04. **sdist_only mode**: BUILD_PACKAGE phase checks `item.build_sdist_only` flag and skips wheel build
05. **Already seen packages**: START returns `[]`, stopping the item from flowing through remaining phases (early exit)
06. **Progress bar correctness**: Main loop updates progress when work item completes (returns `[]`); handles both already-seen packages (counted but stop at START) and fully-processed packages (reach COMPLETE)
07. **Git URL requirements**: Resolution unchanged, happens before work item creation
08. **Deep chains**: Work stack on heap, no recursion limit
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
