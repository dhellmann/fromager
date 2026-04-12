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

### 0. Required Imports

Add these imports to `src/fromager/bootstrapper.py`:

```python
import dataclasses
from enum import StrEnum  # Python 3.11+ (or use `class BootstrapPhase(str, Enum)` for 3.9+)
```

### 1. Work Item Structure

Add a new dataclass to represent each package version to bootstrap:

```python
@dataclasses.dataclass
class BootstrapWorkItem:
    """Represents a single bootstrap work item (stack frame).

    Each work item processes one package version through multiple phases.
    Items flow through phases linearly with START phase determining if
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

**Phase Flow Diagram:**

```
Work Item Flow (Linear Pipeline with Early Exit):

┌─────────────┐
│   START     │ Check if already seen
└──────┬──────┘
       │ not seen
       ├─[already seen]─→ return [] → main loop calls update() → done
       │
       ▼
┌─────────────────────┐
│ EXTRACT_BUILD_DEPS  │ Collect build dependencies
└──────┬──────────────┘
       │ return [item] + build_deps
       │ (build deps process to completion first)
       ▼
┌─────────────────┐
│ BUILD_PACKAGE   │ Build sdist and/or wheel
└──────┬──────────┘
       │ return [item]
       ▼
┌────────────────────────┐
│ EXTRACT_INSTALL_DEPS   │ Collect install dependencies
└──────┬─────────────────┘
       │ return [item] + install_deps
       │ (install deps process to completion first)
       ▼
┌──────────┐
│ COMPLETE │ Cleanup
└──────┬───┘
       │ return []
       ▼
   main loop calls update() → done
```

### 3. External Interface (Unchanged)

The `bootstrap()` method maintains the same signature and behavior from the caller's perspective:

- **Input:** `bootstrap(req: Requirement, req_type: RequirementType) -> None`
- **Output:** None (modifies internal state, writes to files)
- **Behavior:** Bootstraps a package and all its dependencies
- **External callers:** No changes required in `commands/bootstrap.py` or other callers

### 4. Main Iterative Loop

Replace recursive `bootstrap()` with iterative loop:

```python
def bootstrap(self, req: Requirement, req_type: RequirementType) -> None:
    """Bootstrap a package and its dependencies (iterative)."""
    # Resolve versions (same as current)
    resolved_versions = self.resolve_versions(
        req=req, req_type=req_type,
        return_all_versions=self.multiple_versions
    )

    # Create initial work items using helper function
    work_stack = self._create_work_items_from_versions(
        req=req,
        req_type=req_type,
        resolved_versions=resolved_versions,
        build_sdist_only=self._should_build_sdist_only(req_type),
    )

    # Process work stack (LIFO = depth-first)
    while work_stack:
        item = work_stack.pop()

        # Restore dependency chain context
        self.why = item.why_snapshot.copy()

        # Use existing context manager for self.why management (reuse existing code)
        with self._track_why(item.req_type, item.req, item.resolved_version):
            try:
                # Process current phase - returns items to add to stack
                new_items = self._process_phase(item)
                work_stack.extend(new_items)

                # Update progress bar based on return value
                if len(new_items) == 0:
                    # Work completed (already seen or finished)
                    self.progressbar.update()
                elif len(new_items) == 1:
                    # Same item continuing to next phase - no change to total
                    pass
                else:
                    # Created dependencies: n items returned means n-1 net new work
                    self.progressbar.update_total(len(new_items) - 1)
            except Exception as err:
                # Error handling - don't add anything to stack
                if self.test_mode:
                    self._record_test_mode_failure(
                        item.req, str(item.resolved_version), err, "bootstrap"
                    )
                    # Still update progress - work item processed (failed)
                    self.progressbar.update()
                elif self.multiple_versions:
                    pkg_name = canonicalize_name(item.req.name)
                    self._failed_versions.append((pkg_name, str(item.resolved_version), err))
                    self.ctx.dependency_graph.remove_dependency(pkg_name, item.resolved_version)
                    self.ctx.write_to_graph_to_file()
                    # Still update progress - work item processed (failed)
                    self.progressbar.update()
                else:
                    raise  # Fail-fast in normal mode
```

### 4. Phase Processing

**Linear progression with early exit:** Work items flow through phases linearly. START acts as a filter - if a package has already been processed, it returns `[]` and the item stops flowing through the pipeline.

**Phase Handler Contract:**

```python
def _phase_X(self, item: BootstrapWorkItem) -> list[BootstrapWorkItem]:
    """Phase handler contract.

    Each phase handler:
    1. Performs phase-specific work (extract deps, build package, etc.)
    2. Modifies item.phase in-place to advance to next phase
    3. Returns list of work items to add to stack:
       - [] = stop processing this item (already seen or complete)
       - [item] = continue item to next phase
       - [item] + dep_items = add dependencies and continue
         (deps on top of stack due to LIFO, processed first)

    Phase handlers MUST NOT:
    - Update progress bar (main loop handles this)
    - Manipulate self.why (main loop handles this)
    - Manipulate work_stack (main loop handles this)
    """
```

**Phase handler patterns:**

1. Do work for this phase (may create dependency work items)
2. Check if processing should stop:
   - START: If already seen, return `[]` (stop processing)
   - COMPLETE: Always return `[]` (end of pipeline)
3. Otherwise, advance `item.phase` to next phase
4. Return list of items to add to stack:
   - Usually `[item]` (current item with next phase)
   - May include dependency items: `[item] + dep_items`
     - **LIFO ordering**: `item` goes first (bottom of stack), deps after (top of stack)
     - **Stack pops deps first** (they're on top), processes them to completion
     - **Then pops item** to continue to next phase
   - Return `[]` to stop processing this item

#### Phase Handlers:

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
    - Create work items for build deps in reverse order (SDIST, BACKEND, SYSTEM)
    - Build deps pop in correct order (SYSTEM, BACKEND, SDIST) and complete before parent continues
  - **Next:** Advance to BUILD_PACKAGE
  - **Return:** `[item] + build_dep_items`
    - `item` goes on stack first (bottom), deps on top
    - LIFO stack pops deps first, then item continues
    - Main loop sees n items returned → updates total by n-1 (net new work)

- **`_phase_build_package()`**: Build the package and record in build order

  - **Work:**
    - Call `_build_package()` to build sdist and/or wheel
    - Respects `item.build_sdist_only` flag (skip wheel if True)
    - Store result in `item.build_result`
    - Call `self._add_to_build_order()` to record in build-order.json
  - **Next:** Advance to EXTRACT_INSTALL_DEPS
  - **Return:** `[item]`

- **`_phase_extract_install_deps()`**: Extract install dependencies and create work items

  - **Work:**
    - Call `_get_install_dependencies()` to extract from wheel/sdist
    - Handle exceptions in test_mode (non-fatal, use empty list)
    - For each dependency (in reverse order for depth-first):
      - Resolve versions for that dependency
      - Create work items for dependency versions in reverse order
      - Stack handles depth-first processing automatically
  - **Next:** Advance to COMPLETE
  - **Return:** `[item] + dep_items` (all dependency work items at once)
    - `item` goes on stack first (bottom), deps on top
    - LIFO stack pops deps first, then item continues to COMPLETE
    - Main loop sees n items returned → updates total by n-1 (net new work)

- **`_phase_complete()`**: Clean up and mark package complete

  - **Work:**
    - Call cleanup hooks/methods
    - Clean build directories
  - **Next:** Done
  - **Return:** `[]` ← **End of pipeline! Don't add back to stack**
  - Main loop sees `[]` and calls `progressbar.update()` (work item done)

#### Phase Dispatcher Implementation

Add `_process_phase()` method to dispatch to appropriate phase handler:

```python
def _process_phase(self, item: BootstrapWorkItem) -> list[BootstrapWorkItem]:
    """Dispatch to appropriate phase handler based on item.phase.

    Returns list of work items to add to the stack.
    """
    if item.phase == BootstrapPhase.START:
        return self._phase_start(item)
    elif item.phase == BootstrapPhase.EXTRACT_BUILD_DEPS:
        return self._phase_extract_build_deps(item)
    elif item.phase == BootstrapPhase.BUILD_PACKAGE:
        return self._phase_build_package(item)
    elif item.phase == BootstrapPhase.EXTRACT_INSTALL_DEPS:
        return self._phase_extract_install_deps(item)
    elif item.phase == BootstrapPhase.COMPLETE:
        return self._phase_complete(item)
    else:
        raise ValueError(f"Unknown phase: {item.phase}")
```

#### Parent Context Helper

Add `_get_current_parent()` helper method for extracting parent from `self.why`:

```python
def _get_current_parent(self) -> tuple[Requirement, Version] | None:
    """Extract parent requirement and version from self.why stack.

    Returns None if self.why is empty (root package).
    Returns (parent_req, parent_version) if non-empty.
    """
    if not self.why:
        return None
    _, parent_req, parent_version = self.why[-1]
    return (parent_req, parent_version)
```

#### Work Item Creation Helper

Add `_create_work_items_from_versions()` helper to reduce code duplication:

```python
def _create_work_items_from_versions(
    self,
    req: Requirement,
    req_type: RequirementType,
    resolved_versions: list[tuple[str, Version]],
    build_sdist_only: bool = False,
) -> list[BootstrapWorkItem]:
    """Create work items from resolved versions in reverse order for LIFO.

    Args:
        req: The requirement to create work items for
        req_type: Type of requirement (INSTALL, BUILD_SYSTEM, etc.)
        resolved_versions: List of (source_url, version) tuples
        build_sdist_only: If True, only build sdist (skip wheel)

    Returns:
        List of work items in reverse order (for LIFO stack)
    """
    work_items = []
    for source_url, resolved_version in reversed(resolved_versions):
        work_items.append(BootstrapWorkItem(
            req=req,
            req_type=req_type,
            source_url=source_url,
            resolved_version=resolved_version,
            parent=self._get_current_parent(),
            why_snapshot=self.why.copy(),
            build_sdist_only=build_sdist_only,
            phase=BootstrapPhase.START,
        ))
    return work_items
```

**Usage locations:**

1. `bootstrap()` method - creating initial work items
2. `_phase_extract_build_deps()` - creating build dependency work items (3 calls)
3. `_phase_extract_install_deps()` - creating install dependency work items

This eliminates code duplication and ensures consistent work item creation.

### 5. Build Dependencies Handling

**Critical**: Build dependencies must complete BEFORE building the package.

**Build Dependency Ordering:**

Build dependencies have strict ordering requirements because each type needs the previous types installed:

1. **BUILD_SYSTEM** must be installed before querying build backend
2. **BUILD_BACKEND** requirements need build system installed
3. **BUILD_SDIST** requirements need both above installed

We create work items in **REVERSE order** (SDIST, BACKEND, SYSTEM) so the LIFO stack pops them in **CORRECT order** (SYSTEM, BACKEND, SDIST). Each build dep flows through all 5 phases to completion before the next.

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
After EXTRACT_BUILD_DEPS: [parent@BUILD_PACKAGE, SDIST@START, BACKEND@START, SYSTEM@START]
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

```python
def _phase_extract_install_deps(self, item):
    # Extract dependencies
    install_dependencies = self._get_install_dependencies(...)

    # Create ALL dependency work items at once using helper
    dep_items = []
    for dep in reversed(install_dependencies):  # Reverse for depth-first
        resolved_versions = self.resolve_versions(
            req=dep, req_type=RequirementType.INSTALL,
            return_all_versions=self.multiple_versions
        )
        # Use helper to create work items for this dependency
        dep_items.extend(self._create_work_items_from_versions(
            req=dep,
            req_type=RequirementType.INSTALL,
            resolved_versions=resolved_versions,
            build_sdist_only=False,
        ))

    item.phase = BootstrapPhase.COMPLETE
    # Return [item] + dep_items
    # Main loop handles progress: if n items returned, update_total(n-1)
    return [item] + dep_items
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

  Return: [parent@COMPLETE, C-3.0@START, B-2.0@START, B-1.0@START]
  (Note: reversed order for LIFO depth-first)

Stack pops and processes:
  B-1.0 → processes fully → complete
  B-2.0 → processes fully → complete
  C-3.0 → processes fully → complete
  parent@COMPLETE → continues

```

The LIFO stack ensures depth-first processing: each install dep (all its versions) and transitive deps complete before the next install dep.

### 7. Progress Bar Updates

**Simplified approach**: All progress bar logic centralized in the main loop. Phase handlers never touch the progress bar.

**Main loop logic based on return value:**

```python
new_items = self._process_phase(item)
work_stack.extend(new_items)

# Update progress bar based on return value
if len(new_items) == 0:
    # Work completed (already seen or finished)
    self.progressbar.update()
elif len(new_items) == 1:
    # Same item continuing to next phase - no change to total
    pass
else:
    # Created dependencies: n items returned means n-1 net new work
    self.progressbar.update_total(len(new_items) - 1)
```

**Why this works:**

- **0 items returned**: Work item completed (START found already-seen, or COMPLETE finished processing) → increment progress
- **1 item returned**: Same work item advancing to next phase (e.g., BUILD_PACKAGE → EXTRACT_INSTALL_DEPS) → no change (just continuing existing work)
- **n items returned (n > 1)**: Work item created dependencies → net new work = n - 1
  - Had 1 work item (parent), now have n items (parent + dependencies)
  - Net increase = n - 1 new items to process
  - Update total to reflect new work discovered

**Example 1: Item with 3 dependencies**

```
EXTRACT_INSTALL_DEPS returns [item, dep1, dep2, dep3] (4 items)
Main loop: len(new_items) = 4 → update_total(4 - 1 = 3)
Logic: Had 1 item, now have 4 → +3 net new work ✓
```

**Example 2: Already seen package**

```
START returns [] (0 items)
Main loop: len(new_items) = 0 → update()
Logic: Work completed without creating dependencies ✓
```

**Example 3: Item advancing to next phase**

```
BUILD_PACKAGE returns [item] (1 item)
Main loop: len(new_items) = 1 → no change
Logic: Same work continuing, not creating new work ✓
```

**Advantages:**

1. **Separation of concerns**: Phase handlers focus on business logic, main loop handles progress
2. **Automatic correctness**: Math works automatically based on return values
3. **No manual tracking**: Phase handlers don't need to count dependencies
4. **Centralized logic**: All progress bar handling in one place

**Result**: Progress bar behaves identically to recursive version—updates after each package completes, total grows as dependencies are discovered.

#### Shared Dependencies and Progress Tracking

**Important**: Because the dependency graph is discovered dynamically and dependencies can be shared, multiple work items may be created for the same package version.

**Example**: If packages A and B both depend on C, the work stack will have:

- Work item for C created by A's EXTRACT_INSTALL_DEPS phase
- Work item for C created by B's EXTRACT_INSTALL_DEPS phase

**Behavior**:

1. Both work items are added to stack and increment progress total
2. First work item for C (whichever is popped first) builds the package and marks it as seen
3. Second work item for C hits the `_has_been_seen()` check in START phase and returns []
4. **Both work items increment progress** when they complete (return [])

**Why this is correct**: Progress tracks "dependency work items processed", not "unique packages built". Each work item represents work to verify a dependency is satisfied, even if the package was already built by another work item.

**Progress bar semantics**: "N of M dependency relationships satisfied"

- M grows as dependencies are discovered (via `update_total()`)
- N increments for each work item completed (via `update()`)
- Multiple work items for same package are legitimate (shared dependencies)

#### Progress Bar Initialization

**Keep unchanged**: The current recursive implementation in `commands/bootstrap.py` initializes the progress bar with `total=len(to_build) * 2`. This refactoring preserves existing behavior, so **keep this initialization unchanged**.

Optimizing the progress bar initialization can be addressed in a separate PR after verifying the iterative version works correctly.

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

    # Create dependency work items (if any)
    # ... create dep_items from install_dependencies ...

    item.phase = BootstrapPhase.COMPLETE
    return [item] + dep_items  # Main loop handles progress update
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
  - Add 5 new phase handler methods:
    - `_phase_start()`
    - `_phase_extract_build_deps()`
    - `_phase_build_package()`
    - `_phase_extract_install_deps()`
    - `_phase_complete()`
  - Add `_process_phase()` dispatcher method
  - Add `_get_current_parent()` helper for extracting parent from `self.why`
  - Add `_create_work_items_from_versions()` helper for creating work items from resolved versions
  - Update main loop with error handling and self.why management (using existing `_track_why()` context manager)
  - **Note**: `_build_stack` field (line 116) is unchanged - still used for build-order.json output via `_add_to_build_order()`

**Reference files (read-only, for patterns):**

- `src/fromager/dependency_graph.py` (lines 217-255) - Stack-based DFS traversal pattern
- `src/fromager/commands/graph.py` (lines 136-179) - Stack with context preservation
- `src/fromager/requirements_file.py` (lines 14-29) - RequirementType enum

**Test files (for verification):**

- `tests/test_bootstrapper.py` - All existing tests must pass unchanged

## Implementation Steps

**Direct replacement approach:** This is a pure refactoring (behavior-preserving change), so we'll replace the recursive implementation with the iterative version in one change. The comprehensive test suite provides sufficient safety net.

### Step 1: Add Data Structures

- Add required imports: `dataclasses`, `StrEnum` (or `str, Enum` for Python 3.9+)
- Add `BootstrapPhase` enum (5 phases)
- Add `BootstrapWorkItem` dataclass
- Run type check: `hatch run mypy:check src/fromager/bootstrapper.py`

### Step 2: Add Phase Handler Methods

- Implement 5 `_phase_*()` methods by extracting logic from existing code:
  - `_phase_start()` - from `_bootstrap_impl()` lines 392-405
  - `_phase_extract_build_deps()` - from `_handle_build_requirements()` lines 696-709
  - `_phase_build_package()` - from `_bootstrap_impl()` lines 406-470
  - `_phase_extract_install_deps()` - from `_bootstrap_impl()` lines 480-526
  - `_phase_complete()` - new cleanup phase
- Implement `_process_phase()` dispatcher
- Implement `_get_current_parent()` helper
- Implement `_create_work_items_from_versions()` helper
- Run type check: `hatch run mypy:check src/fromager/bootstrapper.py`

### Step 3: Replace bootstrap() Method

- Replace `bootstrap()` method with iterative implementation (main loop)
- Remove `_bootstrap_single_version()`, `_bootstrap_impl()`, `_handle_build_requirements()`
- **Keep** `commands/bootstrap.py` progress bar initialization unchanged (preserve existing behavior)
- Run type check and lint: `hatch run mypy:check && hatch run lint:fix`

### Step 4: Run Tests and Verify

- Run file-scoped tests: `hatch run test:test tests/test_bootstrapper.py -v`
- Run full test suite: `hatch run test:test`
- Run e2e tests: `hatch run test:test e2e/`
- All tests must pass unchanged

### Step 5: Commit Changes

- Use conventional commit format: `refactor(bootstrapper): convert from recursive to iterative`
- Include signed-off-by: `git commit -s`
- Reference any related issues

## Self.why Stack Management

**Challenge**: `self.why` is a shared stack tracking the current dependency chain. The recursive version manages it via the call stack.

**Solution**: Snapshot and restore approach using existing `_track_why()` context manager

1. Each work item captures `why_snapshot = self.why.copy()` at creation time (preserves parent context)
2. Main loop restores parent context: `self.why = item.why_snapshot.copy()`
3. Main loop uses existing `_track_why()` context manager to push/pop current item
4. Context manager ensures cleanup even on exceptions (existing code, lines 527-543)
5. **Phase handlers MUST NOT manipulate `self.why`** - they see current item already on the stack

**Main loop uses existing context manager:**

```python
while work_stack:
    item = work_stack.pop()

    # Restore parent context from snapshot
    self.why = item.why_snapshot.copy()

    # Use existing context manager (reuses existing code)
    with self._track_why(item.req_type, item.req, item.resolved_version):
        try:
            new_items = self._process_phase(item)
            work_stack.extend(new_items)
            # ... handle progress bar updates ...
        except Exception as err:
            # ... handle error based on mode ...
```

**Advantages of using existing context manager:**

- Reuses existing, tested code
- Ensures correct cleanup via try/finally
- Same push/pop semantics as recursive version

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

Run the complete test suite to verify all existing behavior is preserved:

```bash
# Run specific bootstrapper tests with verbose output
hatch run test:test tests/test_bootstrapper.py -v

# Run specific test function
hatch run test:test tests/test_bootstrapper.py::test_multiple_versions_continues_on_error -v

# Run with debug logging
hatch run test:test tests/test_bootstrapper.py --log-level DEBUG

# Run full test suite
hatch run test:test

# Run e2e tests
hatch run test:test e2e/
```

All tests must pass unchanged.

### Type Checking and Linting

```bash
# Type check bootstrapper file
hatch run mypy:check src/fromager/bootstrapper.py

# Format code
hatch run lint:fix src/fromager/bootstrapper.py

# Run all quality checks
hatch run lint:fix && hatch run test:test && hatch run mypy:check && hatch run lint:check
```

### Error Mode Testing

Verify all error handling modes work correctly:

1. **Normal mode:** Verify fail-fast still works (exception propagates immediately)
2. **Test mode:**
   - Verify prebuilt fallback works for build failures
   - Verify non-fatal errors (hooks, deps) are recorded but continue
   - Verify failure report JSON is written
   - Test coverage: `tests/test_bootstrap_test_mode.py`
3. **Multiple versions mode:**
   - Verify all matching versions are processed
   - Verify failures are recorded per-version
   - Verify failed nodes are removed from graph
   - Verify other versions continue after one fails
   - Test coverage: `test_multiple_versions_continues_on_error`

### Stress Testing (Recommended Before Production Use)

Test scenarios not explicitly covered by current test suite:

**1. Deep recursion chains:**

Create test with 500+ level deep dependency chain:

- Previous recursive implementation would hit stack overflow
- Verify iterative version handles it without issues
- Check memory usage stays reasonable

**2. Wide dependency trees:**

Test package with 100+ direct dependencies:

- Verify work stack doesn't grow excessively
- Monitor memory usage (each work item ~500 bytes)
- 1000 parallel deps ≈ 500KB additional memory (acceptable)

**3. Circular dependencies:**

Verify `_seen_requirements` prevents infinite loops:

- Already tested implicitly by existing test suite
- Document expected behavior for manual verification

**4. Large multiple versions:**

Test with package having 50+ matching versions:

- Current tests only cover 2-3 versions
- Verify performance remains acceptable
- Check that all versions are processed or errors recorded

### Performance Testing

Compare with historical baselines:

1. **Execution time:** Should be similar (within 10% of typical runs)
2. **Memory usage:** Should be similar or better than recursive version
3. **Profile if needed:** Use Python profiler to identify any bottlenecks

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
Main loop:  Sees 0 items → calls update() ← STOPS HERE!

(Item removed from pipeline - doesn't flow through remaining phases)
(Progress bar updated - this work item is now complete)
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

Used in the START phase to break cycles and avoid redundant work.

### Progress Bar Behavior

**Initialization:** If not provided to constructor, creates `progress.Progressbar(None)` (line 100)

**Total tracking:** Updated automatically by main loop based on phase return values

- Phase returns n items (n > 1): Main loop calls `update_total(n - 1)` for net new work
- Phase handlers never touch progress bar

**Progress tracking:** Incremented after each work item completes

- Phase returns 0 items: Main loop calls `update()` (work completed)
- Both already-seen packages (START → []) and finished packages (COMPLETE → []) increment progress

**Result:** Progress bar shows "N of M packages processed" where M grows automatically as dependencies are discovered.

### Build Result Storage

`item.build_result: SourceBuildResult | None` stores the result from `_build_package()`. Currently not used for control flow, but available for:

- Error reporting
- Future optimizations
- Debugging

### Linear Phase Progression with Early Exit

**Design choice:** Work items flow through phases linearly with START acting as a filter/gatekeeper.

**Benefits:**

1. **Simplicity**: Straightforward pipeline with one decision point (START)
2. **Efficiency**: Already-seen packages stop at START and don't flow through remaining phases
3. **Correctness**: Matches recursive behavior - duplicates don't update progress bar

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

Git history provides rollback capability:

- **If issues discovered:** Use `git revert` to roll back the commit
- **Git provides:** Complete history of recursive implementation for reference
- **Test suite provides:** Safety net to catch issues before merge
- **No feature flag needed:** This is a pure refactoring (behavior-preserving change)
  - Feature flags make sense for behavior changes
  - For refactoring, passing tests = correct implementation
  - Keeping both implementations doubles maintenance burden unnecessarily

## Success Criteria

1. All existing tests pass unchanged
2. Bootstrap same packages produces identical output files
3. Can bootstrap packages with arbitrarily deep dependency graphs
4. No regressions in test_mode, multiple_versions, or sdist_only modes
5. Performance within 10% of recursive version
