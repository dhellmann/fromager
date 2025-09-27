#!/bin/bash
# -*- indent-tabs-mode: nil; tab-width: 2; sh-indentation: 2; -*-

# Test build-sequence (with and without skipping already built wheel)

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPTDIR/common.sh"

# What are we building?
DIST="stevedore"
VERSION="5.2.0"

BUILD_SUMMARY_FILE="$OUTDIR/work-dir/build-sequence-summary.json"

# Bootstrap the test project
fromager \
    --debug \
    --sdists-repo="$OUTDIR/sdists-repo" \
    --wheels-repo="$OUTDIR/wheels-repo" \
    --work-dir="$OUTDIR/work-dir" \
    --settings-dir="$SCRIPTDIR/changelog_settings" \
    bootstrap "${DIST}==${VERSION}"

# Verify that build-sequence-summary.json reports that stevedore was built.
if [ -f "$BUILD_SUMMARY_FILE" ]; then
  skipped=$(jq '.[] | select(.name=="stevedore") | .skipped' "$BUILD_SUMMARY_FILE")
  if [ "$skipped" != "false" ]; then
    echo "FAIL: build-sequence-summary.json has incorrect skipped status $skipped for stevedore, should be false" 1>&2
    pass=false
  fi
fi

# Save the build order file but remove everything else.
cp "$OUTDIR/work-dir/build-order.json" "$OUTDIR/"

# Rebuild everything even if it already exists
log="$OUTDIR/build-logs/${DIST}-build.log"
fromager \
    --debug \
    --log-file "$log" \
    --work-dir "$OUTDIR/work-dir" \
    --sdists-repo "$OUTDIR/sdists-repo" \
    --wheels-repo "$OUTDIR/wheels-repo" \
    --settings-dir="$SCRIPTDIR/changelog_settings" \
    build-sequence --force "$OUTDIR/build-order.json"

find "$OUTDIR/wheels-repo/"

if grep -q "skipping building wheel for stevedore" "$log"; then
  echo "Found message indicating build of stevedore was skipped" 1>&2
  pass=false
fi

EXPECTED_FILES="
$OUTDIR/wheels-repo/downloads/setuptools-*.whl
$OUTDIR/wheels-repo/downloads/pbr-*.whl
$OUTDIR/wheels-repo/downloads/stevedore-*.whl

$OUTDIR/sdists-repo/downloads/stevedore-*.tar.gz
$OUTDIR/sdists-repo/downloads/setuptools-*.tar.gz
$OUTDIR/sdists-repo/downloads/pbr-*.tar.gz

$OUTDIR/work-dir/logs/stevedore-*.log
$OUTDIR/work-dir/logs/setuptools-*.log
$OUTDIR/work-dir/logs/pbr-*.log

$OUTDIR/work-dir/build-sequence-summary.md
$OUTDIR/work-dir/build-sequence-summary.json
"

pass=true
for pattern in $EXPECTED_FILES; do
  if [ ! -f "${pattern}" ]; then
    echo "Did not find $pattern" 1>&2
    pass=false
  fi
done

# Verify that build-sequence-summary.json has correct skipped status for force builds
# When using --force, all packages should be built (skipped: false)
if [ -f "$OUTDIR/work-dir/build-sequence-summary.json" ]; then
  # Check that all entries have skipped: false
  not_skipped_count=$(jq '[.[] | select(.skipped == false)] | length' "$OUTDIR/work-dir/build-sequence-summary.json")
  total_count=$(jq 'length' "$OUTDIR/work-dir/build-sequence-summary.json")
  if [ "$not_skipped_count" != "$total_count" ]; then
    echo "Expected all $total_count packages to be built (not skipped), but only $not_skipped_count were marked as not skipped" 1>&2
    echo "Contents of build-sequence-summary.json:" 1>&2
    jq '.' "$OUTDIR/work-dir/build-sequence-summary.json" 1>&2
    pass=false
  fi
else
  echo "build-sequence-summary.json file not found after force build" 1>&2
  pass=false
fi

$pass

# Rebuild everything while reusing existing local wheels
log="$OUTDIR/build-logs/${DIST}-build-skip.log"
fromager \
    --debug \
    --log-file "$log" \
    --work-dir "$OUTDIR/work-dir" \
    --sdists-repo "$OUTDIR/sdists-repo" \
    --wheels-repo "$OUTDIR/wheels-repo" \
    --settings-dir="$SCRIPTDIR/changelog_settings" \
    build-sequence "$OUTDIR/build-order.json"

find "$OUTDIR/wheels-repo/"

if ! grep -q "skipping builds for versions of packages available" "$log"; then
  echo "Did not find message indicating builds would be skipped" 1>&2
  pass=false
fi
if ! grep -q "${DIST}-${VERSION}: using existing wheel" "$log"; then
  echo "Did not find message indicating build of stevedore was skipped" 1>&2
  pass=false
fi

# Verify that build-sequence-summary.json reports that stevedore was not built.
if [ -f "$BUILD_SUMMARY_FILE" ]; then
  skipped=$(jq '.[] | select(.name=="stevedore") | .skipped' "$BUILD_SUMMARY_FILE")
  if [ "$skipped" != "true" ]; then
    echo "FAIL: build-sequence-summary.json has incorrect skipped status $skipped for stevedore, should be true" 1>&2
    pass=false
  fi
fi

$pass

# Rebuild everything while reusing wheels from external server
rm -rf "$OUTDIR"/wheels-repo
log="$OUTDIR/build-logs/${DIST}-build-skip-env.log"
fromager \
    --debug \
    --log-file "$log" \
    --work-dir "$OUTDIR/work-dir" \
    --sdists-repo "$OUTDIR/sdists-repo" \
    --wheels-repo "$OUTDIR/wheels-repo" \
    build-sequence --cache-wheel-server-url="https://pypi.org/simple" "$OUTDIR/build-order.json"

find "$OUTDIR/wheels-repo/"

if ! grep -q "skipping builds for versions of packages available" "$log"; then
  echo "Did not find message indicating builds would be skipped" 1>&2
  pass=false
fi
if ! grep -q "skipping building wheel since ${DIST}-${VERSION}-" "$log"; then
  echo "Did not find message indicating build of stevedore was skipped" 1>&2
  pass=false
fi

# Verify that build-sequence-summary.json has correct skipped status
# When using --cache-wheel-server-url with PyPI, all packages should be skipped
if [ -f "$OUTDIR/work-dir/build-sequence-summary.json" ]; then
  # Check that all entries have skipped: true
  skipped_count=$(jq '[.[] | select(.skipped == true)] | length' "$OUTDIR/work-dir/build-sequence-summary.json")
  total_count=$(jq 'length' "$OUTDIR/work-dir/build-sequence-summary.json")
  if [ "$skipped_count" != "$total_count" ]; then
    echo "Expected all $total_count packages to be skipped, but only $skipped_count were marked as skipped" 1>&2
    echo "Contents of build-sequence-summary.json:" 1>&2
    jq '.' "$OUTDIR/work-dir/build-sequence-summary.json" 1>&2
    pass=false
  fi
else
  echo "build-sequence-summary.json file not found" 1>&2
  pass=false
fi

$pass
