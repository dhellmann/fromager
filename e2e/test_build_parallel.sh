#!/bin/bash
# -*- indent-tabs-mode: nil; tab-width: 2; sh-indentation: 2; -*-

# Test build-sequence (with and without skipping already built wheel)

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPTDIR/common.sh"

# What are we building?
DIST="imapautofiler"
VERSION="1.14.0"

# # Bootstrap the test project
# fromager \
#     --sdists-repo="$OUTDIR/sdists-repo" \
#     --wheels-repo="$OUTDIR/wheels-repo" \
#     --work-dir="$OUTDIR/work-dir" \
#     --settings-dir="$SCRIPTDIR/build-parallel" \
#     bootstrap "${DIST}==${VERSION}"

# Save the build order file but remove everything else.
# cp "$OUTDIR/work-dir/graph.json" "$OUTDIR/"

# Copy the cached graph file to the working directory
cp "$SCRIPTDIR/build-parallel/graph.json" "$OUTDIR/graph.json"

# Build everything a first time
log="$OUTDIR/build-logs/${DIST}-build.log"
fromager \
    --log-file "$log" \
    --work-dir "$OUTDIR/work-dir" \
    --sdists-repo "$OUTDIR/sdists-repo" \
    --wheels-repo "$OUTDIR/wheels-repo" \
    --settings-dir="$SCRIPTDIR/build-parallel" \
    build-parallel "$OUTDIR/graph.json"

if ! grep -q "ready to build cython" "$log"; then
  echo "Did not find message indicating build of cython would start" 1>&2
  pass=false
fi
if ! grep -q "cython: requires exclusive build" "$log"; then
  echo "Did not find message indicating build of cython would run on its own" 1>&2
  pass=false
fi

# Rebuild everything even if it already exists
log="$OUTDIR/build-logs/${DIST}-build.log"
fromager \
    --log-file "$log" \
    --work-dir "$OUTDIR/work-dir" \
    --sdists-repo "$OUTDIR/sdists-repo" \
    --wheels-repo "$OUTDIR/wheels-repo" \
    --settings-dir="$SCRIPTDIR/build-parallel" \
    build-parallel --force "$OUTDIR/graph.json"

find "$OUTDIR/wheels-repo/"

if grep -q "skipping building wheel for $DIST" "$log"; then
  echo "Found message indicating build of $DIST was skipped" 1>&2
  pass=false
fi


EXPECTED_FILES="
$OUTDIR/wheels-repo/downloads/setuptools-*.whl
$OUTDIR/wheels-repo/downloads/$DIST-*.whl

$OUTDIR/sdists-repo/downloads/$DIST-*.tar.gz
$OUTDIR/sdists-repo/downloads/setuptools-*.tar.gz

$OUTDIR/work-dir/logs/$DIST-*.log
$OUTDIR/work-dir/logs/setuptools-*.log

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

$pass

# Rebuild everything while reusing existing local wheels
log="$OUTDIR/build-logs/${DIST}-build-skip.log"
fromager \
    --log-file "$log" \
    --work-dir "$OUTDIR/work-dir" \
    --sdists-repo "$OUTDIR/sdists-repo" \
    --wheels-repo "$OUTDIR/wheels-repo" \
    --settings-dir="$SCRIPTDIR/build-parallel" \
    build-parallel "$OUTDIR/graph.json"

find "$OUTDIR/wheels-repo/"

if ! grep -q "skipping builds for versions of packages available" "$log"; then
  echo "Did not find message indicating builds would be skipped" 1>&2
  pass=false
fi
if ! grep -q "skipping building wheel for $DIST" "$log"; then
  echo "Did not find message indicating build of $DIST was skipped" 1>&2
  pass=false
fi

$pass

# Rebuild everything while reusing wheels from external server
rm -rf $OUTDIR/wheels-repo
log="$OUTDIR/build-logs/${DIST}-build-skip-env.log"
fromager \
    --log-file "$log" \
    --work-dir "$OUTDIR/work-dir" \
    --sdists-repo "$OUTDIR/sdists-repo" \
    --wheels-repo "$OUTDIR/wheels-repo" \
    build-parallel --cache-wheel-server-url="https://pypi.org/simple" "$OUTDIR/graph.json"

find "$OUTDIR/wheels-repo/"

if ! grep -q "skipping builds for versions of packages available" "$log"; then
  echo "Did not find message indicating builds would be skipped" 1>&2
  pass=false
fi
if ! grep -q "skipping building wheel for $DIST" "$log"; then
  echo "Did not find message indicating build of $DIST was skipped" 1>&2
  pass=false
fi

$pass
