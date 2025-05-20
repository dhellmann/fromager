#!/bin/bash
# -*- indent-tabs-mode: nil; tab-width: 2; sh-indentation: 2; -*-

# Test to show we can build a single wheel, if the dependencies are
# available.

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPTDIR/common.sh"

# What are we building?
DIST="stevedore"
VERSION="5.2.0"

# Install hook for test
pip install e2e/fromager_hooks

# Bootstrap the test project
fromager \
    --sdists-repo="$OUTDIR/sdists-repo" \
    --wheels-repo="$OUTDIR/wheels-repo" \
    --work-dir="$OUTDIR/work-dir" \
    bootstrap "${DIST}==${VERSION}"

# Remove traces of stevedore
rm "$OUTDIR/wheels-repo/downloads/${DIST}"*
rm "$OUTDIR/sdists-repo/downloads/${DIST}"*
rm -r "$OUTDIR/work-dir/${DIST}"*

# Remove stevedore from the local index
rm -r "$OUTDIR/wheels-repo/simple/stevedore/"

# Rebuild the wheel
fromager \
    --log-file="$OUTDIR/build.log" \
    --sdists-repo="$OUTDIR/sdists-repo" \
    --wheels-repo="$OUTDIR/wheels-repo" \
    --work-dir="$OUTDIR/work-dir" \
    --settings-dir="$SCRIPTDIR/build_settings" \
    build "${DIST}" "${VERSION}" "unreachable_url"

EXPECTED_FILES="
wheels-repo/downloads/stevedore-5.2.0-0-py3-none-any.whl
sdists-repo/downloads/stevedore-custom-5.2.0.tar.gz
sdists-repo/builds/stevedore-5.2.0.tar.gz
sdists-repo/builds/test-output-file.txt
build.log
"

pass=true
for f in $EXPECTED_FILES; do
  if [ ! -f "$OUTDIR/$f" ]; then
    echo "FAIL: Did not find $OUTDIR/$f" 1>&2
    pass=false
  fi
done

if $pass; then
  if ! grep -q "${DIST}==${VERSION}" $OUTDIR/sdists-repo/builds/test-output-file.txt; then
    echo "FAIL: Did not find content in post-build hook output file" 1>&2
    pass=false
  fi
fi

$pass
