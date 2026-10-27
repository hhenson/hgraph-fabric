# Split-head publication and recovery review

Reviewed baseline: `c4d01d8` (PR #40).

No production defect was reproduced in the split-pointer protocol under the
storage contract below. The review adds seven regression tests containing 39
fault and interleaving scenarios to the common object-store contract. They run
against real memory and filesystem stores, and against S3 when configured.

## Storage and delivery assumptions

Publication creates are atomic and fail when the destination already exists.
Completed creates are visible to subsequent reads. Writers use the public
publication API; versions and per-version revision counts are contiguous.
Published records and referenced payloads are retained. A deleted record is
an integrity failure, not a simulated interrupted write.

Each publisher has its own cache. Incoming notifications are applied through
the normal coordinated cache/session path. Storage or transport exceptions
propagate. Recovery is an explicit restart operation, not an assumption that
the failed publication transparently continued.

## Publication boundaries

The fault matrix covers first publication, successor publication, a revision
on the latest version, and a revision on an older version. For each operation,
raise an error both immediately before and immediately after every store write:

- A new table writes its unique payload, revision-zero descriptor, per-version
  revision hint, and dataset version hint.
- A dependency revision writes its immutable record and per-version hint.

An error after a successful write models an acknowledgement lost after storage
accepted the object. The oracle treats the descriptor/revision create as the
commit, independently of the two hints. Restarted readers must recover exactly
the committed records, with the correct table. No notification is sent after a
storage error. An abandoned payload must not consume a version number.

A repeated table publication is a new publication, even if the earlier call
committed and then raised. It therefore receives the next version. A repeated
dependency publication with unchanged dependencies is a no-op. Recovery lets the
application inspect and deliver committed results before deciding whether to
publish again; the protocol does not make table retries idempotent.

## Concurrent publication and recovery

The added interleavings establish these behaviours:

- A publisher pauses after committing. Another publisher commits its successor,
  and recovery sees the successor. The first publisher then resumes and writes
  an older hint. Fresh lookup still finds the successor, and reversed notification
  delivery does not regress a reader's head. Both tables or revision records
  remain intact.
- Recovery probes the next record and finds it absent. A writer commits that
  record before recovery repairs the hint. Recovery may return the earlier
  snapshot; the normal notification and the next lookup still expose the new
  publication. Replaying the older recovery result does not regress a cache.
- Recovery bounds its version scan, then another writer publishes a new version
  and revises its predecessor. The scan can return the predecessor's newer
  revision without including the new version. Coordinated notifications retain
  both heads, and another recovery pass returns both versions.
- A polling publisher selects a version before another writer publishes a new
  table. The dependency write stays on its originally selected version. If the
  competing write instead revises that version, the expected-count check fails;
  the stale attempt neither rebases nor sends a notification.

Existing tests additionally cover actual parallel writers competing for the
same publication, payload/metadata ownership, simultaneous version and revision
commits in either order, malformed and regressed hints, and historical conflict
watches releasing a consistent cut after an older version is revised.

## Recovery and notification failures

Recovery is interrupted before and after each of its pointer writes. Retrying
recovery returns the same publication heads, leaves every immutable object
unchanged, and permits later revisions of an older version.

A transport send can fail after publication commits. Explicit recovery returns
that publication, allowing an already-initialised session to advance. A new
version delivers its table; a dependency-only revision updates the selected
revision without reporting a new table. Duplicate recovery delivery produces
no additional change set.

Recovery scans what it observes; it is not an atomic snapshot of all writers.
Publications beyond the scan still require their normal notification or a later
recovery pass. Pointer repair alone is not notification recovery.

## Validation

The installed-package suites passed on Python 3.12 and 3.14: 484 tests each,
with 48 opt-in integration skips (40 S3 and eight broker tests). The storage
contract passed 80 memory/filesystem tests; its 40 S3 variants were skipped.

Run the common storage contract with:

```sh
python -I -m unittest discover -s tests -p test_object_store_cache.py
```

These tests exercise object-store operations and deterministic scheduling
boundaries. They do not simulate storage violating atomic-create semantics,
validate physical power-loss durability, or claim live S3 coverage when the
S3 test configuration is absent.
