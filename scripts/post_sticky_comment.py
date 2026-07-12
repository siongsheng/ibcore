#!/usr/bin/env python3
"""Post ONE sticky comment on a PR — create it, or edit it in place.

Shared by panel's CI agent-reviewers (architecture-review, findings-ledger):
the agent composes a markdown body to a file, then this script normalizes it
(strips a wrapping ``` code fence + leading blank lines so the marker lands on
line 1), asserts the marker, and posts it as a single comment identified by that
marker (its exact first line). On a later run it finds that comment by the marker
and PATCHes it, so the PR carries exactly ONE of each reviewer's comments instead
of a new one per push.

It replaces the awk/bash "Post" state machine that was previously inlined and
duplicated across four workflow files, with the fence/marker/dedup edge cases
unit-tested (see test_post_sticky_comment.py). Matches this repo's
scripts/deepseek_review.py convention: stdlib only, shells out to `gh`.

Usage:
    python3 scripts/post_sticky_comment.py <pr-number> \
        --marker "## 📋 Review Findings Ledger" --body-file path/to/body.md

ADVISORY: the reviewers are non-blocking, so a missing/malformed body or a
transient `gh api` failure is surfaced as a loud GitHub Actions ::error:: /
::warning:: annotation but still exits 0 -- it must never fail the build.
"""

import argparse
import json
import os
import pathlib
import subprocess
import sys
import tempfile


def normalize(text: str) -> str:
    """Strip a wrapping ``` code fence and leading blank lines.

    An over-formatted agent may wrap the whole body in a fenced block or add
    leading blanks, which would push the marker off line 1. Strip a leading
    fence + leading blanks; strip the matching trailing fence ONLY when a
    leading fence was stripped, so a body that legitimately ENDS with a code
    block keeps its closing fence. Returns the body with no trailing newline.
    """
    lines = text.split("\n")
    stripped_lead = False
    out: list[str] = []
    started = False
    for line in lines:
        if not started:
            if line.strip().startswith("```"):
                stripped_lead = True
                continue
            if line.strip() == "":
                continue
            started = True
        out.append(line)
    # Drop trailing blank lines first, so the closing fence (if any) is last.
    while out and out[-1].strip() == "":
        out.pop()
    if stripped_lead and out and out[-1].strip() == "```":
        out.pop()
        while out and out[-1].strip() == "":
            out.pop()
    return "\n".join(out)


def has_marker(text: str, marker: str) -> bool:
    """True iff the FIRST line of text starts with marker."""
    first = text.split("\n", 1)[0]
    return first.startswith(marker)


def parse_comments(text: str) -> list[dict]:
    """Parse the output of `gh api ... --paginate --jq '.[]'` into a list of dicts.

    That command streams ONE json object per line (JSONL). Parsing line-by-line
    avoids the trap of a plain `--paginate` (no --jq), which concatenates each
    page's array (`[...][...]`) into invalid JSON -- a single json.loads on that
    fails, and swallowing the error to [] silently posts a duplicate sticky
    comment every run. Also tolerates a single JSON array (one line) for
    robustness, skips unparseable lines, and keeps only dict items (so an error
    object / bare value can never reach find_existing_id as a non-dict).
    """
    out: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            val = json.loads(line)
        except json.JSONDecodeError:
            continue
        items = val if isinstance(val, list) else [val]
        out.extend(x for x in items if isinstance(x, dict))
    return out


def find_existing_id(comments: list[dict], marker: str):
    """Return the id of the LAST comment whose body starts with marker, else None.

    Last-match so that if duplicates ever exist, we converge on the newest and
    keep editing it. Null-body-safe (a deleted/empty comment body is skipped).
    """
    found = None
    for c in comments:
        body = c.get("body") or ""
        if body.startswith(marker):
            found = c["id"]
    return found


def gh(args: list[str]) -> tuple[int, str]:
    """Run `gh` and return (returncode, stdout). Never raises."""
    r = subprocess.run(["gh", *args], capture_output=True, text=True, check=False)
    if r.returncode != 0 and r.stderr.strip():
        print(r.stderr.strip(), file=sys.stderr)
    return r.returncode, r.stdout


def _repo() -> str:
    repo = os.environ.get("GITHUB_REPOSITORY")
    if repo:
        return repo
    _, out = gh(["repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner"])
    return out.strip()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("pr", type=int, help="PR number")
    ap.add_argument("--marker", required=True,
                    help="sticky marker; must be the body's exact first line")
    ap.add_argument("--body-file", required=True, help="path to the markdown body")
    args = ap.parse_args()

    path = pathlib.Path(args.body_file)
    raw = path.read_text() if path.is_file() else ""
    if not raw.strip():
        # Advisory: the compose step produced nothing usable -- loud but green.
        print(f"::error title=Sticky comment::no body at {args.body_file} -- "
              "nothing posted (see the compose step log).")
        return 0

    body = normalize(raw)
    if not has_marker(body, args.marker):
        # Marker MUST be line 1, or a later run can't find this comment and
        # would post a duplicate every run. Fail loud instead of duplicating.
        print(f"::error title=Sticky comment::body missing marker '{args.marker}' "
              "on line 1 -- NOT posting (would duplicate next run).")
        return 0

    repo = _repo()
    # --jq '.[]' streams one comment object per line (JSONL); see parse_comments
    # for why plain --paginate on an array is unsafe.
    rc, out = gh(["api", f"repos/{repo}/issues/{args.pr}/comments",
                  "--paginate", "--jq", ".[]"])
    if rc != 0:
        print("::error title=Sticky comment::failed to list comments "
              "(advisory, not blocking).")
        return 0
    existing = find_existing_id(parse_comments(out), args.marker)

    tmp = pathlib.Path(tempfile.gettempdir()) / f"sticky-{args.pr}-{os.getpid()}.md"
    tmp.write_text(body)
    if existing is not None:
        rc, _ = gh(["api", "--method", "PATCH",
                    f"repos/{repo}/issues/comments/{existing}",
                    "-F", f"body=@{tmp}"])
        if rc != 0:
            print(f"::error title=Sticky comment::failed to update comment "
                  f"{existing} (advisory, not blocking).")
            return 0
        print(f"updated sticky comment {existing} on PR #{args.pr}")
    else:
        rc, _ = gh(["api", "--method", "POST",
                    f"repos/{repo}/issues/{args.pr}/comments",
                    "-F", f"body=@{tmp}"])
        if rc != 0:
            print("::error title=Sticky comment::failed to post comment "
                  "(advisory, not blocking).")
            return 0
        print(f"posted new sticky comment on PR #{args.pr}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
