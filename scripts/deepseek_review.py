#!/usr/bin/env python3
"""Independent PR review via the DeepSeek API.

Fetches a pull request's metadata and unified diff through the `gh` CLI,
sends them to DeepSeek (`deepseek-v4-pro` by default) with a review prompt
grounded in this repo's conventions, and prints the review as markdown.
With --post, creates or updates a single review comment on the PR (matched
by COMMENT_MARKER), so the .github/workflows/deepseek-review.yml Action can
run per-push and refresh one comment instead of spamming.

Stdlib only (urllib), matching this repo's scripts/ convention.

Usage:
    python3 scripts/deepseek_review.py <pr-number> [--post] [--model MODEL]

Auth: DEEPSEEK_API_KEY from the environment (the GitHub Action passes the
repo secret this way), or from the first .env file found at ./.env or
~/.config/panel/.env. The key never appears in output.
"""

import argparse
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request

API_URL = "https://api.deepseek.com/chat/completions"
# A unified diff runs ~4 chars/token; cap the diff so prompt + output
# stays comfortably inside the model's context window.
MAX_DIFF_CHARS = 150_000
# Marker prefix used to find-and-update this reviewer's single PR comment
# (so a per-push GitHub Action updates in place instead of spamming).
COMMENT_MARKER = "## DeepSeek review"

SYSTEM_PROMPT = """You are an adversarial, independent code reviewer. You \
did not write this code. Review honestly; do not invent findings to seem \
thorough, and say explicitly when something is correct.

Honor the target repository's OWN conventions. The PR description and diff \
below reference the project's guidance files — read and defer to any \
AGENTS.md, CLAUDE.md, or CONTRIBUTING document the repo ships, and to the \
patterns already established in the surrounding code. Those repo-specific \
rules take precedence over your defaults. When you flag a convention \
violation, cite which repo rule it breaks. When the repo's guidance is \
silent, apply the generic lenses below and mainstream best practice for the \
language and framework in the diff.

Apply software-engineering best practices rigorously, as distinct review \
lenses:
- Correctness: hand-verify arithmetic and boundary conditions; hunt \
division-by-zero, off-by-one, unit mismatches, silent truncation, and \
logic errors.
- Failure modes: what happens on crash mid-operation, partial write, \
concurrent access, IO error? Errors must propagate, never be swallowed \
or disguised as valid states.
- API design: unambiguous signatures and error/optional semantics, \
invariants enforced at the type or schema level rather than by caller \
discipline, minimal public surface.
- Architecture: module boundaries and separation of concerns, coupling and \
dependency direction (do dependencies point inward/toward stable code?), \
breaking changes to public API/schema/types, and whether the change matches \
the design/spec it claims to implement. You are the SECOND model family to \
judge architecture -- reason it out independently, do not just defer.
- Idempotency & retries: any operation a scheduler or retry loop can \
re-run must be safe to re-run.
- Test quality: tests must pin behavior with hand-computed expectations \
and cover boundaries; flag tests that merely restate the implementation \
or encode buggy behavior as expected.
- Performance: algorithmic issues and IO patterns (per-row commits, \
missing transactions, unbounded scans, N+1 queries) — flag only what \
plausibly matters at this project's scale.
- Security: injection, secrets handling, missing auth, untrusted input \
at trust boundaries.

Structure your review as markdown with sections: Overview; Correctness \
findings (each with file, severity Critical/Major/Minor, and a concrete \
failure scenario); Architecture findings (coupling, breaking changes, \
dependency direction, spec/design compliance); Convention adherence (against \
the repo's own guidance files); Test coverage assessment; Suggestions \
(non-blocking); Verdict (approve / request changes with rationale)."""


def read_key() -> str:
    key = os.environ.get("DEEPSEEK_API_KEY")
    if key:
        return key
    candidates = [
        pathlib.Path(".env"),
        pathlib.Path.home() / ".config" / "panel" / ".env",
    ]
    for path in candidates:
        if not path.is_file():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if line.startswith("DEEPSEEK_API_KEY="):
                return line.split("=", 1)[1].strip().strip("'\"")
    sys.exit(
        "error: DEEPSEEK_API_KEY not found in environment, ./.env, "
        "or ~/.config/panel/.env"
    )


def gh(args: list[str]) -> str:
    result = subprocess.run(
        ["gh", *args], capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        sys.exit(f"error: gh {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout


def post_or_update(number: int, body: str) -> None:
    """Create the review comment, or update this reviewer's existing one.

    Idempotent so a per-push GitHub Action refreshes a single comment
    (matched by COMMENT_MARKER) instead of posting a new one each run.
    """
    repo = os.environ.get("GITHUB_REPOSITORY") or gh(
        ["repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner"]
    ).strip()
    comments = json.loads(
        gh(["api", f"repos/{repo}/issues/{number}/comments", "--paginate"])
    )
    existing = next(
        (c for c in comments if c["body"].lstrip().startswith(COMMENT_MARKER)),
        None,
    )
    tmp = pathlib.Path(tempfile.gettempdir()) / f"deepseek-review-{number}.md"
    tmp.write_text(body)
    if existing:
        gh(
            [
                "api",
                "--method",
                "PATCH",
                f"repos/{repo}/issues/comments/{existing['id']}",
                "-F",
                f"body=@{tmp}",
            ]
        )
        print(f"\n[updated existing review comment on PR #{number}]", file=sys.stderr)
    else:
        gh(["pr", "comment", str(number), "--body-file", str(tmp)])
        print(f"\n[posted new review comment on PR #{number}]", file=sys.stderr)


def fetch_pr(number: int) -> tuple[dict, str]:
    meta = json.loads(
        gh(
            [
                "pr",
                "view",
                str(number),
                "--json",
                "title,body,author,baseRefName,headRefName,state,"
                "additions,deletions,changedFiles",
            ]
        )
    )
    diff = gh(["pr", "diff", str(number)])
    return meta, diff


def review(key: str, model: str, meta: dict, diff: str) -> str:
    truncated = ""
    if len(diff) > MAX_DIFF_CHARS:
        diff = diff[:MAX_DIFF_CHARS]
        truncated = (
            "\n\n[NOTE: diff truncated at "
            f"{MAX_DIFF_CHARS} characters — state this in your Overview]"
        )
    user_prompt = (
        f"Review pull request #{meta.get('number', '')}: "
        f"{meta['title']}\n"
        f"Branch: {meta['headRefName']} -> {meta['baseRefName']} | "
        f"+{meta['additions']} -{meta['deletions']} across "
        f"{meta['changedFiles']} files\n\n"
        f"PR description:\n{meta.get('body', '')}\n\n"
        f"Unified diff:\n```diff\n{diff}\n```{truncated}"
    )
    payload_base = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": 16000,
        "stream": False,
    }
    # Ask for maximum reasoning depth; retry without the parameter if this
    # API version rejects it.
    attempts = [
        {**payload_base, "reasoning_effort": "high"},
        payload_base,
    ]
    payload = None
    for i, attempt in enumerate(attempts):
        request = urllib.request.Request(
            API_URL,
            data=json.dumps(attempt).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {key}",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=900) as response:
                payload = json.loads(response.read())
            break
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:500]
            if exc.code == 400 and i + 1 < len(attempts):
                print(
                    "warning: reasoning_effort rejected; retrying without it",
                    file=sys.stderr,
                )
                continue
            sys.exit(f"error: DeepSeek API {exc.code}: {detail}")
        except urllib.error.URLError as exc:
            sys.exit(f"error: DeepSeek API unreachable: {exc.reason}")
    choice = payload["choices"][0]
    if choice.get("finish_reason") == "length":
        print(
            "warning: review hit the output-token limit and may be cut off",
            file=sys.stderr,
        )
    return choice["message"]["content"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pr", type=int, help="pull request number")
    parser.add_argument(
        "--post", action="store_true", help="post the review as a PR comment"
    )
    parser.add_argument(
        "--model", default="deepseek-v4-pro", help="DeepSeek model id"
    )
    args = parser.parse_args()

    key = read_key()
    meta, diff = fetch_pr(args.pr)
    text = review(key, args.model, meta, diff)

    header = f"{COMMENT_MARKER} (`{args.model}`)\n\n"
    print(header + text)
    if args.post:
        post_or_update(args.pr, header + text)


if __name__ == "__main__":
    main()
