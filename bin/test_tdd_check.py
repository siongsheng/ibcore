#!/usr/bin/env python3
"""Tests for the tdd-check two-commit-TDD checker.

Runs two ways:

    python3 -m pytest test_tdd_check.py
    python3 test_tdd_check.py

The pure-function tests (classify_commits and helpers) build commit
fixtures by hand and need NO real git. The integration test at the end
builds a throwaway git repo in a tempdir and runs the real script.
"""

import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT = os.path.join(_HERE, "tdd-check")


def _load_module():
    """Import the extensionless `tdd-check` script as a module."""
    spec = importlib.util.spec_from_file_location(
        "tdd_check", _SCRIPT,
        loader=importlib.machinery.SourceFileLoader("tdd_check", _SCRIPT),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


import importlib.machinery  # noqa: E402

tc = _load_module()


def commit(sha, message, files):
    return {"sha": sha, "message": message, "files": list(files)}


class TestIsTestFile(unittest.TestCase):
    def test_python(self):
        self.assertTrue(tc.is_test_file("tests/test_foo.py"))
        self.assertTrue(tc.is_test_file("test_foo.py"))
        self.assertTrue(tc.is_test_file("pkg/foo_test.py"))
        self.assertFalse(tc.is_test_file("pkg/foo.py"))

    def test_typescript_js(self):
        self.assertTrue(tc.is_test_file("src/foo.test.ts"))
        self.assertTrue(tc.is_test_file("src/foo.spec.tsx"))
        self.assertTrue(tc.is_test_file("__tests__/foo.ts"))
        self.assertFalse(tc.is_test_file("src/foo.ts"))

    def test_go(self):
        self.assertTrue(tc.is_test_file("pkg/handler_test.go"))
        self.assertFalse(tc.is_test_file("pkg/handler.go"))

    def test_java(self):
        self.assertTrue(tc.is_test_file("src/main/FooTest.java"))
        self.assertTrue(tc.is_test_file("src/main/FooTests.java"))
        self.assertFalse(tc.is_test_file("src/main/Foo.java"))

    def test_rust_under_tests_dir(self):
        self.assertTrue(tc.is_test_file("tests/integration.rs"))
        self.assertFalse(tc.is_test_file("src/lib.rs"))

    def test_spec_dir(self):
        self.assertTrue(tc.is_test_file("spec/models/user_spec.rb"))

    def test_specs_docs_dir_is_not_test(self):
        # A `specs/` directory is the spec-driven-development docs convention
        # (edge-radar/huat/dokima all use it), NOT tests. And a doc file is
        # never a test regardless of path.
        self.assertFalse(tc.is_test_file("specs/roadmap.md"))
        self.assertFalse(tc.is_test_file("specs/mission.md"))
        self.assertFalse(tc.is_test_file("tests/README.md"))
        # real spec *test* files are still detected via the filename pattern:
        self.assertTrue(tc.is_test_file("spec/models/user_spec.rb"))
        self.assertTrue(tc.is_test_file("src/foo.spec.ts"))


class TestIsCosmeticFile(unittest.TestCase):
    def test_css_and_config(self):
        for p in ["app.css", "theme.scss", "config.yaml", "data.json",
                  "Cargo.toml", "app.config.js", "src/constants.py"]:
            self.assertTrue(tc.is_cosmetic_file(p), p)

    def test_code_is_not_cosmetic(self):
        for p in ["src/main.py", "pkg/handler.go", "lib.rs"]:
            self.assertFalse(tc.is_cosmetic_file(p), p)


class TestParsePrefix(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(tc.parse_prefix("test: add cases"), "test")
        self.assertEqual(tc.parse_prefix("feat: thing"), "feat")
        self.assertEqual(tc.parse_prefix("fix(ui): bug"), "fix")
        self.assertEqual(tc.parse_prefix("feat!: breaking"), "feat")

    def test_none(self):
        self.assertIsNone(tc.parse_prefix("just a message"))


class TestClassifyCommits(unittest.TestCase):
    def test_proper_red_then_green_is_clean(self):
        commits = [
            commit("aaa1111", "test: add failing test", ["tests/test_foo.py"]),
            commit("bbb2222", "feat: implement foo", ["src/foo.py"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertTrue(rep.ok)
        self.assertEqual(rep.blockers, 0)
        kinds = [c.classification for c in rep.commits]
        self.assertEqual(kinds, ["test", "impl"])

    # ---- Regression: co-located tests (the bug this fix addresses) --------
    def test_rust_colocated_test_commit_is_red(self):
        """Rust #[cfg(test)] lives inside src/foo.rs -- prefix must win.

        This is the exact case that made tdd-check a no-op on edge-radar M3:
        the `test:` commit touches only a `src/*.rs` file (no `tests/` path),
        so path-based classification called it impl and reported
        "no test commit found". With prefix-primary classification the
        `test:` commit is RED, the following `feat:` is GREEN, and the range
        is clean (RED ancestor of GREEN) with no warnings.
        """
        commits = [
            commit("aaa1111", "test(store): add failing bulk-upsert test",
                   ["src/store/iv_store.rs"]),
            commit("bbb2222", "feat(store): implement bulk upsert",
                   ["src/store/iv_store.rs"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertTrue(rep.ok)
        self.assertEqual(rep.blockers, 0)
        self.assertEqual(rep.warnings, 0, [i for i in rep.issues])
        self.assertEqual([c.classification for c in rep.commits],
                         ["test", "impl"])
        self.assertEqual([c.role for c in rep.commits], ["test", "impl"])
        msgs = " ".join(m for _, m, _ in rep.issues).lower()
        self.assertNotIn("no test", msgs)

    def test_python_colocated_test_commit_is_red(self):
        """Python often co-locates tests/doctests inside the module."""
        commits = [
            commit("aaa1111", "test: add failing case", ["src/mod.py"]),
            commit("bbb2222", "feat: make it pass", ["src/mod.py"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertTrue(rep.ok)
        self.assertEqual(rep.warnings, 0, [i for i in rep.issues])
        self.assertEqual(rep.commits[0].classification, "test")
        self.assertEqual(rep.commits[0].role, "test")

    def test_prefix_beats_path_not_bundled(self):
        """A test: commit touching a src/*.rs file is NOT a bundling BLOCKER.

        Paths cannot reveal co-located-test bundling, so we must not raise a
        false bundling BLOCKER just because a `test:` commit edits source.
        """
        commits = [
            commit("aaa1111", "test: cover the parser", ["src/parser.rs"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertTrue(rep.ok)
        self.assertEqual(rep.blockers, 0)
        self.assertNotEqual(rep.commits[0].classification, "bundled")
        self.assertEqual(rep.commits[0].role, "test")

    def test_docs_commit_touching_specs_not_bundled(self):
        """A docs: commit touching specs/*.md + top-level docs is NOT a bundled
        test+impl commit -- specs/ is documentation, not tests. Regression for
        the false positive that failed edge-radar's docs-only PR."""
        commits = [
            commit(
                "aaa1111",
                "docs: sync status",
                ["specs/roadmap.md", "CLAUDE.md", "README.md"],
            ),
        ]
        rep = tc.classify_commits(commits)
        self.assertTrue(rep.ok)
        self.assertEqual(rep.blockers, 0)
        self.assertNotEqual(rep.commits[0].classification, "bundled")

    def test_test_prefix_touching_no_test_code_warns(self):
        """A test: commit touching only clearly-non-test code is a possible
        mislabel -> SHOULD-FIX (not a hard fail)."""
        commits = [
            commit("aaa1111", "test: tweak", ["Makefile"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertTrue(rep.ok)  # warning only
        self.assertEqual(rep.blockers, 0)
        self.assertGreaterEqual(rep.warnings, 1)
        self.assertIn("mislabel",
                      " ".join(m for _, m, _ in rep.issues).lower())

    def test_path_decisive_bundling_still_blocker(self):
        """When paths ARE decisive (a clearly-test-path file AND a
        clearly-non-test code file in one commit), bundling is still a
        BLOCKER."""
        commits = [
            commit("ccc3333", "feat: bar with bundled test",
                   ["tests/foo_test.go", "foo.go"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertFalse(rep.ok)
        self.assertGreaterEqual(rep.blockers, 1)
        self.assertEqual(rep.commits[0].classification, "bundled")
        self.assertEqual(rep.commits[0].severity, tc.BLOCKER)

    def test_bundled_commit_is_blocker(self):
        commits = [
            commit("ccc3333", "feat: impl + test",
                   ["src/foo.py", "tests/test_foo.py"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertFalse(rep.ok)
        self.assertGreaterEqual(rep.blockers, 1)
        self.assertEqual(rep.commits[0].classification, "bundled")
        self.assertEqual(rep.commits[0].severity, tc.BLOCKER)

    def test_impl_without_preceding_test_is_should_fix(self):
        commits = [
            commit("ddd4444", "feat: implement foo", ["src/foo.py"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertTrue(rep.ok)  # SHOULD-FIX only, not a blocker
        self.assertEqual(rep.blockers, 0)
        self.assertGreaterEqual(rep.warnings, 1)
        msgs = " ".join(m for _, m, _ in rep.issues).lower()
        self.assertIn("no test", msgs)

    def test_css_only_feat_downgraded_to_should_fix(self):
        commits = [
            commit("eee5555", "feat: restyle panel", ["ui/panel.css"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertTrue(rep.ok)  # downgraded, not a blocker
        self.assertEqual(rep.blockers, 0)
        self.assertEqual(rep.commits[0].classification, "cosmetic")
        self.assertEqual(rep.commits[0].severity, tc.SHOULD_FIX)
        self.assertIn("chore", rep.commits[0].message.lower() + " " +
                      "".join(m for _, m, _ in rep.issues).lower())

    def test_green_before_red_is_blocker(self):
        # impl commit first, test committed afterwards -> ancestry violation
        commits = [
            commit("f001111", "feat: implement foo", ["src/foo.py"]),
            commit("f002222", "test: add test afterwards",
                   ["tests/test_foo.py"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertFalse(rep.ok)
        self.assertGreaterEqual(rep.blockers, 1)
        msgs = " ".join(m for _, m, _ in rep.issues).lower()
        self.assertIn("green before red", msgs)

    def test_empty_range_is_clean(self):
        rep = tc.classify_commits([])
        self.assertTrue(rep.ok)
        self.assertEqual(rep.blockers, 0)

    def test_cosmetic_chore_not_warned(self):
        commits = [
            commit("c0ffee1", "chore: bump config", ["config.yaml"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertTrue(rep.ok)
        self.assertEqual(rep.commits[0].classification, "cosmetic")
        self.assertEqual(rep.commits[0].severity, tc.OK)

    def test_multi_language_test_commits(self):
        for path in ["tests/test_x.py", "a.test.ts", "h_test.go",
                     "FooTest.java", "tests/it.rs"]:
            rep = tc.classify_commits(
                [commit("s", "test: x", [path])])
            self.assertEqual(rep.commits[0].classification, "test", path)

    def test_test_path_without_prefix_warns(self):
        """Test-path files without a 'test:' prefix -> SHOULD-FIX nudge."""
        rep = tc.classify_commits(
            [commit("s", "add cases", ["tests/test_x.py"])])
        self.assertTrue(rep.ok)
        self.assertEqual(rep.commits[0].role, "test")
        self.assertEqual(rep.commits[0].severity, tc.SHOULD_FIX)

    def test_chore_touching_code_is_excluded(self):
        """chore:/style:/docs: are non-code/cosmetic -- excluded from the
        RED/GREEN discipline even when they touch source (e.g. rustfmt)."""
        commits = [
            commit("aaa1111", "chore(fmt): apply rustfmt",
                   ["src/store/iv_store.rs", "src/store/mod.rs"]),
        ]
        rep = tc.classify_commits(commits)
        self.assertTrue(rep.ok)
        self.assertEqual(rep.commits[0].classification, "cosmetic")
        self.assertEqual(rep.commits[0].severity, tc.OK)


class TestIntegration(unittest.TestCase):
    """Build a real throwaway git repo and run the actual script."""

    def setUp(self):
        if not shutil.which("git"):
            self.skipTest("git not available")
        self.dir = tempfile.mkdtemp(prefix="tddcheck-it-")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _git(self, *args):
        env = dict(os.environ)
        env.update({
            "GIT_AUTHOR_NAME": "T", "GIT_AUTHOR_EMAIL": "t@e.x",
            "GIT_COMMITTER_NAME": "T", "GIT_COMMITTER_EMAIL": "t@e.x",
        })
        return subprocess.run(
            ["git", "-C", self.dir, *args],
            check=True, capture_output=True, text=True, env=env)

    def _write(self, rel, content="x\n"):
        p = os.path.join(self.dir, rel)
        os.makedirs(os.path.dirname(p) or self.dir, exist_ok=True)
        with open(p, "w") as fh:
            fh.write(content)

    def _run_check(self, *args):
        return subprocess.run(
            [sys.executable, _SCRIPT, *args],
            cwd=self.dir, capture_output=True, text=True)

    def _init_main(self):
        self._git("init", "-q")
        self._git("checkout", "-q", "-b", "main")
        self._write("README.md", "seed\n")
        self._git("add", "-A")
        self._git("commit", "-q", "-m", "chore: seed")

    def test_passing_case(self):
        self._init_main()
        self._git("checkout", "-q", "-b", "feature")
        self._write("tests/test_foo.py", "def test_foo():\n    assert foo()\n")
        self._git("add", "-A")
        self._git("commit", "-q", "-m", "test: add failing test for foo")
        self._write("src/foo.py", "def foo():\n    return True\n")
        self._git("add", "-A")
        self._git("commit", "-q", "-m", "feat: implement foo")
        res = self._run_check("--base", "main", "--head", "HEAD")
        self.assertEqual(res.returncode, 0, res.stdout + res.stderr)

    def test_bundled_failing_case(self):
        self._init_main()
        self._git("checkout", "-q", "-b", "feature")
        self._write("tests/test_bar.py", "def test_bar():\n    assert bar()\n")
        self._write("src/bar.py", "def bar():\n    return True\n")
        self._git("add", "-A")
        self._git("commit", "-q", "-m", "feat: bar with test bundled together")
        res = self._run_check("--base", "main", "--head", "HEAD")
        self.assertNotEqual(res.returncode, 0, res.stdout + res.stderr)
        self.assertIn("bundled", (res.stdout + res.stderr).lower())

    def test_rust_colocated_passing_case(self):
        """Real git repo, Rust co-located tests: a `test:` commit and a
        `feat:` commit that both touch only `src/*.rs`. Must PASS and must
        recognize the test commit as RED (not 'no test commit found')."""
        self._init_main()
        self._git("checkout", "-q", "-b", "feature")
        self._write("src/store.rs",
                    "pub fn upsert() {}\n#[cfg(test)]\nmod tests {\n"
                    "    #[test]\n    fn it_upserts() { assert!(true); }\n}\n")
        self._git("add", "-A")
        self._git("commit", "-q", "-m", "test(store): failing upsert test")
        self._write("src/store.rs",
                    "pub fn upsert() { /* real */ }\n#[cfg(test)]\nmod tests {\n"
                    "    #[test]\n    fn it_upserts() { assert!(true); }\n}\n")
        self._git("add", "-A")
        self._git("commit", "-q", "-m", "feat(store): implement upsert")
        res = self._run_check("--base", "main", "--head", "HEAD")
        out = res.stdout + res.stderr
        self.assertEqual(res.returncode, 0, out)
        self.assertNotIn("no test commit", out.lower())
        # The RED commit is reported as a test, not impl.
        self.assertIn("test", out.lower())

    def test_invalid_base_ref_errors(self):
        self._init_main()
        res = self._run_check("--base", "nonexistent-ref-xyz")
        self.assertNotEqual(res.returncode, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
