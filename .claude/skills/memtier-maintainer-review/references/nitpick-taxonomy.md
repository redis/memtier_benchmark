# Cross-cutting nitpick taxonomy — memtier_benchmark, real precedent only

15 evidenced categories, each with a real precedent from mined GitHub review history on redis/memtier_benchmark.
Work through these against the target PR. Categories 2 and 3 carry outsized weight (see SKILL.md) — everything
else is a normal-weight check, and most PRs will only trip a handful of these, if any.

1. **Buffer sizing / snprintf correctness.** yossigo, repeatedly: "buffer should be bigger for null terminator
   no?", "Better stick to snprintf()". Check any fixed-size buffer + format-string construction for correct
   sizing and NUL-termination.

2. **Breaking output-format changes require more than disclosure.** yossigo PR#60: "Not sure about breaking
   output format just for the sake of X. I'd either make this optional or at least push it to the end... to
   minimize risk of breaking something." oranagra PR#212: chose the less-breaking of two viable fixes
   (`strtod` over `strtoull`) explicitly because the other would break previously-valid input, even though it
   was "more correct" in isolation. Both of those are genuine maintainer reasoning in real review comments —
   the strongest evidence in this taxonomy. PR#379 is weaker precedent than it looks: its commit message says
   it kept JSON keys back-compat "for back-compat with existing consumers and the test suite," but that's the
   PR author's own stated rationale, and the one human review it got (paulorsousa) was later dismissed, not a
   standing approval — so treat it as "an existing choice in this repo's history," not "a maintainer-endorsed
   decision." Any change to CLI flag behavior, output format, or JSON schema should still be checked against:
   was a less-breaking design actually considered and ruled out, or did the PR just pick the simplest fix and
   disclose the break? — that question stands on its own merits independent of how strong any single citation is.

3. **JSON/output-format bugfixes need a regression test that provably fails-then-passes.** PR#364 is a real
   example already in this repo's history: "Add regression test... that pins the invariant... the multi-run
   test fails on master without the fix, validating it catches the bug." That specific framing is the PR
   author's own description, not a maintainer's stated bar — PR#364's only review was a real but empty-body
   approval, so cite it as "here's a concrete example already accepted in this repo," not as an
   independently-articulated maintainer mandate. Regardless of provenance, the underlying check is worth making
   on its own merits: does an actual committed test exercise the exact broken scenario and fail on the pre-fix
   code, or is "I manually tested" being treated as sufficient?

4. **No commented-out dead code, no whitespace-only diffs mixed into substantive changes.** yossigo: "Make sure
   unused code is removed, not just commented", "Please avoid changes that are purely white space (suggest to
   clean them on final rebase of the PR)", "Please avoid commented out code as it's ambiguous."

5. **Input validation / valid-vs-invalid ambiguity.** yossigo PR#67: flags when a parser can't distinguish a
   legitimate zero/empty value from a parse failure.

6. **Hot-loop / per-request performance impact of new branching or string work.** YaacovHazan PR#154 (runtime
   `str.replace` on every request — precompute instead); yossigo PR#182 ("I wonder if these additional branches
   are going to have some measurable impact on performance, being executed in tight loops").

7. **CLI flag backward compatibility and naming precision.** YaacovHazan PR#182 (new flag must not silently
   change an existing flag's meaning); PR#234 ("Doesn't `--tls-versions` will be more correct?" — plural/singular
   mismatch against what it actually accepts).

8. **Resist new surface area if an existing option could be extended instead.** yossigo PR#182: "Did you
   consider extending `--protocol` instead of adding another flag?"

9. **Code duplication — prefer the smaller diff that reasons through existing logic.** YaacovHazan PR#172;
   paulorsousa PR#320 ("If it's easy... it would be nice for all errors to go through the same path. Does that
   sound feasible?").

10. **Stray/accidental files and genuinely dead code.** paulorsousa: "Was this file added intentionally? Looks
    like a backup (?)", "Looks like this function isn't used. Do you confirm?"

11. **Redundant/overlapping config flags.** paulorsousa PR#381: "isn't `retry_on_error` redundant? from the
    comments `max_retries = 0` seems to disable `retry_on_error`."

12. **Type/precision correctness at the root cause.** oranagra PR#212: traces a fractional-precision bug to the
    actual wrong type (`float` vs `double`) rather than patching the symptom.

13. **Scope discipline is usually author-initiated, not reviewer-enforced.** The real pattern found is authors
    proactively offering to split a stalled/multi-feature PR, not reviewers demanding it. If a PR already
    contains an author's own offer to split ("happy to split this into its own PR"), note that they've already
    met the project's actual norm here — don't manufacture a harsher reviewer stance than the evidence supports.

14. **AGENTS.md's new-CLI-option checklist is real project doctrine.** For any new CLI flag: `extended_options`
    enum entry, `long_options[]` entry, `getopt_long` switch case, `benchmark_config` struct field, default init
    in `config_init_defaults()`, `usage()` help text, man page (`memtier_benchmark.1`), bash completion
    (`bash-completion/memtier_benchmark`), and a test. This wasn't found being manually policed comment-by-comment
    in review history (it's codified in AGENTS.md itself), but it's documented institutional standard — if a PR
    adds a new CLI flag, check its diff (`gh pr diff`) for whether it also touches `memtier_benchmark.1` and
    `bash-completion/memtier_benchmark` alongside the flag itself; if a local checkout with those files present is
    available, a direct `grep <flag> memtier_benchmark.1` is a fine substitute, but don't assume that access.

15. **Reason manually about integer-division truncation, unbounded loops on adversarial/empty input, off-by-one
    on empty collections.** Real Cursor Bugbot catches on recent PRs that human reviewers missed: an unbounded
    probe loop that can spin forever on cluster MGET, a crash on an empty keylist (unsigned-int underflow),
    duplicate keys sent when a slot has few keys, integer division silently truncating an averaged counter to
    zero (defeating a warning threshold). Since human reviewers increasingly rubber-stamp after Bugbot runs,
    actively check for these classes yourself rather than assuming "CI is green" covers them — Bugbot catches a
    lot, but a green CI run doesn't prove the logic is right, only that nothing crashed under the cases actually
    exercised.
