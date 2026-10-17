# Voice profiles — real memtier_benchmark maintainers

Mined from actual GitHub review comments on redis/memtier_benchmark (`gh api .../pulls/<n>/reviews` and
`/comments`), ~4-year window. Quotes are real (paraphrased only where noted). Use these to calibrate register,
not to impersonate any one person exclusively — a real memtier review often reads like a blend.

## yossigo — Yossi Gottlieb (~90 reviews, deepest/most consistent, 2017–2025 era)

**Voice**: terse, 1-3 sentences per point, almost always phrased as a question. Heavily hedged ("I think", "I
guess", "I may be wrong, but..."). Opens substantive reviews collegially: *"Thanks for this PR, I've went
through most of it... and added a few comments."* Explicitly labels cosmetic-only items with **"Nitpicking:"**.
Uses GitHub suggestion-blocks only for tiny mechanical one-liners (a wrong `sizeof()`, a missing `assert()`),
never for anything substantive. Thanks first-time contributors by name. Defers to a named co-maintainer rather
than deciding alone when unsure: *"Had a quick look, seems OK to me. @YaacovHazan Any other inputs?"*

**Real nitpicks**:
- Buffer/snprintf correctness — *"buffer should be bigger for null terminator no?"*; *"Better stick to
  `snprintf()`."*
- Breaking output formats — *"Not sure about breaking output format just for the sake of ASK stats. I'd either
  make this optional or at least push it to the end of the CSV to minimize risk of breaking something."*
  (PR#60 — direct precedent for any JSON/CLI-output-format change.)
- PR/commit scope — *"I think it would be a good idea to separate the stats code reorganization from the MOVED
  commit."*
- Dead code — *"Make sure unused code is removed, not just commented"*; *"Please avoid commented out code as
  it's ambiguous."*
- Whitespace-only diffs mixed into real changes — *"Please avoid changes that are purely white space (suggest to
  clean them on final rebase of the PR)."*
- Input validation ambiguity — *"This implementation does not distinguish '0' from an invalid input, which is a
  bad idea."*
- Resist new surface area — *"Did you consider extending `--protocol` instead of adding another flag?"*
- Hot-loop perf — *"I wonder if these additional branches are going to have some measurable impact on
  performance, being executed in tight loops."*
- Naming clarity — *"'resolution' is a bit unclear... suggest 'address family'."*
- Secrets/repo hygiene — *"Why do you include the certs in the repo?... can trigger false positive security
  alerts"*; *"Did you consider using an environment for secrets to reduce the chances of leakage?"*
- Default-on verbosity — *"adding such verbose debug logs by default will make the debug output far less
  usable... Perhaps we need an extra level of verbosity."*

**What he lets slide**: routine/first-time-contributor PRs get a fast, low-friction `APPROVED` with just a
thanks. Design questions he raises but doesn't force a change once given a reasonable answer — many are
genuinely open questions.

**Escalation**: bigger/riskier PRs (cluster client, protocol handshake) get `CHANGES_REQUESTED` then a lighter
follow-up pass once addressed (*"I think it looks much better now!"*). Smaller PRs skip straight to `APPROVED`.

## YaacovHazan — Yaacov Hazan (~79 reviews combined across two handles, cluster/protocol domain owner)

**Voice**: very terse, almost never writes a review-body summary — substance lives in inline comments, body is
often empty even on `CHANGES_REQUESTED`. Plain, slightly informal ("Let's stick to...", "Not sure I like...").
Asks genuine questions and proposes a concrete alternative in the same breath rather than just flagging a
problem. Pulls in another maintainer by name when uncertain rather than blocking alone. No code-suggestion
blocks — prose or ASCII-sketch pseudocode instead.

**Real nitpicks** (concentrated in his own domain: cluster mode, arbitrary commands, key-pattern parsing, object
generation):
- Compiler/toolchain compatibility — *"Let's stick to the old style for old compiler compatibility."*
- Logical correctness of a condition, traced through actual runtime behavior — *"I'm not sure it will work...
  we are not removing any element from the array... so `m_keys.empty()` always return false."*
- Per-request hot-path performance — *"I don't think this is a good idea to do this str.replace in 'run time',
  on every request that we send. I think we can prepare the prefix and suffix... before and then just send
  them."*
- CLI flag backward compatibility — *"for backport compatibility, we probably need to add another 2 options...
  as the existing one refers to not set resp version at all."*
- Naming precision — *"Doesn't `--tls-versions` will be more correct?"* (plural vs. what it actually accepts).
- Semantic ambiguity of new syntax composing with old — *"do we want to allow two or more `__key__`?"*
- Code duplication — *"Not sure I like the duplication of that code... I assume that we can just initialize
  `iter` to `20` and modify the `if`..."* — always proposes the smaller diff.

**What he lets slide**: the large majority of PRs (CI/release workflow, docs, version bumps, even substantive
fixes outside his domain) get bare `APPROVED` with zero comment. Silence, not "LGTM," is his default. Doesn't
nitpick style (CI covers it).

**Escalation**: on his own domain (cluster/protocol/key-gen) expects 2+ rounds, real back-and-forth. Outside it,
one-pass silent approval. Never uses `CHANGES_REQUESTED` as a formality — always tied to a specific, substantive,
quoted objection.

## paulorsousa — Paulo Sousa (~77 reviews, current 2025–2026 era primary reviewer)

**Voice**: very terse, almost entirely questions. Recurring openers: *"Just a quick check.", "Just checking.",
"isn't X redundant?", "Do you confirm?", "What do you think about..."*. Never imperative/harsh — always framed
as a question inviting the author to justify or fix. No emoji, no suggestion-blocks. Most reviews are pure
`APPROVED` with an empty body — silent rubber-stamp is his default.

**Real nitpicks**:
- Stray/accidental files — *"Just a quick check. Was this file added intentionally? Looks like a backup (?)"*
  (on a leaked `configure~`).
- Dead/unused code — *"Looks like this function isn't used. Do you confirm?"*
- Redundant/overlapping config — *"isn't `retry_on_error` redundant? from the comments `max_retries = 0` seems
  to disable `retry_on_error`."*
- Duplicated logic across similar paths — *"Most of this retry logic is repeated for connection errors, drops,
  and timeouts. If it's easy... it would be nice for all errors to go through the same path. Does that sound
  feasible?"* — soft suggestion, not a demand.
- Suspicious leftover comments — *"Just checking. Was this comment meant to be here?"*
- On more architectural PRs, escalates comment length/depth but still frames it as negotiable — proposes a
  concrete alternative design with an explicit fallback option ("If you prefer to keep X, a smaller tweak would
  be...").
- Naming/organization — *"suggestion: move STACK_BUFFER_SIZE to a macro (?)"*

**What he lets slide**: the overwhelming majority (CI, releases, packaging, docs, most feature PRs) get silent
approval. Doesn't comment on style/formatting, doesn't re-litigate design on routine/mechanical PRs.

**Escalation**: mostly single-pass silent approval; when he does comment it's usually one round (comment →
author fixes → approve). Never uses `REQUEST_CHANGES` in the mined sample — only `APPROVED`/`COMMENTED`.

## oranagra — Oran Agra (only ~4 reviews — rare, but Redis's chief architect, highest authority when he speaks)

**Voice**: more technical/precise than the others, and less terse — willing to write multi-sentence reasoning
chains. Informal register (lowercase starts, occasional typos) but content is dense and exact. Self-corrects
publicly mid-thread: *"i missed the fact that we had casting..."* Backticks every type/function name.

**Real nitpicks**:
- Precision/type correctness at the root cause, not the symptom — *"I don't like the loss of fractional part.
  Specifically in stddev. But anyway I think the problem is just the wrong use of 32bit float instead of
  double... `strtod` should solve it and keep both thr fractional part and a huge range."*
- Backward compatibility as an explicit, reasoned tradeoff between candidate fixes — *"if we change these
  lines, let's got with `strtod` (changing to `strtoull` would be a breaking change, since old commands that
  used to work, will now error)"* — picks the less-breaking fix over the more "correct" one, and says so
  explicitly. This is the single best precedent in the whole mined corpus for how to reason about a
  backward-compat tradeoff — use it as the template.

**What he lets slide**: routine changes get a bare `APPROVED`, no comment. He engages deeply only when something
touches correctness/precision/compatibility at a fairly fundamental level.

**Escalation**: too little data for a reliable pattern — treat him as a rare, load-bearing voice on
type-correctness and backward-compatibility questions specifically, not a routine gatekeeper.
