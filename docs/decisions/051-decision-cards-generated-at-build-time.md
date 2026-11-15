# 051: The website's design-decision cards are generated from docs/decisions at build time
- Context: The site has a "Design decisions" section. The source of truth is these markdown
  records, 51 of them now. Copying them into the site by hand would drift the first time an
  ADR changes.
- Options considered:
  1. **A build step parses `docs/decisions/*.md` into `src/data/decisions.json`**
     (`website/scripts/decisions.mjs`, run before dev, build and test). The JSON is gitignored.
  2. **Fetch the markdown at runtime** and parse it in the browser: no build step, but it
     needs the raw files deployed and a markdown parser in the bundle.
  3. **Hand-written cards** for the ten "best" decisions: the most polished, but they drift,
     and choosing ten hides the other forty.
- Decision: Option 1.
- Why:
  - **The page can't drift from the docs:** every build re-reads them.
  - **The format is ours, so the parser is small** (~60 lines, no dependency). It reads
    the `- Field:` lines, joins continuation lines, strips list markers and markdown, and
    turns "Decision: Option 2" into the option's own bold title, so a card never says
    "Option 2" without saying what that was.
  - **Tested against every real record:** a vitest test parses all ADRs and fails if one has
    no Decision or Redis comparison, so a malformed ADR breaks CI, not the page.
- Tradeoffs we accept:
  - **The format is now an API.** Older ADRs that fold "Why" into "Decision", or say "What
    real Redis clients do", are handled case by case.
  - **Cards show the first two sentences** of long fields. The full record is linked by
    path.
- What real Redis does: Not applicable.
- At bigger scale I'd: Use a real docs pipeline (MDX or a static-site generator with
  frontmatter), with a linter for the ADR template, plus cross-links between ADRs,
  benchmarks and the code they changed.
