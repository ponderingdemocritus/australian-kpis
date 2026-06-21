# UI/UX Approachability PRD

Status: product review draft  
Date: 2026-06-20  
Surface: `apps/web`  
Product type: data-dense reference app for public economic data

## 1. Executive Summary

Australian KPIs has a credible foundation: the app loads real CPI data, charts
it clearly, supports search, compares regional series, and exposes live API
queries. The main UX problem is not capability. The problem is approachability.

The current site reads like an internal API demo. It leads with implementation
terms such as `Explorer`, `Dataflow`, `API base`, and SDK method names before it
answers the user's first question:

> What is the latest number, what does it mean, where did it come from, and what
> can I do with it?

This PRD defines the next product slice: make Australian KPIs understandable by
scanning, useful within 60 seconds, trustworthy by default, and easy to hand off
to code.

Core shift:

- From: browse dataflows and dimensions through the SDK.
- To: find an Australian economic indicator, understand the latest result, trust
  the source, export the data, and copy working API or SDK code.

## 2. Evidence Reviewed

Rendered app:

- Local API: `http://127.0.0.1:3000`, `/v1/health` returned `{"status":"ok"}`.
- Local web app: `http://127.0.0.1:4173`.
- Desktop screenshots captured for Explore, Search, Compare, and API
  Playground at `1440x900`.
- Mobile screenshot captured for Explore at `390x844`.
- Browser console showed only local React/Next development messages in reviewed
  states.
- Explorer loaded live CPI data with latest value `101.7` for `Jan 2026`.
- Search returned CPI results and opened Consumer Price Index in Explore.
- Compare rendered Australia, Sydney, and Melbourne CPI series.
- Playground returned live JSON plus curl and SDK snippets.

Local artifacts:

- Screenshots: `/Users/os/.gstack/projects/australian-kpis/designs/ui-ux-prd-20260620/screenshots/`
- Metrics: `/Users/os/.gstack/projects/australian-kpis/designs/ui-ux-prd-20260620/ux-metrics.json`

Measured findings:

- No horizontal scroll at `390px` mobile width.
- Top nav buttons rendered at `32px` high.
- Selects and inputs rendered at `40px` high.
- Primary mobile touch targets should be at least `44px`.
- Mobile first screen showed controls and implementation copy before the main
  answer.
- Direct route-backed pages are not present in the app shell. The current app
  uses client state for the active surface.

Spec note:

- `Spec.md` still describes `apps/web` as Vite + React, while the current app is
  Next.js 16. If Next.js is now the intended architecture, the implementation PR
  should update `Spec.md` separately. This PRD assumes the current Next.js app is
  the target surface.

## 3. Current UX Diagnosis

### What Works

- Calm, official-feeling visual tone that suits public economic data.
- Real loaded data, not a mock-only UI.
- Search, Explore, Compare, and Playground are all functional product surfaces.
- Charts and tables are readable on desktop and mobile.
- Source attribution is present.
- Playground generates live curl and SDK examples.
- The UI avoids decorative marketing patterns that would damage trust.

### What Blocks Approachability

1. Internal vocabulary leads the experience.
   Users see dataflow, dimensions, SDK methods, and API base before plain
   dataset language.

2. The answer is not first on mobile.
   The first mobile screen prioritizes navigation, controls, dataflow selectors,
   and implementation details before latest value, chart, source, and actions.

3. Navigation is not shareable.
   Current page and filters are React state. Users cannot reliably copy, refresh,
   or deep-link to a specific Compare or Playground state.

4. Key actions are missing.
   Users can view data and snippets, but they cannot download CSV, copy snippets
   with feedback, copy a chart/table link, or open the current query in docs.

5. Raw JSON dominates developer mode.
   Playground is useful, but it makes users read raw response bodies before
   showing a request summary, rows returned, period range, source, license, and
   copy-ready examples.

6. Trust context is too passive.
   Attribution appears at the bottom of Explore, but source, license, cadence,
   unit, filters, and timestamp should travel with charts, exports, and copied
   snippets.

7. Error recovery is too generic.
   Failure states should name the attempted API base URL, likely cause, and next
   action. They should not leave blank charts or final `n/a` states.

8. Mobile ergonomics need product decisions, not just stacking.
   The layout stacks without horizontal scroll, but it does not reprioritize for
   the mobile task. Controls are also below the recommended touch size.

9. Visual system is credible but thin.
   The app has a quiet teal system, cards, tables, and chart panels, but lacks a
   written baseline for data summaries, filters, errors, empty states, copy
   actions, provenance, and chart annotations.

## 4. Product Goals

1. A first-time user understands the site without learning API vocabulary first.
2. CPI can be found, understood, exported, and copied into code in under 60
   seconds.
3. The latest answer appears before the machinery.
4. Every chart, table, export, and snippet carries enough source context to be
   trusted.
5. Developer handoff is obvious: curl, TypeScript SDK, endpoint docs, and live
   Playground.
6. Mobile is task-prioritized, not just a stacked desktop layout.
7. The visual design remains calm, dense, and work-focused.

## 5. Non-Goals

- Redesigning ingestion, storage, scheduler, API internals, or SDK internals.
- Adding accounts, saved dashboards, billing, or personalization.
- Adding editorial economic commentary beyond data available from the API.
- Building a marketing homepage.
- Replacing the generated OpenAPI reference.
- Solving every dataset-specific UX detail in one pass. CPI is the proof path.

## 6. Target Users

- Analysts who need current Australian economic indicators quickly.
- Developers who need working API and SDK examples.
- Policy and research users who need source-backed numbers they can cite.
- Internal contributors validating end-to-end data quality.

## 7. Primary Jobs

1. Find CPI and understand the latest result.
2. Compare CPI across regions.
3. Export the exact data currently being viewed.
4. Copy working code for the exact query.
5. Verify source, license, cadence, and selected filters.
6. Recover when the API is unreachable or a query is invalid.

## 8. North-Star Journey

Entry: user lands on Australian KPIs with no prior API knowledge.

Expected path:

1. Search for `CPI`.
2. Select `Consumer Price Index`.
3. Land on Explore with CPI selected.
4. See latest value, period, unit, source, license, selected region, and change
   from previous period when available.
5. Inspect chart and table.
6. Export CSV for the current query.
7. Open the same query in API Playground.
8. Copy the TypeScript SDK snippet.
9. Open endpoint docs for deeper reference.

Success metric: a new user completes this path on desktop and `390px` mobile
without knowing what a dataflow is.

## 9. Product Requirements

### P0. Information Architecture And Routing

Requirements:

- Rename visible `Explorer` to `Explore`.
- Rename visible `Playground` to `API Playground`.
- Rename visible `Dataflow` to `Dataset`.
- Add route-backed navigation for Explore, Search, Compare, API Playground, and
  Docs.
- Preserve current page, dataset, region, dimensions, date range, limit, and
  selected comparison series in the URL.
- Keep current page visibly selected in navigation.
- Default Explore to CPI when CPI data is available.

Acceptance:

- `/explore?dataset=abs.cpi&region=50` opens CPI Explore.
- `/compare?dataset=abs.cpi&regions=50,1,2` opens Compare with those regions.
- `/playground?dataset=abs.cpi&region=50&limit=4` restores that request.
- Refreshing any app page preserves page and selected state.
- User-facing copy avoids unexplained `dataflow` before advanced API context.

### P0. Search-Led Entry

Requirements:

- Make Search the clearest first-use path.
- Search `CPI` returns Consumer Price Index.
- Result rows show dataset name, source, cadence, available dimensions, latest
  period when available, and an obvious open action.
- Selecting a result opens Explore with URL-backed state.
- Empty search suggests known examples such as CPI, AEMO, ASX, APRA, and budget
  datasets when available.

Acceptance:

- Playwright can search `CPI`, select Consumer Price Index, and land on CPI
  Explore with URL-backed state.
- Pressing Enter and clicking Search produce the same result.

### P0. Answer-First Explore

Requirements:

- Add a dataset summary above charts:
  - dataset name
  - source
  - license or attribution
  - cadence
  - unit
  - latest period
  - selected filters
- Add a plain-language result summary:
  - latest value
  - latest period
  - previous value when available
  - period-over-period change when available
  - selected region
- Add unit labels to chart headers, chart axes, and table headers.
- Keep chart, table, export, and source context visually grouped.
- On mobile, render content in this order:
  1. dataset title
  2. latest result summary
  3. source, cadence, unit, and selected filters
  4. primary chart
  5. actions
  6. filters
  7. table

Acceptance:

- On `390px` mobile, latest result and primary chart appear before advanced
  filter controls.
- Failed or empty requests never render `n/a`, `waiting`, or blank charts as
  final data.

### P0. Export And Copy Actions

Requirements:

- Add CSV export for the current Explore table.
- Add CSV export for selected Compare series.
- CSV output includes dataset, source, license or attribution, selected filters,
  unit, period, and value.
- Add `Open in API Playground` from Explore and Compare using the current query.
- Add `View API docs` from Playground for the current endpoint.
- Add copy buttons for curl and SDK snippets.
- Show copy success with text, not color alone.

Acceptance:

- Exported CPI CSV includes dataset, source, region, unit, period, and value.
- Copying the SDK snippet shows visible success feedback.

### P0. Error, Empty, And Loading States

Requirements:

- Replace generic fetch failures with actionable messages that include:
  - attempted API base URL
  - likely cause
  - next action
  - retry action where safe
- Distinguish API unreachable, CORS or origin blocked, empty result, invalid
  date range, invalid limit, and unsupported dimensions.
- Loading skeletons match final chart and table shapes.
- Bounded retries show retry status and then stop.
- Failed panels replace blank charts or stale final-looking data.

Acceptance:

- Running the web app against an unreachable API produces a clear setup message.
- An observations failure shows attempted request context and a retry action.

### P0. Mobile Ergonomics

Requirements:

- Raise nav buttons, selects, inputs, and primary buttons to at least `44px` on
  mobile.
- Convert filter sidebars into a compact filter panel on mobile.
- Show answer before filters on mobile.
- Make Compare series rows tappable, not only the checkbox.
- Collapse raw JSON on mobile until the user asks to inspect it.
- Put copy and export actions near the content they affect.
- Maintain no horizontal scroll at `390px`.

Acceptance:

- The mobile CPI journey has no clipped primary text, overlapping UI, horizontal
  scroll, or sub-44px primary controls.

### P0. Docs Gateway

Requirements:

- Add a lightweight docs gateway before generated reference detail:
  - Start here
  - base URL
  - authentication model
  - install SDK
  - query CPI in 60 seconds
  - copy curl
  - copy TypeScript SDK example
  - common errors
  - link to generated endpoint reference
- Link docs examples back to a working Playground query.

Acceptance:

- A developer can copy one working CPI curl example and one working SDK example
  from the docs gateway.

### P1. Compare Improvements

Requirements:

- Show selected series as removable chips above the chart.
- Add select all, clear, and reset to default actions.
- Use redundant encodings beyond color: direct labels, symbols, or table labels.
- Preserve selected comparison series in the URL.
- Add copy/export actions to the latest-values table.

Acceptance:

- A user can add Brisbane, remove Sydney, refresh, and keep the same selected
  series.

### P1. API Playground Improvements

Requirements:

- Add a response summary above raw JSON:
  - rows returned
  - first period
  - last period
  - selected dimensions
  - source and license
- Put endpoint preview, curl, and SDK snippets above raw JSON on mobile.
- Add copy buttons to each snippet.
- Use plain names in summaries and raw API names in code blocks.
- Validate date and limit before running the query.

Acceptance:

- A user can understand whether the request returned useful data without
  reading raw JSON.

### P1. Visual System Baseline

Requirements:

- Define a lightweight design baseline for:
  - typography
  - spacing
  - touch targets
  - chart palette
  - tables
  - filter panels
  - empty states
  - loading states
  - error states
  - copy/export buttons
  - provenance blocks
- Use tabular numerals for metric values and table columns.
- Keep the calm teal accent, but avoid a one-note palette.
- Use cards only for discrete tools, repeated items, and genuinely framed
  panels.
- Normalize chart panels so each includes title, selected filters, latest value,
  date, unit, and source affordance where relevant.

Acceptance:

- Explore, Search, Compare, API Playground, and Docs share one obvious
  interaction language.

### P2. Dataset Guidance

Requirements:

- Add richer dataset detail states for high-value datasets:
  - CPI
  - wage price index
  - unemployment
  - cash rate
  - ASX market statistics
  - AEMO dispatch
- Each dataset surface includes source context, available dimensions, update
  cadence, common queries, and links to Explore, Compare, Playground, and Docs.

Acceptance:

- CPI has enough dataset context that a non-developer can understand what is
  measured and how often it updates.

## 10. Release Slices

1. Routes and language
   - Add real routes.
   - Rename internal terms in visible copy.
   - Preserve page and filter state in URLs.
   - Add route restore tests.

2. Search-led Explore
   - Make search the first-use path.
   - Enrich result rows.
   - Route selected result into Explore.
   - Preserve Enter and button behavior.

3. Answer-first Explore
   - Add latest/change summary.
   - Add dataset/source/filter summary.
   - Reorder mobile content.
   - Add stable loading, empty, and error states.

4. Export and copy
   - Add Explore CSV export.
   - Add Compare CSV export.
   - Add snippet copy buttons and success feedback.
   - Add Open in API Playground actions.

5. Playground and Compare polish
   - Add response summary.
   - Move snippets above JSON on mobile.
   - Add Compare chips and redundant series labels.

6. Docs gateway
   - Add CPI quickstart docs.
   - Link docs examples to Playground.
   - Keep generated reference behind the quickstart.

7. Visual baseline
   - Normalize touch targets.
   - Add tabular numerals.
   - Normalize chart, table, error, loading, and empty-state patterns.

## 11. Success Metrics

- First-use CPI journey completion on desktop: target `>= 90%` in moderated
  internal testing.
- First-use CPI journey completion on `390px` mobile: target `>= 85%`.
- Time to latest CPI result from landing: target `< 30 seconds`.
- Time to copy working SDK snippet from landing: target `< 60 seconds`.
- Zero critical or serious axe violations across first-use surfaces.
- Zero horizontal scroll at `390px`.
- No primary mobile control below `44px`.

## 12. Acceptance Criteria

- A new user can complete this desktop Playwright journey: land, search `CPI`,
  select Consumer Price Index, view latest value, inspect chart, export CSV,
  open API Playground, copy SDK snippet.
- The same journey passes at `390px` mobile width with no horizontal scroll,
  clipped primary text, overlapping UI, or sub-44px primary touch targets.
- Refreshing Explore, Search, Compare, and API Playground preserves selected
  page and filters from the URL.
- API outage and CORS or origin blocked states name the attempted API base URL
  and give a concrete next action.
- Empty or failed observation requests never display blank charts, `n/a`, or
  `waiting` as final data.
- Pressing Enter in Search performs the same action as clicking Search.
- All snippets have copy buttons and visible success feedback.
- CSV export includes source attribution and selected filters.
- User-facing copy avoids unexplained implementation terms before advanced API
  context.

## 13. Test Requirements

- Playwright E2E for the first-use CPI journey on desktop.
- Playwright E2E for the first-use CPI journey at `390px` mobile width.
- Playwright E2E for URL-backed state restore on Explore, Search, Compare, and
  API Playground.
- Playwright E2E for CSV export contents and filename.
- Playwright E2E for snippet copy success feedback.
- Playwright E2E for API unreachable, CORS or origin blocked, empty result,
  invalid date, and invalid limit states.
- Axe checks for Explore, Search, Compare, API Playground, docs gateway, and
  generated docs reference.
- Unit tests for URL state serialization and parsing.
- Unit tests for CSV generation metadata.

## 14. UX Scorecard

| Area | Current | Target | Reason |
| --- | --- | --- | --- |
| First-use clarity | C+ | A | Useful data exists, but internal terms lead. |
| Information architecture | C | A | Direct routes and refresh-safe state are missing. |
| Data comprehension | B- | A | Values and charts exist; answer summary is missing. |
| Trust and provenance | B | A | Attribution exists; it must attach to exports and snippets. |
| Developer handoff | B- | A | Snippets exist; copy actions and docs links are missing. |
| Mobile ergonomics | C | A- | No horizontal scroll, but controls precede the answer. |
| Error recovery | C- | A- | Errors are present but not diagnostic or actionable enough. |
| Accessibility | B | A | Existing axe checks help; mobile, docs, and error states need coverage. |
| Visual system | B- | A- | Calm and credible, but not yet codified. |

Headline score: `C+` today. Target: `A-` for the CPI journey.

## 15. Open Product Decisions

- Should `/` open Search with CPI suggested, or Explore with CPI preselected?
- Should `apps/web` or `apps/docs` be the public entry point?
- Should the UI fully translate `dataflow` to `dataset`, or show both after the
  first-use path?
- Which datasets should be featured after CPI?
- Should CSV export cover only visible rows first, or the complete query result?

## 16. Definition Of Done

The redesign is done when the CPI path is understandable by scanning headings
and primary values alone, works on desktop and `390px` mobile, survives refresh
and shared links, exports source-backed CSV, copies working SDK code, and gives
clear recovery instructions when the API is unavailable.
