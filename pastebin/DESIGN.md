# ipfS3 Pastebin Design System

## 0. Product context

A static publishing utility for technical operators. The publishing form and
share result are the primary surfaces; configuration stays available without
competing with the current task. All assets are local and the layout remains
usable without JavaScript animation.

## 1. Atmosphere & Identity

A quiet, document-like workbench. Warm neutral surfaces, compact monospace
metadata and restrained blue actions distinguish object identifiers and sharing
controls without decorative effects.

## 2. Color

| Role | Token | Value |
|---|---|---|
| Canvas | `--canvas` | `#f7f6f3` |
| Surface | `--surface` | `#ffffff` |
| Surface muted | `--surface-muted` | `#f0efeb` |
| Text | `--text` | `#20211f` |
| Text muted | `--text-muted` | `#62645f` |
| Border | `--border` | `#d8d7d1` |
| Control border | `--control-border` | `#72746d` |
| Accent | `--accent` | `#245c8c` |
| Accent hover | `--accent-hover` | `#194669` |
| Focus | `--focus` | `#9a4f00` |
| Success text | `--success` | `#285d36` |
| Success surface | `--success-bg` | `#edf3ec` |
| Warning text | `--warning` | `#765000` |
| Warning surface | `--warning-bg` | `#fbf3db` |
| Error text | `--error` | `#8c2929` |
| Error surface | `--error-bg` | `#fdebec` |

Accent is reserved for links, selected controls and primary actions. Semantic
colors communicate state. No gradients or shadows are used.

## 3. Typography

| Level | Token | Size / line height | Weight |
|---|---|---|---|
| Display | `--type-display` | `2rem / 1.2` | 650 |
| H2 | `--type-h2` | `1.25rem / 1.35` | 650 |
| H3 | `--type-h3` | `1rem / 1.5` | 650 |
| Body | `--type-body` | `1rem / 1.6` | 400 |
| Small | `--type-small` | `0.875rem / 1.5` | 400 |
| Caption | `--type-caption` | `0.75rem / 1.5` | 600 |

Font stacks:
- UI: `ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif`
- Metadata: `ui-monospace, "Cascadia Code", "SFMono-Regular", Consolas, monospace`

## 4. Spacing & Layout

Base unit: 4px.

| Token | Value |
|---|---|
| `--space-1` | 4px |
| `--space-2` | 8px |
| `--space-3` | 12px |
| `--space-4` | 16px |
| `--space-5` | 20px |
| `--space-6` | 24px |
| `--space-8` | 32px |
| `--space-10` | 40px |
| `--space-12` | 48px |

Content width is 1120px with 16px mobile and 24px desktop gutters. The publishing
area becomes a two-column workbench at 768px. Form controls are at least 44px
high; compact result links have at least a 24px target.

## 5. Components

### Planned Showcase Primitives

- Text input, password input, textarea and file input: default, hover, focus,
  disabled and error.
- Primary and secondary buttons: default, hover, active, focus, disabled and
  loading.
- Mode radios and disclosure panels: selected, unselected, keyboard focus.
- Empty result, publishing progress, success, partial extraction and error.
- Readonly share URL, copy action, external preview links and raw object links.
- ZIP entry/failure tables with long Unicode names and small-screen reflow.

### Action

- **Structure**: native `button` or `a` with a text label.
- **Variants**: primary, secondary and inline.
- **Spacing**: `--space-2` through `--space-4`.
- **States**: default, hover, active, focus-visible, disabled and loading.
- **Accessibility**: native keyboard behavior and visible two-pixel outline.
- **Motion**: 120ms color transition; no layout movement.

### Status

- **Structure**: text in a persistent status region, with semantic modifiers.
- **Variants**: neutral, success, warning and error.
- **Accessibility**: `role=status` for progress/success and `role=alert` for
  submission errors; focus remains on the initiating control.

## 6. Motion & Interaction

Micro transitions use 120ms ease-out and only color, opacity or transform.
Reduced-motion mode removes transitions. Publishing retains a stable button
label area and exposes `aria-busy`; no decorative progress animation is used.

## 7. Depth & Surface

Borders only. The default divider is `1px solid var(--border)`. Panels use the
surface token, with at most an 8px radius. There are no shadows.

## 8. Accessibility Constraints & Accepted Debt

Target the applicable WCAG 2.2 A/AA criteria for the form and result flow.
Body text reaches at least 4.5:1 contrast; control boundaries and focus
indicators reach at least 3:1 against adjacent colors. All operations have a
keyboard path, explicit labels and live status text. Layout reflows at 320px.
No accessibility debt is accepted.
