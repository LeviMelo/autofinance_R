# manual_events.md (Optional input to Module 04)

## Purpose
`manual_events` is an optional override channel for corporate actions when vendors are wrong/incomplete.
Manual events are merged with vendor events during event normalization.

## Table contract (schema)

### Required columns
| column      | type      | required | semantics |
|-------------|-----------|----------|----------|
| symbol      | character | yes      | uppercase ticker |
| refdate     | Date      | yes      | effective trading date you want the event to apply at |
| action_type | character | yes      | `split` or `dividend` |
| value       | numeric   | yes      | split: **price factor**, dividend: cash amount |
| source      | character | optional | if absent, Module 04 forces `"manual"` |

### Manual split convention (hard rule)
Manual split `value` is always a **price factor**:
- forward 2-for-1 split ⇒ `value = 0.5`
- reverse 1-for-10 split ⇒ `value = 10`

## Merge policy (with vendor events)
- Manual events are unioned with vendor events.
- Exact duplicates are removed before aggregation:
  `(symbol, refdate, action_type, value, source)`.

## Consumer rules
- Manual events should be used sparingly and documented (commit message + note).
- Presence of any manual event for a symbol upgrades its `adjustment_state` to `manual_override` (unless later overridden to `suspect_unresolved` by safety checks).
