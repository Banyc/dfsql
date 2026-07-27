# Instructions

## Backend behavior

- One-to-one behavioral alignment between the dynamic and Polars backends is a non-goal.
- Enforce behavior the project defines as required. Treat unspecified or optional behavior as backend-dependent, and do not add implementation complexity or tests solely to align it.
- Group-by result ordering is unspecified, so ordering differences between backends are acceptable.
- When required behavior differs between backends, fix the mismatch using the lowest-complexity valid approach with the fewest special cases. Following Polars is acceptable when its result satisfies the required behavior.
- For example, `2 / 2 == 1` is required. If a backend disagrees, either implement the defined result directly or follow Polars when that produces the defined result, whichever is simpler.
