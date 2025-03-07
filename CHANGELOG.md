# Harlequin-Postgres CHANGELOG

All notable changes to this project will be documented in this file.

## [Unreleased]

## [1.2.0] - 2025-02-27

-   Adds interactions to list relations, indexes, constraints, and to describe relations (similar to psql's `\d+`) ([tconbeer/harlequin#586](https://github.com/tconbeer/harlequin/discussions/586) - thank you [@JPFrancoia](https://github.com/JPFrancoia)!).

## [1.1.1] - 2025-02-05

-   This adapter now supports `infinity` and `-infinity` dates and timestamps by loading their values as `date[time].max` or `date[time].min` ([tconbeer/harlequin#690](https://github.com/tconbeer/harlequin/issues/690)).

## [1.1.0] - 2025-01-27

-   This adapter now lazy-loads the catalog, which will dramatically improve the catalog performance for large databases with thousands of objects.
-   This adapter now implements interactions for catalog items, like dropping tables, setting the search path, etc.

## [1.0.0] - 2025-01-07

-   Drops support for Python 3.8
-   Adds support for Python 3.13
-   Adds support for Harlequin 2.X

## [0.4.0] - 2024-08-20

-   Upgrades client library to `psycopg3` (from `psycopg2`).
-   Adds an implementation of `connection_id` to improve catalog and history persistence.
-   Implements `cancel()` to interrupt in-flight queries.

## [0.3.0] - 2024-07-22

-   Adds an implementation of `close` to gracefully close the connection pool on Harlequin shut-down.
-   Adds support for Harlequin Transaction Modes and manual transactions.

## [0.2.2] - 2024-01-09

-   Sorts databases, schemas, and relations alphabetically; sorts columns ordinally. ([#10](https://github.com/tconbeer/harlequin-postgres/issues/10) - thank you [@frankbreetz](https://github.com/frankbreetz)!)

## [0.2.1] - 2023-12-14

-   Lowercases inserted values for keyword completions.

## [0.2.0] - 2023-12-14

### Features

-   Implements get_completions for keywords, functions, and settings.

## [0.1.3] - 2023-11-28

### Bug fixes

-   Implements connection pools instead of sharing a connection across threads.

## [0.1.2] - 2023-11-27

### Bug fixes

-   Fixes issues with package metadata.

## [0.1.1] - 2023-11-27

### Bug fixes

-   Fixes typo in release script.

## [0.1.0] - 2023-11-27

### Features

-   Adds a basic Postgres adapter with most common connection options.

[Unreleased]: https://github.com/tconbeer/harlequin-postgres/compare/1.2.0...HEAD

[1.2.0]: https://github.com/tconbeer/harlequin-postgres/compare/1.1.1...1.2.0

[1.1.1]: https://github.com/tconbeer/harlequin-postgres/compare/1.1.0...1.1.1

[1.1.0]: https://github.com/tconbeer/harlequin-postgres/compare/1.0.0...1.1.0

[1.0.0]: https://github.com/tconbeer/harlequin-postgres/compare/0.4.0...1.0.0

[0.4.0]: https://github.com/tconbeer/harlequin-postgres/compare/0.3.0...0.4.0

[0.3.0]: https://github.com/tconbeer/harlequin-postgres/compare/0.2.2...0.3.0

[0.2.2]: https://github.com/tconbeer/harlequin-postgres/compare/0.2.1...0.2.2

[0.2.1]: https://github.com/tconbeer/harlequin-postgres/compare/0.2.0...0.2.1

[0.2.0]: https://github.com/tconbeer/harlequin-postgres/compare/0.1.3...0.2.0

[0.1.3]: https://github.com/tconbeer/harlequin-postgres/compare/0.1.2...0.1.3

[0.1.2]: https://github.com/tconbeer/harlequin-postgres/compare/0.1.1...0.1.2

[0.1.1]: https://github.com/tconbeer/harlequin-postgres/compare/0.1.0...0.1.1

[0.1.0]: https://github.com/tconbeer/harlequin-postgres/compare/8611e628dc9d28b6a24817c761cd8a6da11a87ad...0.1.0
