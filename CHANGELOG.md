# Harlequin-Postgres CHANGELOG

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.3.0] - 2024-07-22

-   Adds an implementation of `close` to gracefull close the connection pool on Harlequin shut-down.
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

[Unreleased]: https://github.com/tconbeer/harlequin-postgres/compare/0.3.0...HEAD

[0.3.0]: https://github.com/tconbeer/harlequin-postgres/compare/0.2.2...0.3.0

[0.2.2]: https://github.com/tconbeer/harlequin-postgres/compare/0.2.1...0.2.2

[0.2.1]: https://github.com/tconbeer/harlequin-postgres/compare/0.2.0...0.2.1

[0.2.0]: https://github.com/tconbeer/harlequin-postgres/compare/0.1.3...0.2.0

[0.1.3]: https://github.com/tconbeer/harlequin-postgres/compare/0.1.2...0.1.3

[0.1.2]: https://github.com/tconbeer/harlequin-postgres/compare/0.1.1...0.1.2

[0.1.1]: https://github.com/tconbeer/harlequin-postgres/compare/0.1.0...0.1.1

[0.1.0]: https://github.com/tconbeer/harlequin-postgres/compare/8611e628dc9d28b6a24817c761cd8a6da11a87ad...0.1.0
