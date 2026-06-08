# PostgreSQL migrations

Migrations are numbered `NNN_description.sql` with matching `NNN_description.down.sql` rollback files.

## Version numbering

- Use the next sequential number when adding a migration (currently **015+**).
- Numbers **006**, **007**, and **009** were reserved but never shipped; do not reuse them — lexicographic sort order must match apply order.
- `listUpMigrations` rejects duplicate or out-of-order numeric prefixes at startup.

## Applying

Migrations run automatically via `driver.Migrate(ctx)` or the Fluvio CLI migrate command.
