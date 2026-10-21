# StockDB update transport repair — 2026-10-21

The [vendor notice](https://dns.novapi.cc/tabs/notice.html) requires the repaired
v0.3.5 client (September 8), HTTP sync origins (September 16), a one-time reset
of `data/disable` to receive corrected history and new listings (October 6), and
prohibits using the documentation host as a sync origin (October 8).

The [official Windows package](https://dns.novapi.cc/downloads/free-stockdb-windows-v0.3.5-more-power.zip)
contains `https://ah.123128.xyz` and `https://ad.123128.xyz always` in
`stockdb/sync_url.txt`. Apply the subsequent HTTP notice to both entries.
The download link is relative to the home page that embeds the notice tab,
not the tab URL.

The observed legacy updater SHA-256 is
`7ebe7ebee9367400e2910c33af85bdc462a5289c28d5601cb38e6f5a1be08d32`.
The inspected repaired updater SHA-256 is
`ee833769e9b2d8765e5919d93c73cb0f5f7732b4971ee31746eb6e733b910fc8`.
Only that known legacy executable is automatically replaced; other user-supplied
binaries remain untouched. A changed download fails checksum verification before
replacement. The updater is a standalone native synchronizer; the local server,
SDK, database files, and server configuration are preserved.

Before an update that local validation says is needed, the managed runtime
prepares the updater and migrates exact official sync origins. It preserves
custom origins, comments, line endings, BOM and `always` flags. For official
one-time sources, it renames the old `data/disable` to
`.quantmaster-disable-before-20261006` outside the vendor data partitions once; this preserves the sentinel
and prevents repeating the full-history repair after successful synchronization.
Preparation failures leave the running service available and report a failed
update. Existing session, coverage, OHLCV and data-quiescence gates still decide
whether synchronization succeeded; notices and process exit are not evidence.

Rollback: revert the code and restore the previous vendor updater from the
original distribution. Restore the saved sentinel only while no update is
running; doing so blocks the full-history resync again. No market schema changes.
