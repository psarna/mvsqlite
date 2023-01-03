.load ../../target/debug/mwal
.open file:test?wal=mwal
pragma journal_mode=wal;
