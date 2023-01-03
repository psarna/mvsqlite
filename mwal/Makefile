all:	debug

debug:	debug_standalone

debug_standalone:	mwal.c src/lib.rs
	cargo build && clang -Wall -fPIC -shared mwal.c ../target/debug/libmwal.a -o ../target/debug/mwal.so -lssl -lcrypto -lpthread -ldl -lm

release:	prep release_standalone

release_standalone:	mwal.c src/lib.rs
	cargo build -j1	--quiet --release && \
		clang -fPIC -shared wal.c ../target/release/libmwal.a \
		-o ../target/release/mwal.so -lssl -lcrypto -lpthread -ldl -lm
