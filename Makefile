
help:
	$(info Targets)
	$(info test              | run the tests)
	$(info continuous-test   | run the tests on changes)

always:

target/debug/git-commits-by-blob: always
	cargo build

test: target/debug/git-commits-by-blob
	tests/test.sh $<
	
continuous-test:
	watchexec $(MAKE) test
