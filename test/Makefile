
NODEUNIT=../node_modules/nodeunit/bin/nodeunit

.PHONY: test
test:
	find . -name '*.test.js' | xargs -n 1 $(NODEUNIT)
