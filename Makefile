.PHONY: tests lint format format-fix build clean install publish bump-version-patch bump-version-minor bump-version-major

lint:
	clojure -M:lint --lint src test

tests:
	clojure -M:test

format:
	cljfmt check

format-fix:
	cljfmt fix

clean:
	clojure -T:build clean

build:
	clojure -T:build jar

install:
	clojure -T:build install

publish:
	clojure -T:build publish :bump :$(or $(BUMP),patch)

bump-version-patch:
	@$(MAKE) publish BUMP=patch

bump-version-minor:
	@$(MAKE) publish BUMP=minor

bump-version-major:
	@$(MAKE) publish BUMP=major
