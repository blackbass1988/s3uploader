GOFMT_FILES?=$$(find . -name '*.go' | grep -v vendor)

bin: fmtcheck
	@TF_RELEASE=1 sh -c "'$(CURDIR)/scripts/build.sh'"

dev: fmtcheck
	go build .

fmtcheck:
	@sh -c "'$(CURDIR)/scripts/gofmtcheck.sh'"


fmt:
	gofmt -w $(GOFMT_FILES)


.NOTPARALLEL:

.PHONY: bin cover default dev e2etest fmt fmtcheck generate protobuf plugin-dev quickdev test-compile test testacc testrace tools vendor-status website website-test
