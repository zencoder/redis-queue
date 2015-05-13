GO ?= godep go
ifdef CIRCLE_ARTIFACTS
  COVERAGEDIR = $(CIRCLE_ARTIFACTS)
endif

all: test
godep:
	go get github.com/tools/godep
godep-save:
	godep save ./...
fmt:
	$(GO) fmt ./...
test:
	$(GO) test -v ./...
bench:
	$(GO) test -bench ./...
clean:
	$(GO) clean
	