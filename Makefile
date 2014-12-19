GO ?= go	
GOPATH := $(CURDIR):$(GOPATH)
export GOPATH

all: fmt test

fmt:
	cd $(CURDIR)/rq; $(GO) fmt

test:
	cd $(CURDIR)/rq; $(GO) test .

bench:
	cd $(CURDIR)/rq; $(GO) test -bench .
