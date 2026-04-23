# Makefile
.PHONY: build test clean docker-build docker-push

VERSION ?= v1.0.0
REPO ?= yourorg/traefik-backend-selector

build:
	go build -o plugin ./...

test:
	go test -v ./...

clean:
	rm -f plugin

docker-build:
	docker build -t $(REPO):$(VERSION) .

docker-push:
	docker push $(REPO):$(VERSION)

mod:
	go mod tidy
	go mod vendor

release: mod test docker-build docker-push
	git tag $(VERSION)
	git push origin $(VERSION)

