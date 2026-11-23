# Canonical Pi toolbox entrypoints. Pin the exercised toolchain.
export GOTOOLCHAIN := "go1.25.5"
build:
    CGO_ENABLED=0 go build ./...

lint *paths:
    #!/bin/sh
    set -eu
    if [ -z "{{paths}}" ]; then dirs="cmd internal"; else dirs="{{paths}}"; fi
    for path in $dirs; do
        if [ -d "$path" ]; then find "$path" -name '*.go' -exec gofmt -w {} +; else gofmt -w "$path"; fi
    done
    go vet ./...

test *paths:
    #!/bin/sh
    set -eu
    if [ -z "{{paths}}" ]; then go test ./...; else
        for path in {{paths}}; do go test "./$path"; done
    fi
