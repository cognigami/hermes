# Canonical Pi toolbox entrypoints. Pin the exercised toolchain.
export GOTOOLCHAIN := "go1.25.5"
# Native development binaries only; not target-qualified deployment packages.
build:
    mkdir -p dist/local
    CGO_ENABLED=0 go build -trimpath -o dist/local/ ./cmd/...

lint *paths:
    #!/bin/sh
    set -eu
    if [ -z "{{paths}}" ]; then dirs="cmd internal"; else dirs="{{paths}}"; fi
    for path in $dirs; do
        if [ -d "$path" ]; then find "$path" -name '*.go' -exec gofmt -w {} +; else gofmt -w "$path"; fi
    done
    go vet ./...

# Explicit Darwin packages; compilation is not target/service qualification.
build-agent goos goarch:
    #!/bin/sh
    set -eu
    case '{{goos}}/{{goarch}}' in darwin/amd64|darwin/arm64) ;; *) echo 'Unsupported persistent agent target' >&2; exit 1 ;; esac
    version="${HERMES_VERSION:-dev}"
    case "$version" in ''|*[!A-Za-z0-9._+-]*) echo 'Invalid HERMES_VERSION' >&2; exit 1 ;; esac
    out='dist/agent/{{goos}}-{{goarch}}'
    mkdir -p "$out"
    GOOS='{{goos}}' GOARCH='{{goarch}}' CGO_ENABLED=0 GOAMD64=v1 GOARM64=v8.0 go build -trimpath -ldflags "-X hermes/internal/platform.Version=$version" -o "$out/hermes-agent" ./cmd/hermes-agent
    cp -R deploy/agent/darwin/. "$out/"
    go version -m "$out/hermes-agent" > "$out/build-info.txt"
    (cd "$out" && shasum -a 256 hermes-agent > SHA256SUMS)

build-controller goos goarch:
    #!/bin/sh
    set -eu
    case '{{goos}}/{{goarch}}' in darwin/amd64|darwin/arm64) ;; *) echo 'Unsupported controller target' >&2; exit 1 ;; esac
    version="${HERMES_VERSION:-dev}"
    case "$version" in ''|*[!A-Za-z0-9._+-]*) echo 'Invalid HERMES_VERSION' >&2; exit 1 ;; esac
    out='dist/controller/{{goos}}-{{goarch}}'
    mkdir -p "$out"
    GOOS='{{goos}}' GOARCH='{{goarch}}' CGO_ENABLED=0 GOAMD64=v1 GOARM64=v8.0 go build -trimpath -ldflags "-X hermes/internal/platform.Version=$version" -o "$out/hermes-controller" ./cmd/hermes-controller
    cp -R deploy/controller/darwin/. "$out/"
    cp deploy/agent/darwin/service.sh "$out/service.sh"
    go version -m "$out/hermes-controller" > "$out/build-info.txt"
    (cd "$out" && shasum -a 256 hermes-controller > SHA256SUMS)

# Assemble only; never installs services or invokes sudo. Settings come from
# the coordinator after inspection/configuration gates are clear.
stage-service-trial goos goarch settings:
    #!/bin/sh
    set -eu
    case '{{goos}}/{{goarch}}' in darwin/amd64|darwin/arm64) ;; *) echo 'Unsupported service-trial target' >&2; exit 1 ;; esac
    test -f '{{settings}}/trial-config.json' || { echo 'Missing trial settings' >&2; exit 1; }
    just build-agent '{{goos}}' '{{goarch}}'
    just build-controller '{{goos}}' '{{goarch}}'
    mkdir -p dist/service-trial
    out=$(mktemp -d 'dist/service-trial/{{goos}}-{{goarch}}.XXXXXX')
    out=$(CDPATH= cd "$out" && pwd)
    mkdir "$out/bin" "$out/agent" "$out/controller"
    GOOS='{{goos}}' GOARCH='{{goarch}}' CGO_ENABLED=0 GOAMD64=v1 GOARM64=v8.0 go build -trimpath -o "$out/trial-helper" ./internal/packaging/testdata/service-trial
    GOOS='{{goos}}' GOARCH='{{goarch}}' CGO_ENABLED=0 GOAMD64=v1 GOARM64=v8.0 go build -trimpath -o "$out/bin/" ./cmd/hermes ./cmd/hermes-synthetic
    cp -R 'dist/agent/{{goos}}-{{goarch}}/.' "$out/agent/"
    cp -R 'dist/controller/{{goos}}-{{goarch}}/.' "$out/controller/"
    cp internal/packaging/testdata/service-trial/run.sh "$out/run.sh"
    cp '{{settings}}/trial-config.json' "$out/trial-config.json"
    if [ -d '{{settings}}/config' ]; then cp -R '{{settings}}/config' "$out/config"; fi
    printf 'Staged only (not executed): %s\n' "$out"

test *paths:
    #!/bin/sh
    set -eu
    if [ -z "{{paths}}" ]; then go test ./...; else
        for path in {{paths}}; do go test "./$path"; done
    fi
