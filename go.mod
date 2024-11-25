module github.com/canonical/go-dqlite

go 1.16

// Dependents of go-dqlite should switch to either the v2 series (LTS) or the
// v3 series (ongoing development).
retract [v1.0.0, v1.99.99]

require (
	github.com/Rican7/retry v0.3.1
	github.com/google/renameio v1.0.1
	github.com/kr/pretty v0.1.0 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/mattn/go-sqlite3 v1.14.7
	github.com/peterh/liner v1.2.2
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v1.8.1
	github.com/stretchr/testify v1.10.0
	golang.org/x/sync v0.8.0
	golang.org/x/sys v0.0.0-20211117180635-dee7805ff2e1
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/yaml.v2 v2.4.0
)
