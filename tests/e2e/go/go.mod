module github.com/feast-dev/feast/tests/e2e/go

go 1.14

require (
	github.com/feast-dev/feast/sdk/go v0.0.0-20200902044419-0b09bd9b7ed6
	github.com/google/go-cmp v0.5.1
)

replace github.com/feast-dev/feast/sdk/go => ../../../sdk/go
