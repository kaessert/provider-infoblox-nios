# openapi2crd generator golden files

This directory holds pinned, byte-exact expected output for the
`openapi2crd generate` templates implemented in the parent `generator` package.

## Layout

```
testdata/
├── golden/    expected output: <slug>_<scope>_types.go.golden,
│              <slug>_<scope>_register.go.golden
└── README.md  this file
```

There is no `fixtures/` directory of input spec files for this provider —
Infoblox NIOS does not publish an OpenAPI/Swagger spec (input format
rest-nospec-sdk). The generator's input is the hand-curated
`pkg/catalog.ResourceDescriptor` Go literals, not a parsed file, so there is
no spec fixture to check in. Degenerate/edge-case `ResourceDescriptor`
inputs (zero-field resource, nested type with zero fields, resource with no
immutable fields) are constructed inline as Go literals in
`structural_test.go` (this package) instead of file fixtures.

## Updating golden files

When an intentional change to the generator templates changes rendered
output, regenerate the golden files and review the diff before committing:

```sh
cd tools/openapi2crd
go test ./pkg/generator/... -run TestGolden -update-golden
git diff pkg/generator/testdata/golden/
```

Never hand-edit a `.golden` file — always regenerate it from the generator
and review the diff, so the golden file always reflects what the code
actually produces today.
