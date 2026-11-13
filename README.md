# FlyPig IEC 60870-5-104 protocol library

A single Go module containing the IEC 60870-5-104 protocol implementation shared by FlyPig Host and the IEC104 plugin.

Packages under `go-iecp5/` derive from `github.com/wendy512/go-iecp5`; packages under `iec104/` derive from `github.com/wendy512/iec104`. Original source headers are retained. The `licenses/` directory preserves the license texts carried by the source repositories. The go-iecp5 source has an upstream license-file/header mismatch (LGPL-3.0 license file, some files state GPL-3.0); this repository preserves both notices and does not assert a replacement blanket license.

FlyPig-specific changes include raw APDU observation. TX observation fires only after the complete socket write succeeds; failed writes are not reported. RX observation remains in the protocol state-machine path. The observer receives a copy of APDU bytes.

This module is independent of FlyPig SDK and contains no Host or plugin contract types. It is a source/API migration from the two upstream module paths, so consumers should use this module path consistently; Go types from the old paths are not assignable to these package types.

## Packages

- `go-iecp5/asdu`, `go-iecp5/cs104`, `go-iecp5/cs101`, `go-iecp5/clog`
- `iec104/client`, `iec104/server`, `iec104/pkg/waitgroup`

## Validation

Run `GOWORK=off go test ./...`, `GOWORK=off go vet ./...`, and `GOWORK=off go build ./...` from this module root.
