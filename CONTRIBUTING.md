# Develop Large Payload Service (LPS)

This doc is for contributors to  Large Payload Service (hopefully that's you!)

<!-- toc -->

- [Prerequisites](#prerequisites)
  * [Build prerequisites](#build-prerequisites)
- [Development](#development)
  * [Check out the code](#check-out-the-code)
  * [Build the Source](#build-the-source)
  * [Run the Tests](#run-the-tests)
  * [Format the Code](#format-the-code)
  * [Third party code](#third-party-code)
- [CI](#ci)
- [Filing issues](#filing-issues)
- [Contributing patches](#contributing-patches)

<!-- tocstop -->

## Prerequisites

### Build prerequisites

* [Go Lang](https://golang.org/) (minimum version required is 1.19):
    - Install on macOS with `brew install go`.
    - Install on Ubuntu with `sudo apt install golang`.

## Development

### Check out the code

LPS uses go modules.
Clone the repo into the preferred location:

```bash
git clone https://github.com/DataDog/temporal-large-payload-codec.git
```

### Build the Source

```sh
go build -o lps cmd/server/main.go
```

### Run the Tests

To run the unit tests:

```sh
go test ./...
```

### Format the Code

```sh
gofmt -l -w .
```

### Third party code

The license, origin, and copyright of all third party code is tracked in `LICENSE-3rdparty.csv`.
To verify that this file is up to date execute:

```sh
go run ./internal/tools/licensecheck
```

## CI

CI is configured via a GitHub Workflow in [.github/workflows/ci.yaml](.github/workflows/ci.yaml).
You can test and run the pipeline locally by installed `[act](https://github.com/nektos/act)` and running:

```shell
act pull_request
```

## Filing issues

File issues using the standard [Github issue tracker](https://github.com/DataDog/temporal-large-payload-codec/issues) for the repository.
Before you submit a new issue, we recommend that you search the list of issues to see if anyone already submitted a similar issue.

## Contributing patches

Thank you for your contributions! Please follow this process to submit a patch:

- Create an issue describing your proposed change to the repository.
- The repository owners will triage and respond to your issue promptly.
- Fork the repository and create a topic branch.
- Submit a pull request with the proposed changes.
