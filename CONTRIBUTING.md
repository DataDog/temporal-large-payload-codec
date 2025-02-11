# Develop Large Payload Service (LPS)

This doc is for contributors to Large Payload Service (hopefully that's you!).

<!-- toc -->

- [Prerequisites](#prerequisites)
  * [Build prerequisites](#build-prerequisites)
- [Development](#development)
  * [Check out the code](#check-out-the-code)
  * [Build the Source](#build-the-source)
  * [Run the Tests](#run-the-tests)
  * [Format the Code](#format-the-code)
  * [Third party code](#third-party-code)
  * [Go file headers](#go-file-headers)
- [CI](#ci)
- [Releasing](#releasing)
- [Filing issues](#filing-issues)
- [Contributing patches](#contributing-patches)

<!-- tocstop -->

## Prerequisites

### Build prerequisites

* [Go Lang](https://golang.org/) (minimum version required is 1.23):
    - Install on macOS with `brew install go`.
    - Install on Ubuntu with `sudo apt install golang`.
*[Make command](https://www.gnu.org/software/make/)
    - install on MacOS with `xcode-select --install`
    - install on Ubuntu with `sudo apt install make`

## Development

### Check out the code

LPS uses go modules.
Clone the repo into the preferred location:

```bash
git clone https://github.com/DataDog/temporal-large-payload-codec.git
```

### Build the Source

```sh
make build
```

### Run the Tests

To run the unit tests:

```sh
make test
```

### Format the Code

```sh
gofmt -l -w .
```

### Third party code

The license, origin, and copyright of all third party code is tracked in `LICENSE-3rdparty.csv`.
To verify that this file is up-to-date execute:

```sh
make update_license_file
```

### Go file headers

To ensure all Go files contain the correct license header, execute:

```sh
make update_copyright_headers
```

## CI

CI is configured via a GitHub Workflow in [.github/workflows/ci.yaml](.github/workflows/ci.yaml).
You can test and run the pipeline locally by installed `[act](https://github.com/nektos/act)` and running:

```shell
act pull_request
```

## Releasing

The release process is automated via [ncipollo/release-action@v1.14.0](https://github.com/ncipollo/release-action) and executed via the GitHub Workflow in [.github/workflows/release.yaml](.github/workflows/release.yaml).
To cut a release create a tag locally and push it to GitHub. the tag has the following format : `<MODULE>/<VERSION>` whith module being either `server` or `codec` depending on what you want to
release and version being a valid [semantic version](https://semver.org/), prefixed with 'v', eg _codec/v1.0.0_.

To release v1.0.0 you would run:

```shell
```sh
git tag -a v1.0.0
git push --tags origin HEAD
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
