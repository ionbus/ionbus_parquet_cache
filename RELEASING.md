# Releasing ionbus_parquet_cache

This document is for maintainers releasing the package to PyPI and Anaconda.org.

Published names:

- PyPI package: `ionbus-parquet-cache`
- Conda package: `ionbus::ionbus-parquet-cache`
- Import name: `ionbus_parquet_cache`

## Normal release

The normal release command is:

```bash
cd /path/to/ionbus_parquet_cache
./release.sh all --tag
```

This will:

- create the next local tag
- verify the tag is on `HEAD`
- build the pip artifacts
- build the conda artifacts
- upload to PyPI
- upload to `ionbus` on Anaconda.org

If everything succeeds, push the branch and the new git tag manually when you are ready.

## Release environment

Releases are built from the `pixi_313_pd22` Pixi environment managed by:

```bash
~/bin/python_env_management
```

The release scripts are:

- [release.sh](release.sh) on macOS/Linux
- [release.bat](release.bat) on Windows

Required tools in the Pixi environment:

- `python-build`
- `twine`
- `conda-build`
- `setuptools-git-versioning`
- `anaconda-client`

## Versioning and tags

Version numbers come from git tags.

- `#bugfix` in a commit message bumps only the fourth version number
- `--tag` tells the release script to create the next local tag automatically
- the script verifies that `HEAD` is exactly on a tag before it builds or uploads
- the script never pushes anything to git; tags remain local until you push them yourself

## Build and send modes

Combined modes:

- `./release.sh build` builds pip and conda locally
- `./release.sh send` uploads pip and conda artifacts
- `./release.sh all --tag` tags, builds, and uploads both

Single-target modes:

- `./release.sh build-pip` builds only the pip artifacts
- `./release.sh build-conda` builds only the conda artifacts
- `./release.sh send-pip` uploads only the pip artifacts
- `./release.sh send-conda` uploads only the conda artifacts

Any of the build modes can also be run with `--tag` to create the next local tag first.

## Build only

Build packages locally without uploading:

```bash
cd /path/to/ionbus_parquet_cache
./release.sh build
```

Build and create the next local tag first:

```bash
./release.sh build --tag
```

Artifacts are written to:

- pip: `dist/`
- conda: `../ionbus_parquet_cache_conda-bld/noarch/`

## Upload only

Upload already-built artifacts for the current exact tag:

```bash
./release.sh send
```

This uploads:

- `dist/*` to PyPI with `twine`
- the exact `.conda` artifact to `ionbus` on Anaconda.org

## PyPI setup

PyPI uploads use your local PyPI credentials, for example through `~/.pypirc`.

You can also upload only the pip artifacts directly with:

```bash
~/bin/python_env_management/run_env.sh pixi_313_pd22 python -m twine upload dist/*
```

## Anaconda.org setup

Conda uploads go to the `ionbus` channel on Anaconda.org:

```text
https://anaconda.org/channels/ionbus
```

Log in with:

```bash
~/bin/python_env_management/run_env.sh pixi_313_pd22 anaconda -s anaconda.org login
```

Verify the active login with:

```bash
~/bin/python_env_management/run_env.sh pixi_313_pd22 anaconda -s anaconda.org whoami
```

You can also upload the conda artifact directly with:

```bash
~/bin/python_env_management/run_env.sh pixi_313_pd22 anaconda -s anaconda.org upload -u ionbus /path/to/ionbus_parquet_cache_conda-bld/noarch/ionbus-parquet-cache-<version>-py_0.conda
```

## Recommended release flow

1. Commit your changes, using `#bugfix` while packaging issues are still being worked out.
2. Make sure PyPI credentials are configured and Anaconda.org login is active.
3. Run:

```bash
cd /path/to/ionbus_parquet_cache
./release.sh all --tag
```
4. If everything succeeds, push the branch and the new git tag manually when you are ready.
