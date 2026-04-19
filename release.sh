#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="pixi_313_pd22"
RUN_ENV="${HOME}/bin/python_env_management/run_env.sh"
MODE="${1:-all}"
TAG_FLAG="${2:-}"
CREATED_TAG=""

if [[ ! -x "$RUN_ENV" ]]; then
  echo "ERROR: could not find run_env.sh at $RUN_ENV"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONDA_BLD_DIR="$(dirname "$ROOT_DIR")/$(basename "$ROOT_DIR")_conda-bld"
cd "$ROOT_DIR"

case "$MODE" in
  all|build|send)
    ;;
  *)
    echo "Usage: $0 [all|build|send] [--tag]"
    echo "  build: build PyPI and conda artifacts locally"
    echo "  send: upload dist/* to PyPI and conda artifacts to ionbus"
    echo "  --tag: create, verify, and optionally push a new git tag before running"
    exit 2
    ;;
esac

if [[ -n "$TAG_FLAG" && "$TAG_FLAG" != "--tag" ]]; then
  echo "Usage: $0 [all|build|send] [--tag]"
  echo "  build: build PyPI and conda artifacts locally"
  echo "  send: upload dist/* to PyPI and conda artifacts to ionbus"
  echo "  --tag: create, verify, and optionally push a new git tag before running"
  exit 2
fi

verify_head_tag() {
  local expected_tag="${1:-}"
  local current_tag

  current_tag="$(git describe --tags --exact-match 2>/dev/null || true)"
  if [[ -z "$current_tag" ]]; then
    echo "ERROR: HEAD is not tagged."
    exit 1
  fi
  if [[ -n "$expected_tag" && "$current_tag" != "$expected_tag" ]]; then
    echo "ERROR: expected HEAD tag '$expected_tag' but found '$current_tag'"
    exit 1
  fi
}

build_release() {
  rm -rf build dist "$CONDA_BLD_DIR"
  find . -maxdepth 1 -name "*.egg-info" -exec rm -rf {} +

  verify_head_tag
  TAG="$(git describe --tags --exact-match)"
  export GIT_DESCRIBE_TAG="$TAG"

  if "$RUN_ENV" "$ENV_NAME" python -c "import build" >/dev/null 2>&1; then
    if ! "$RUN_ENV" "$ENV_NAME" python -m build --no-isolation --skip-dependency-check; then
      "$RUN_ENV" "$ENV_NAME" python setup.py sdist bdist_wheel
    fi
  else
    "$RUN_ENV" "$ENV_NAME" python setup.py sdist bdist_wheel
  fi

  if "$RUN_ENV" "$ENV_NAME" python -c "import twine" >/dev/null 2>&1; then
    "$RUN_ENV" "$ENV_NAME" python -c "import pathlib, subprocess, sys; files=sorted(str(p) for p in pathlib.Path('dist').glob('*')); sys.exit(subprocess.run([sys.executable, '-m', 'twine', 'check', *files], check=False).returncode if files else 1)"
  else
    echo "WARNING: twine is not installed in $ENV_NAME; skipping twine check"
  fi

  CONDA_BUILD_EXE="$("$RUN_ENV" "$ENV_NAME" which conda-build)"
  if [[ -n "$CONDA_BUILD_EXE" ]]; then
    "$CONDA_BUILD_EXE" conda-recipe -c ionbus -c conda-forge --croot "$CONDA_BLD_DIR"
  elif command -v conda >/dev/null 2>&1; then
    conda build conda-recipe -c ionbus -c conda-forge --croot "$CONDA_BLD_DIR"
  else
    echo "ERROR: conda-build is not available in $ENV_NAME and conda is not on PATH"
    exit 1
  fi

  echo
  echo "Built pip artifacts in: $ROOT_DIR/dist"
  echo "Built conda artifacts in: $CONDA_BLD_DIR"
  echo "Version/tag used: $GIT_DESCRIBE_TAG"
}

send_release() {
  verify_head_tag
  TAG="$(git describe --tags --exact-match)"
  export GIT_DESCRIBE_TAG="$TAG"

  "$RUN_ENV" "$ENV_NAME" python -c "import pathlib, subprocess, sys; files=sorted(str(p) for p in pathlib.Path('dist').glob('*')); sys.exit(subprocess.run([sys.executable, '-m', 'twine', 'upload', *files], check=False).returncode if files else 1)"

  if command -v anaconda >/dev/null 2>&1; then
    mapfile -t conda_files < <(find "$CONDA_BLD_DIR" -type f \( -name "*.conda" -o -name "*.tar.bz2" \))
    if [[ ${#conda_files[@]} -eq 0 ]]; then
      echo "ERROR: no conda artifacts found in $CONDA_BLD_DIR"
      exit 1
    fi
    anaconda upload -u ionbus "${conda_files[@]}"
  else
    echo "ERROR: anaconda CLI is required to upload conda packages to ionbus::ionbus-parquet-cache"
    exit 1
  fi
}

maybe_tag_release() {
  if [[ "$TAG_FLAG" == "--tag" ]]; then
    CREATED_TAG="$("$RUN_ENV" "$ENV_NAME" python -m ionbus_utils.git_utils.auto_tag . --name-only | tail -n 1)"
    if [[ -z "$CREATED_TAG" ]]; then
      echo "ERROR: failed to compute new tag name"
      exit 1
    fi
    if git rev-parse -q --verify "refs/tags/$CREATED_TAG" >/dev/null; then
      echo "ERROR: tag '$CREATED_TAG' already exists locally"
      exit 1
    fi
    git tag -a "$CREATED_TAG" -m "auto-tag $CREATED_TAG"
    verify_head_tag "$CREATED_TAG"
    if [[ "$MODE" != "build" ]]; then
      git push origin "refs/tags/$CREATED_TAG"
    fi
  fi
}

if [[ "$MODE" == "build" ]]; then
  maybe_tag_release
  build_release
elif [[ "$MODE" == "send" ]]; then
  maybe_tag_release
  send_release
else
  maybe_tag_release
  build_release
  send_release
fi
