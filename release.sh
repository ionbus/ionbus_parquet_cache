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

get_conda_build_exe() {
  local conda_build_exe

  conda_build_exe="$("$RUN_ENV" "$ENV_NAME" which conda-build 2>/dev/null || true)"
  if [[ -n "$conda_build_exe" ]]; then
    printf '%s\n' "$conda_build_exe"
    return 0
  fi
  if command -v conda >/dev/null 2>&1; then
    printf '%s\n' "conda"
    return 0
  fi
  echo "ERROR: conda-build is not available in $ENV_NAME and conda is not on PATH"
  exit 1
}

get_anaconda_exe() {
  local anaconda_exe

  anaconda_exe="$("$RUN_ENV" "$ENV_NAME" which anaconda 2>/dev/null || true)"
  if [[ -n "$anaconda_exe" ]]; then
    printf '%s\n' "$anaconda_exe"
    return 0
  fi
  if command -v anaconda >/dev/null 2>&1; then
    printf '%s\n' "anaconda"
    return 0
  fi
  echo "ERROR: anaconda-client is not available in $ENV_NAME and anaconda is not on PATH"
  exit 1
}

get_conda_output_path() {
  local conda_build_exe

  conda_build_exe="$(get_conda_build_exe)"
  if [[ "$conda_build_exe" == "conda" ]]; then
    conda build conda-recipe -c ionbus -c conda-forge --croot "$CONDA_BLD_DIR" --output | tail -n 1
  else
    "$conda_build_exe" conda-recipe -c ionbus -c conda-forge --croot "$CONDA_BLD_DIR" --output | tail -n 1
  fi
}

get_next_tag_name() {
  local output tag

  output="$("$RUN_ENV" "$ENV_NAME" python -m ionbus_utils.git_utils.auto_tag . --name-only 2>&1)"
  tag="$(printf '%s\n' "$output" | sed -nE "s/.*tag='([^']+)'.*/\1/p" | tail -n 1)"
  if [[ -z "$tag" ]]; then
    tag="$(printf '%s\n' "$output" | awk 'NF { print }' | tail -n 1)"
  fi
  printf '%s\n' "$tag"
}

verify_dist_artifacts() {
  local tag="$1"

  "$RUN_ENV" "$ENV_NAME" python -c "import pathlib, sys; tag=sys.argv[1]; files=sorted(pathlib.Path('dist').iterdir()) if pathlib.Path('dist').is_dir() else []; wheel=[p for p in files if p.suffix=='.whl' and tag in p.name]; sdist=[p for p in files if p.name.endswith('.tar.gz') and tag in p.name]; sys.exit(0 if wheel and sdist else 1)" "$tag" || {
    echo "ERROR: expected wheel and sdist for tag $tag in dist/"
    exit 1
  }
}

build_release() {
  local tag conda_build_exe conda_output_path

  rm -rf build dist "$CONDA_BLD_DIR"
  find . -maxdepth 1 -name "*.egg-info" -exec rm -rf {} +

  verify_head_tag
  tag="$(git describe --tags --exact-match)"
  export GIT_DESCRIBE_TAG="$tag"

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

  verify_dist_artifacts "$tag"

  conda_build_exe="$(get_conda_build_exe)"
  conda_output_path="$(get_conda_output_path)"
  if [[ "$conda_build_exe" == "conda" ]]; then
    conda build conda-recipe -c ionbus -c conda-forge --croot "$CONDA_BLD_DIR"
  else
    "$conda_build_exe" conda-recipe -c ionbus -c conda-forge --croot "$CONDA_BLD_DIR"
  fi
  if [[ ! -f "$conda_output_path" ]]; then
    echo "ERROR: expected conda artifact was not created: $conda_output_path"
    exit 1
  fi

  echo
  echo "Built pip artifacts in: $ROOT_DIR/dist"
  echo "Built conda artifact: $conda_output_path"
  echo "Version/tag used: $tag"
}

send_release() {
  local tag conda_output_path anaconda_exe

  verify_head_tag
  tag="$(git describe --tags --exact-match)"
  export GIT_DESCRIBE_TAG="$tag"
  verify_dist_artifacts "$tag"
  conda_output_path="$(get_conda_output_path)"
  if [[ ! -f "$conda_output_path" ]]; then
    echo "ERROR: expected conda artifact is missing: $conda_output_path"
    exit 1
  fi

  "$RUN_ENV" "$ENV_NAME" python -c "import pathlib, subprocess, sys; files=sorted(str(p) for p in pathlib.Path('dist').glob('*')); sys.exit(subprocess.run([sys.executable, '-m', 'twine', 'upload', *files], check=False).returncode if files else 1)"

  anaconda_exe="$(get_anaconda_exe)"
  "$anaconda_exe" -s anaconda.org upload -u ionbus "$conda_output_path"
}

maybe_tag_release() {
  if [[ "$TAG_FLAG" == "--tag" ]]; then
    CREATED_TAG="$(get_next_tag_name)"
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
    echo "Created local tag: $CREATED_TAG"
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
