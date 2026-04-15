#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOOLS_DIR="${ROOT_DIR}/.tools"
JAR_PATH="${TOOLS_DIR}/tla2tools.jar"
JAR_URL="https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar"

mkdir -p "${TOOLS_DIR}"

if ! command -v java >/dev/null 2>&1; then
  echo "FAIL: Java is required to run TLC." >&2
  exit 1
fi

if [[ ! -f "${JAR_PATH}" ]]; then
  if [[ -f "${ROOT_DIR}/../../question-market-contracts/tla/.tools/tla2tools.jar" ]]; then
    cp "${ROOT_DIR}/../../question-market-contracts/tla/.tools/tla2tools.jar" "${JAR_PATH}"
  else
    echo "Downloading tla2tools.jar into ${JAR_PATH}" >&2
    curl -fsSL "${JAR_URL}" -o "${JAR_PATH}"
  fi
fi

run_tlc() {
  local module="$1"
  local cfg="$2"
  local label="$3"
  set +e
  java -Xss32m -cp "${JAR_PATH}" tlc2.TLC -config "${cfg}" "${ROOT_DIR}/${module}.tla" -workers auto
  local s=$?
  set -e
  if [[ ${s} -eq 0 ]]; then
    echo "PASS: ${label}"
  else
    echo "FAIL: ${label} (see TLC output above)." >&2
  fi
  return "${s}"
}

status=0
run_tlc "DurableExecution" "${ROOT_DIR}/DurableExecution.cfg" "DurableExecution.cfg (two workers)" || status=$?
run_tlc "DurableExecution" "${ROOT_DIR}/DurableExecution.single_worker.cfg" "DurableExecution.single_worker.cfg" || status=$?
run_tlc "DurableDagFrontier" "${ROOT_DIR}/DurableDagFrontier.cfg" "DurableDagFrontier.cfg" || status=$?

rm -rf "${ROOT_DIR}/states"

if [[ ${status} -eq 0 ]]; then
  echo "PASS: All TLC model checks completed without invariant or property violations."
else
  echo "FAIL: One or more TLC runs failed." >&2
fi

exit "${status}"
