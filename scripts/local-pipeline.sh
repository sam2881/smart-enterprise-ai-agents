#!/usr/bin/env bash
# scripts/local-pipeline.sh
# One-command local CI pipeline — build, migrate, test, smoke-test.
# Run from the project root: bash scripts/local-pipeline.sh
#
# Flags:
#   --skip-build    Use cached images (faster re-runs)
#   --skip-tests    Skip unit tests
#   --down          Tear down all containers after the run
#   --infra         Start only infra (no app services)

set -euo pipefail

SKIP_BUILD=0; SKIP_TESTS=0; TEARDOWN=0; INFRA_ONLY=0
for arg in "$@"; do
  case $arg in
    --skip-build)  SKIP_BUILD=1 ;;
    --skip-tests)  SKIP_TESTS=1 ;;
    --down)        TEARDOWN=1 ;;
    --infra)       INFRA_ONLY=1 ;;
  esac
done

COMPOSE_BASE="docker compose -f docker-compose.yml"
COMPOSE_FULL="docker compose -f docker-compose.yml -f docker-compose.local.yml"

PASSED=(); FAILED=()

cyan='\033[0;36m'; green='\033[0;32m'; red='\033[0;31m'; yellow='\033[0;33m'; reset='\033[0m'
step()  { echo -e "\n${cyan}==> $1${reset}"; }
ok()    { echo -e "  ${green}[OK]  $1${reset}"; }
fail()  { echo -e "  ${red}[FAIL] $1${reset}"; }
info()  { echo -e "  $1"; }
warn()  { echo -e "  ${yellow}[WARN] $1${reset}"; }

run_step() {
  local name="$1"; shift
  step "$name"
  if "$@"; then
    PASSED+=("$name"); ok "Done"
  else
    FAILED+=("$name"); fail "Failed"
    if [[ "$name" =~ Build|Infra|Migrate ]]; then
      echo -e "\n${red}Critical step failed — aborting.${reset}"
      summary; exit 1
    fi
  fi
}

summary() {
  echo -e "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo " LOCAL PIPELINE SUMMARY"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  for s in "${PASSED[@]}"; do echo -e "  ${green}PASS${reset}  $s"; done
  for s in "${FAILED[@]}"; do echo -e "  ${red}FAIL${reset}  $s"; done
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  if [ "${#FAILED[@]}" -eq 0 ]; then
    echo -e "\n  ${green}All checks passed. Safe to push to GCP.${reset}"
  else
    echo -e "\n  ${red}${#FAILED[@]} step(s) failed. Fix before pushing.${reset}"
  fi
}

wait_healthy() {
  local name="$1" url="$2" timeout="${3:-120}"
  info "Waiting for $name at $url..."
  local deadline=$(( $(date +%s) + timeout ))
  while [ $(date +%s) -lt $deadline ]; do
    if curl -sf "$url" > /dev/null 2>&1; then
      ok "$name is up"; return 0
    fi
    sleep 5
  done
  return 1
}

wait_container_healthy() {
  local name="$1" container="$2" timeout="${3:-90}"
  info "Waiting for $name..."
  local deadline=$(( $(date +%s) + timeout ))
  while [ $(date +%s) -lt $deadline ]; do
    status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "missing")
    if [ "$status" = "healthy" ]; then ok "$name healthy"; return 0; fi
    sleep 5
  done
  return 1
}

# ── Preflight ─────────────────────────────────────────────────────────────────
step "Preflight checks"
[ -f ".env" ] || { fail ".env not found. Copy .env.example and fill in ANTHROPIC_API_KEY."; exit 1; }
grep -q "ANTHROPIC_API_KEY=sk-" .env || warn "ANTHROPIC_API_KEY looks unset in .env"
docker info > /dev/null 2>&1 || { fail "Docker is not running."; exit 1; }
ok "Preflight passed"; PASSED+=("Preflight")

# ── Tear down if requested ────────────────────────────────────────────────────
[ "$TEARDOWN" -eq 1 ] && {
  run_step "Tear down existing containers" \
    bash -c "$COMPOSE_FULL down --remove-orphans -v > /dev/null 2>&1"
}

# ── Infra ─────────────────────────────────────────────────────────────────────
run_step "Start infrastructure services" \
  bash -c "$COMPOSE_BASE up -d postgres redis zookeeper kafka neo4j weaviate kafka-ui prometheus grafana tempo loki otel-collector langfuse"

run_step "Wait for Kafka" \
  wait_container_healthy "kafka" "agentic-kafka" 90

run_step "Wait for Postgres" \
  wait_container_healthy "postgres" "agentic-postgres" 60

if [ "$INFRA_ONLY" -eq 1 ]; then
  echo -e "\n${cyan}Infra-only mode. Services:${reset}"
  info "  Kafka UI   http://localhost:8090"
  info "  Grafana    http://localhost:3001"
  info "  Prometheus http://localhost:9090"
  info "  Langfuse   http://localhost:3002"
  info "  Airflow    http://localhost:8083"
  exit 0
fi

# ── Build ─────────────────────────────────────────────────────────────────────
if [ "$SKIP_BUILD" -eq 0 ]; then
  run_step "Build application images" \
    bash -c "$COMPOSE_FULL build --parallel"
else
  step "Build skipped (--skip-build)"; PASSED+=("Build application images")
fi

# ── Migrate ───────────────────────────────────────────────────────────────────
run_step "Run database migrations" \
  bash -c "$COMPOSE_FULL run --rm db-migrate"

# ── App services ──────────────────────────────────────────────────────────────
run_step "Start Backend API + Data Agent" \
  bash -c "$COMPOSE_FULL up -d backend-api data-agent"

run_step "Wait for Backend API" \
  wait_healthy "backend-api" "http://localhost:8000/health" 90

run_step "Wait for Data Agent" \
  wait_healthy "data-agent" "http://localhost:8001/health" 90

run_step "Start remaining app services" \
  bash -c "$COMPOSE_FULL up -d frontend event-orchestrator incident-consumer pipeline-consumer"

# ── Tests ─────────────────────────────────────────────────────────────────────
if [ "$SKIP_TESTS" -eq 0 ]; then
  run_step "Run unit tests" \
    bash -c "$COMPOSE_FULL run --rm --profile test test-runner"
fi

# ── Smoke tests ───────────────────────────────────────────────────────────────
run_step "Smoke test: Backend /health" \
  bash -c 'r=$(curl -sf http://localhost:8000/health); echo "$r" | grep -q "healthy"'

run_step "Smoke test: Data Agent /health" \
  bash -c 'r=$(curl -sf http://localhost:8001/health); echo "$r" | grep -q "healthy"'

run_step "Smoke test: Frontend loads" \
  bash -c 'curl -sf http://localhost:3000 > /dev/null'

run_step "Smoke test: Kafka broker reachable" \
  bash -c 'docker exec agentic-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1'

# ── Summary ───────────────────────────────────────────────────────────────────
summary

echo -e "\n  ${cyan}URLs:${reset}"
info "  Frontend       http://localhost:3000"
info "  Backend API    http://localhost:8000/docs"
info "  Data Agent     http://localhost:8001/docs"
info "  Kafka UI       http://localhost:8090"
info "  Grafana        http://localhost:3001"
info "  Langfuse       http://localhost:3002"
info "  Airflow        http://localhost:8083"

[ "$TEARDOWN" -eq 1 ] && {
  step "Tearing down"
  eval "$COMPOSE_FULL down --remove-orphans"
}

[ "${#FAILED[@]}" -gt 0 ] && exit 1
exit 0
