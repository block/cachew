#!/usr/bin/env bash
# cachew-incremental-report.sh: effectiveness report for cachew's git
# incremental pull-through feature, read from Prometheus.
#
# Metrics used (see cachew git strategy metrics.go):
#   cachew_git_incremental_serves_total{outcome, repository}          counter
#   cachew_git_incremental_fetch_duration_seconds{outcome, repository} histogram
#   cachew_git_incremental_eligible_total{engaged, repository}         counter
#
# Usage:
#   ./cachew-incremental-report.sh [-u prometheus-url] [-n namespace] [-r range] [-t top-n] [--no-color]
#
# Prometheus URL can also come from $PROMETHEUS_URL / $PROM_URL.

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults / argument parsing
# ---------------------------------------------------------------------------

PROM_URL="${PROMETHEUS_URL:-${PROM_URL:-}}"
NAMESPACE="cachew"
RANGE="24h"
TOP_N=5
USE_COLOR=1

# Canonical outcome order (matches internal/strategy/git/incremental.go).
OUTCOME_ORDER=(
  local_hit
  fetched
  fallback_fetch_failed
  fallback_missing
  fallback_local_error
  client_gone
  client_gone_after_fetch
  fallback_not_our_ref
)

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  -u, --url URL         Prometheus base URL (e.g. https://prometheus.ci.tenstorrent.net)
                         Defaults to \$PROMETHEUS_URL or \$PROM_URL.
  -n, --namespace NS     Namespace label to filter on (default: cachew; use '' to omit)
  -r, --range RANGE      Window to report counter growth over (default: 24h)
  -t, --top N            Number of top fallback repos to show (default: 5)
      --no-color         Disable ANSI color output
  -h, --help             Show this help

Examples:
  $(basename "$0") -u https://prometheus.ci.tenstorrent.net
  PROMETHEUS_URL=https://prometheus.ci.tenstorrent.net $(basename "$0") -r 7d
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -u|--url)
      [[ $# -lt 2 ]] && { echo "error: -u/--url requires a URL argument" >&2; usage >&2; exit 1; }
      PROM_URL="$2"; shift 2 ;;
    -n|--namespace)
      [[ $# -lt 2 ]] && { echo "error: -n/--namespace requires a value" >&2; usage >&2; exit 1; }
      NAMESPACE="$2"; shift 2 ;;
    -r|--range)
      [[ $# -lt 2 ]] && { echo "error: -r/--range requires a duration" >&2; usage >&2; exit 1; }
      RANGE="$2"; shift 2 ;;
    -t|--top)
      [[ $# -lt 2 ]] && { echo "error: -t/--top requires a number" >&2; usage >&2; exit 1; }
      TOP_N="$2"; shift 2 ;;
    --no-color) USE_COLOR=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 1 ;;
  esac
done

if [[ -z "$PROM_URL" ]]; then
  echo "error: no Prometheus URL given. Use -u/--url or set \$PROMETHEUS_URL." >&2
  exit 1
fi
PROM_URL="${PROM_URL%/}"

if (( BASH_VERSINFO[0] < 4 )); then
  echo "error: bash >= 4 is required (on macOS: brew install bash)" >&2
  exit 1
fi

for bin in curl jq awk seq; do
  command -v "$bin" >/dev/null 2>&1 || { echo "error: '$bin' is required but not found in PATH." >&2; exit 1; }
done

QUERY_HAD_ERROR=0
TMP_FILES=()

cleanup() {
  local f
  for f in "${TMP_FILES[@]}"; do
    rm -f "$f"
  done
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Colors (disabled if not a tty or --no-color)
# ---------------------------------------------------------------------------

if [[ $USE_COLOR -eq 1 && -t 1 ]]; then
  BOLD=$'\e[1m'; DIM=$'\e[2m'; RED=$'\e[31m'; GREEN=$'\e[32m'; YELLOW=$'\e[33m'; CYAN=$'\e[36m'; RESET=$'\e[0m'
else
  BOLD=""; DIM=""; RED=""; GREEN=""; YELLOW=""; CYAN=""; RESET=""
fi

# ---------------------------------------------------------------------------
# Prometheus query helper
# ---------------------------------------------------------------------------

# ns_label_prefix returns "namespace=\"foo\"," when filtering, or empty when -n ''.
ns_label_prefix() {
  if [[ -n "$NAMESPACE" ]]; then
    printf 'namespace="%s",' "$NAMESPACE"
  fi
}

# counter_window <metric> [extra-selector]: PromQL for how much a counter grew
# within RANGE.
#
# increase() alone undercounts here: it baselines on the first sample inside the
# window, so a series that first appears mid-window or ticks once and stays flat
# reports 0 - and rare outcomes like fetched and fallback_* are exactly the
# sparse series this report exists to surface. Differencing the counter across
# the window edges instead (falling back to the raw value, which already counts
# from zero at process start, when there is no earlier sample) is exact for
# those, but loses whatever was counted before a restart. Neither is right on
# its own, so take whichever is larger, and keep the difference for series
# increase() drops entirely for want of a second sample.
counter_window() {
  local metric="$1" sel="${2:-}" cur prev diff win
  cur="last_over_time(${metric}{${NSPF}${sel}}[${RANGE}])"
  prev="last_over_time(${metric}{${NSPF}${sel}}[${RANGE}] offset ${RANGE})"
  diff="(((${cur}) - (${prev}) >= 0) or (${cur}))"
  win="increase(${metric}{${NSPF}${sel}}[${RANGE}])"
  printf '((%s >= %s) or %s or %s)' "$diff" "$win" "$win" "$diff"
}

# prom_query <promql>: prints the raw JSON `data.result` array on success,
# prints nothing and returns 1 on any error or non-"success" status.
prom_query() {
  local query="$1" resp curl_err status
  local err_file
  err_file="$(mktemp -t cachew-report-curl-err.XXXXXX)"
  TMP_FILES+=("$err_file")
  if ! resp="$(curl -sS -G --max-time 15 --data-urlencode "query=${query}" "${PROM_URL}/api/v1/query" 2>"${err_file}")"; then
    curl_err="$(cat "${err_file}" 2>/dev/null)"
    echo "warning: could not reach Prometheus at ${PROM_URL}: ${curl_err}" >&2
    return 1
  fi
  status="$(jq -r '.status // "error"' <<<"$resp" 2>/dev/null)" || status="error"
  if [[ "$status" != "success" ]]; then
    echo "warning: query failed: ${query}" >&2
    echo "  $(jq -r '.error // "unrecognized response"' <<<"$resp" 2>/dev/null)" >&2
    return 1
  fi
  local result
  result="$(jq -c '.data.result // []' <<<"$resp" 2>/dev/null)" || result='[]'
  if [[ "$result" == "null" ]]; then
    result='[]'
  fi
  echo "$result"
}

# ---------------------------------------------------------------------------
# Small formatting helpers
# ---------------------------------------------------------------------------

pct() { # pct <numerator> <denominator> -> "12.3"
  awk -v a="$1" -v b="$2" 'BEGIN { if (b+0 == 0) { print "0.0" } else { printf "%.1f", (a/b)*100 } }'
}

fnum() { # fnum <float> -> human count, thousands-separated, no decimals
  awk -v v="$1" 'BEGIN {
    n = sprintf("%.0f", v)
    neg = ""
    if (n ~ /^-/) { neg = "-"; n = substr(n, 2) }
    out = ""; len = length(n)
    for (i = 1; i <= len; i++) {
      out = out substr(n, i, 1)
      rem = len - i
      if (rem > 0 && rem % 3 == 0) out = out ","
    }
    print neg out
  }'
}

fdur() { # fdur <seconds> -> "1.234s" or "12.3ms"
  awk -v s="$1" 'BEGIN {
    if (s+0 < 1) printf "%.1fms", s*1000
    else printf "%.3fs", s
  }'
}

bar() { # bar <pct 0-100> <width> -> a simple ASCII bar
  local p="$1" width="${2:-30}" filled empty i
  filled=$(awk -v p="$p" -v w="$width" 'BEGIN { n=int(p/100*w+0.5); if (n<0) n=0; if (n>w) n=w; print n }')
  empty=$((width - filled))
  if (( filled > 0 )); then
    for ((i = 1; i <= filled; i++)); do printf '#'; done
  fi
  if (( empty > 0 )); then
    for ((i = 1; i <= empty; i++)); do printf '.'; done
  fi
}

hr() { printf '%s\n' "${DIM}$(printf '─%.0s' $(seq 1 72))${RESET}"; }

# ---------------------------------------------------------------------------
# Namespace pre-flight
# ---------------------------------------------------------------------------

# The check spans every incremental instrument, not just serves_total: a
# deployment where incremental is enabled but has not engaged yet exports
# eligible_total with engaged="false" and no serves_total at all, and that is a
# result worth reporting rather than an error worth aborting on.
if [[ -n "$NAMESPACE" ]]; then
  ns_check="$(prom_query "count by (namespace) (cachew_git_incremental_serves_total or cachew_git_incremental_eligible_total or cachew_git_incremental_fetch_duration_seconds_count)")" || { QUERY_HAD_ERROR=1; ns_check='[]'; }
  if [[ "$ns_check" == "[]" || -z "$ns_check" ]]; then
    echo "${YELLOW}warning: no cachew incremental series found in Prometheus.${RESET}" >&2
    echo "  Incremental pull-through may be disabled, or the metrics may not be scraped yet." >&2
    echo "  The report below will be empty; re-run with -n '' to omit the namespace filter." >&2
  elif ! jq -e --arg ns "$NAMESPACE" '.[] | select(.metric.namespace == $ns)' <<<"$ns_check" >/dev/null 2>&1; then
    echo "${RED}error: no cachew incremental series with namespace=\"${NAMESPACE}\".${RESET}" >&2
    echo "  Namespaces present:" >&2
    jq -r '.[].metric.namespace // "<missing>"' <<<"$ns_check" | sort -u | sed 's/^/    /' >&2
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

echo "${BOLD}${CYAN}Cachew Incremental Pull-Through Report${RESET}"
echo "${DIM}Prometheus: ${PROM_URL}   namespace=${NAMESPACE:-<none>}   range=${RANGE}   $(date -u '+%Y-%m-%d %H:%M:%S UTC')${RESET}"
hr

NSPF="$(ns_label_prefix)"

# ---------------------------------------------------------------------------
# 1. Outcome breakdown (cachew_git_incremental_serves_total)
# ---------------------------------------------------------------------------

declare -A OUTCOME
for o in "${OUTCOME_ORDER[@]}"; do OUTCOME[$o]=0; done

result="$(prom_query "sum by (outcome) ($(counter_window cachew_git_incremental_serves_total))")" || { QUERY_HAD_ERROR=1; result='[]'; }
if [[ "$result" != "[]" && -n "$result" ]]; then
  while IFS=$'\t' read -r outcome value; do
    [[ -n "$outcome" ]] && OUTCOME[$outcome]="$value"
  done < <(jq -r '.[] | "\(.metric.outcome)\t\(.value[1])"' <<<"$result")
fi

TOTAL_SERVES=0
for o in "${OUTCOME_ORDER[@]}"; do
  TOTAL_SERVES=$(awk -v t="$TOTAL_SERVES" -v v="${OUTCOME[$o]:-0}" 'BEGIN{printf "%.4f", t+v}')
done

echo "${BOLD}1. Outcome breakdown${RESET} ${DIM}(incremental_serves_total, last ${RANGE})${RESET}"
if awk -v t="$TOTAL_SERVES" 'BEGIN{exit !(t==0)}'; then
  echo "  ${YELLOW}no incremental-serve events in this range. The feature may be disabled or not deployed, or there was no traffic.${RESET}"
else
  printf "  %-28s %10s  %6s  %s\n" "outcome" "count" "pct" ""
  for o in "${OUTCOME_ORDER[@]}"; do
    p="$(pct "${OUTCOME[$o]:-0}" "$TOTAL_SERVES")"
    color="$RESET"
    case "$o" in
      local_hit|fetched) color="$GREEN" ;;
      fallback_fetch_failed|fallback_local_error) color="$RED" ;;
      fallback_missing|fallback_not_our_ref) color="$YELLOW" ;;
      client_gone|client_gone_after_fetch) color="$DIM" ;;
    esac
    printf "  %-28s %10s  %5s%%  ${color}%s${RESET}\n" "$o" "$(fnum "${OUTCOME[$o]:-0}")" "$p" "$(bar "$p" 20)"
  done
  printf "  %-28s %10s\n" "TOTAL" "$(fnum "$TOTAL_SERVES")"
fi
echo

# ---------------------------------------------------------------------------
# 2. Effectiveness: engaged vs pure cache miss
# ---------------------------------------------------------------------------

ENGAGED=$(awk -v a="${OUTCOME[local_hit]:-0}" -v b="${OUTCOME[fetched]:-0}" 'BEGIN{printf "%.4f", a+b}')
PURE_MISS=$(awk -v a="${OUTCOME[fallback_fetch_failed]:-0}" -v b="${OUTCOME[fallback_missing]:-0}" -v c="${OUTCOME[fallback_local_error]:-0}" -v d="${OUTCOME[client_gone]:-0}" -v e="${OUTCOME[client_gone_after_fetch]:-0}" -v f="${OUTCOME[fallback_not_our_ref]:-0}" 'BEGIN{printf "%.4f", a+b+c+d+e+f}')
BEHIND_TOTAL=$(awk -v a="${OUTCOME[fetched]:-0}" -v b="$PURE_MISS" 'BEGIN{printf "%.4f", a+b}')

echo "${BOLD}2. Feature effectiveness${RESET}"
if awk -v t="$TOTAL_SERVES" 'BEGIN{exit !(t==0)}'; then
  echo "  ${DIM}n/a (no data)${RESET}"
else
  engaged_pct="$(pct "$ENGAGED" "$TOTAL_SERVES")"
  miss_pct="$(pct "$PURE_MISS" "$TOTAL_SERVES")"
  echo "  Served without a full upstream passthrough (local_hit + fetched): ${GREEN}${engaged_pct}%${RESET} of incremental traffic"
  echo "  Fell back to a full passthrough (fallback_* + client_gone*): ${RED}${miss_pct}%${RESET} of incremental traffic  ${DIM}(same cost as pre-feature behavior)${RESET}"
  if awk -v t="$BEHIND_TOTAL" 'BEGIN{exit !(t==0)}'; then
    echo "  ${DIM}Mirror was never behind in this range, so there is no fetch-vs-fallback signal to report.${RESET}"
  else
    save_pct="$(pct "${OUTCOME[fetched]:-0}" "$BEHIND_TOTAL")"
    echo "  When the mirror was behind, incremental avoided a full re-clone: ${GREEN}${save_pct}%${RESET} of the time"
  fi
fi
echo

# ---------------------------------------------------------------------------
# 3. Traffic coverage (cachew_git_incremental_eligible_total)
# ---------------------------------------------------------------------------

declare -A ELIGIBLE
ELIGIBLE[true]=0
ELIGIBLE[false]=0
result="$(prom_query "sum by (engaged) ($(counter_window cachew_git_incremental_eligible_total))")" || { QUERY_HAD_ERROR=1; result='[]'; }
if [[ "$result" != "[]" && -n "$result" ]]; then
  while IFS=$'\t' read -r engaged value; do
    [[ -n "$engaged" ]] && ELIGIBLE[$engaged]="$value"
  done < <(jq -r '.[] | "\(.metric.engaged)\t\(.value[1])"' <<<"$result")
fi
ELIGIBLE_TOTAL=$(awk -v a="${ELIGIBLE[true]}" -v b="${ELIGIBLE[false]}" 'BEGIN{printf "%.4f", a+b}')

echo "${BOLD}3. Traffic coverage${RESET} ${DIM}(incremental_eligible_total: upload-pack POSTs against a ready mirror)${RESET}"
if awk -v t="$ELIGIBLE_TOTAL" 'BEGIN{exit !(t==0)}'; then
  echo "  ${DIM}n/a (no data; metric may predate this deployment)${RESET}"
else
  engage_rate="$(pct "${ELIGIBLE[true]}" "$ELIGIBLE_TOTAL")"
  echo "  Engaged (had wants to check):     $(fnum "${ELIGIBLE[true]}")  (${engage_rate}%)"
  echo "  Skipped (ls-refs/no-wants/etc.):  $(fnum "${ELIGIBLE[false]}")  ($(pct "${ELIGIBLE[false]}" "$ELIGIBLE_TOTAL")%)"
  if ! awk -v t="$TOTAL_SERVES" 'BEGIN{exit !(t==0)}'; then
    drift="$(pct "$(awk -v a="${ELIGIBLE[true]}" -v b="$TOTAL_SERVES" 'BEGIN{printf "%.4f", (a>b)?a-b:b-a}')" "$TOTAL_SERVES")"
    if awk -v d="$drift" 'BEGIN{exit !(d>5)}'; then
      echo "  ${YELLOW}note: engaged count differs from incremental_serves_total by ${drift}%. Check for a scrape gap, a restart, incremental-pullthrough disabled in this range, or bodies mixing want and want-ref.${RESET}"
    fi
  fi
fi
echo

# ---------------------------------------------------------------------------
# 4. Latency (cachew_git_incremental_fetch_duration_seconds)
# ---------------------------------------------------------------------------

echo "${BOLD}4. Latency by outcome${RESET} ${DIM}(incremental_fetch_duration_seconds)${RESET}"
declare -A AVG_SUM AVG_COUNT P50 P95 P99

result="$(prom_query "sum by (outcome) ($(counter_window cachew_git_incremental_fetch_duration_seconds_sum))")" || { QUERY_HAD_ERROR=1; result='[]'; }
[[ "$result" != "[]" && -n "$result" ]] && while IFS=$'\t' read -r o v; do [[ -n "$o" ]] && AVG_SUM[$o]="$v"; done < <(jq -r '.[] | "\(.metric.outcome)\t\(.value[1])"' <<<"$result")

result="$(prom_query "sum by (outcome) ($(counter_window cachew_git_incremental_fetch_duration_seconds_count))")" || { QUERY_HAD_ERROR=1; result='[]'; }
[[ "$result" != "[]" && -n "$result" ]] && while IFS=$'\t' read -r o v; do [[ -n "$o" ]] && AVG_COUNT[$o]="$v"; done < <(jq -r '.[] | "\(.metric.outcome)\t\(.value[1])"' <<<"$result")

for q in 50 95 99; do
  result="$(prom_query "histogram_quantile(0.${q}, sum by (le, outcome) ($(counter_window cachew_git_incremental_fetch_duration_seconds_bucket)))")" || { QUERY_HAD_ERROR=1; result='[]'; }
  if [[ "$result" != "[]" && -n "$result" ]]; then
    while IFS=$'\t' read -r o v; do
      [[ -n "$o" ]] || continue
      case "$q" in
        50) P50[$o]="$v" ;;
        95) P95[$o]="$v" ;;
        99) P99[$o]="$v" ;;
      esac
    done < <(jq -r '.[] | "\(.metric.outcome)\t\(.value[1])"' <<<"$result")
  fi
done

any_duration=0
for o in "${OUTCOME_ORDER[@]}"; do
  [[ -n "${AVG_COUNT[$o]:-}" && "${AVG_COUNT[$o]:-0}" != "0" ]] && any_duration=1
done

if [[ $any_duration -eq 0 ]]; then
  echo "  ${DIM}n/a (no data)${RESET}"
else
  printf "  %-28s %10s %10s %10s %10s\n" "outcome" "avg" "p50" "p95" "p99"
  for o in "${OUTCOME_ORDER[@]}"; do
    cnt="${AVG_COUNT[$o]:-0}"
    if awk -v c="$cnt" 'BEGIN{exit !(c==0)}'; then
      printf "  %-28s %10s\n" "$o" "no data"
      continue
    fi
    avg=$(awk -v s="${AVG_SUM[$o]:-0}" -v c="$cnt" 'BEGIN{printf "%.6f", s/c}')
    printf "  %-28s %10s %10s %10s %10s\n" "$o" "$(fdur "$avg")" "$(fdur "${P50[$o]:-0}")" "$(fdur "${P95[$o]:-0}")" "$(fdur "${P99[$o]:-0}")"
  done
  echo "  ${DIM}local_hit's near-zero cost is the baseline; fetched/fallback_* show the network cost.${RESET}"
fi
echo

# ---------------------------------------------------------------------------
# 5. Top repos falling back
# ---------------------------------------------------------------------------

echo "${BOLD}5. Top ${TOP_N} repos falling back to full passthrough${RESET} ${DIM}(fallback_* + client_gone*)${RESET}"
result="$(prom_query "topk(${TOP_N}, sum by (repository) ($(counter_window cachew_git_incremental_serves_total 'outcome=~"fallback_.*|client_gone.*"')))")" || { QUERY_HAD_ERROR=1; result='[]'; }
if [[ "$result" == "[]" || -z "$result" || "$(jq 'length' <<<"$result")" == "0" ]]; then
  echo "  ${GREEN}none (no fallbacks in this range)${RESET}"
else
  i=1
  while IFS=$'\t' read -r repo value; do
    printf "  %2d. %-50s %8s\n" "$i" "$repo" "$(fnum "$value")"
    i=$((i + 1))
  done < <(jq -r 'sort_by(-(.value[1]|tonumber)) | .[] | "\(.metric.repository)\t\(.value[1])"' <<<"$result")
fi
echo

# ---------------------------------------------------------------------------
# 6. Other cachew git traffic, for context
# ---------------------------------------------------------------------------

echo "${BOLD}6. Other git request volume${RESET} ${DIM}(cachew_git_requests_total: snapshot/bundle/lfs-snapshot/ensure-refs/receive-pack/forward; upload-pack is not in this counter)${RESET}"
result="$(prom_query "sum by (type) ($(counter_window cachew_git_requests_total))")" || { QUERY_HAD_ERROR=1; result='[]'; }
if [[ "$result" == "[]" || -z "$result" || "$(jq 'length' <<<"$result")" == "0" ]]; then
  echo "  ${DIM}n/a (no data)${RESET}"
else
  while IFS=$'\t' read -r type value; do
    printf "  %-24s %10s\n" "$type" "$(fnum "$value")"
  done < <(jq -r '.[] | "\(.metric.type)\t\(.value[1])"' <<<"$result")
fi

hr
if [[ $QUERY_HAD_ERROR -eq 1 ]]; then
  echo "${RED}${BOLD}One or more queries above failed (see warnings on stderr). The \"n/a\" and zero sections may reflect that rather than genuinely empty data.${RESET}"
fi
echo "${DIM}Queries are plain PromQL against ${PROM_URL}/api/v1/query. See the script source for the exact expressions.${RESET}"
