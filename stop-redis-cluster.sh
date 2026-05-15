#!/usr/bin/env bash
set -Eeuo pipefail

PORTS=(7001 7002 7003 7004 7005 7006)
ACL_USER="dk900912"
ACL_PASS="qwe@1234"

log() {
  printf '%s\n' "$*"
}

node_alive() {
  local port="$1"
  redis-cli --no-auth-warning --user "${ACL_USER}" -a "${ACL_PASS}" -p "$port" ping >/dev/null 2>&1
}

wait_node_down() {
  local port="$1"
  for _ in $(seq 1 20); do
    if ! node_alive "$port"; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

shutdown_node() {
  local port="$1"

  log "关闭节点 ${port} ..."

  if node_alive "$port"; then
    if redis-cli --no-auth-warning --user "${ACL_USER}" -a "${ACL_PASS}" -p "$port" shutdown nosave >/dev/null 2>&1; then
      :
    else
      log "节点 ${port} 发送 shutdown 命令失败"
      return 1
    fi

    if wait_node_down "$port"; then
      log "节点 ${port} 已关闭"
      return 0
    else
      log "节点 ${port} 在预期时间内未关闭"
      return 1
    fi
  else
    log "节点 ${port} 无响应或认证失败"
    return 1
  fi
}

main() {
  log "正在关闭 Redis 集群..."

  local failed=0

  for port in "${PORTS[@]}"; do
    if ! shutdown_node "$port"; then
      failed=1
    fi
  done

  if [[ "$failed" -eq 0 ]]; then
    log "Redis 集群已全部关闭"
  else
    log "Redis 集群部分节点关闭失败，请检查日志或端口状态"
    exit 1
  fi
}

main "$@"