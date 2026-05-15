#!/usr/bin/env bash
set -Eeuo pipefail

BASE_DIR="${HOME}/redis-cluster"
PORTS=(7001 7002 7003 7004 7005 7006)

ACL_USER="dk900912"
ACL_PASS="qwe@1234"

log() {
  printf '%s\n' "$*"
}

redis_major() {
  redis-server --version 2>/dev/null | awk -F'=' '{print $2}' | awk '{print $1}' | cut -d. -f1
}

redis_is_7_plus() {
  local major
  major="$(redis_major || true)"
  [[ -n "${major}" && "${major}" -ge 7 ]]
}

install_redis() {
  if command -v redis-server >/dev/null 2>&1 && command -v redis-cli >/dev/null 2>&1 && redis_is_7_plus; then
    log "Redis 已就绪: $(redis-server --version | head -1)"
    return
  fi

  log "切换并安装 Redis 7..."
  sudo dnf -y module reset redis || true
  sudo dnf -y module enable redis:7
  sudo dnf -y install redis

  if ! command -v redis-server >/dev/null 2>&1 || ! command -v redis-cli >/dev/null 2>&1; then
    log "redis-server 或 redis-cli 不存在，安装失败"
    exit 1
  fi

  if ! redis_is_7_plus; then
    log "安装后仍不是 Redis 7+"
    redis-server --version || true
    exit 1
  fi

  log "Redis 已安装: $(redis-server --version | head -1)"
}

validate_acl_config() {
  if [[ -z "${ACL_USER}" || -z "${ACL_PASS}" ]]; then
    log "ACL_USER 或 ACL_PASS 未配置"
    exit 1
  fi
}

cleanup() {
  log "清理旧环境..."
  pkill -f "redis-server .*${BASE_DIR}" || true
  sleep 1
  rm -rf "${BASE_DIR}"
  mkdir -p "${BASE_DIR}"
}

gen_config() {
  log "生成节点配置..."

  for port in "${PORTS[@]}"; do
    local node_dir="${BASE_DIR}/${port}"
    mkdir -p "${node_dir}"

    cat > "${node_dir}/users.acl" <<EOF
user default off
user ${ACL_USER} on >${ACL_PASS} allcommands allkeys allchannels
EOF

    cat > "${node_dir}/redis.conf" <<EOF
port ${port}
bind 127.0.0.1
protected-mode no
daemonize yes

dir ${node_dir}
dbfilename dump.rdb
pidfile ${node_dir}/redis.pid
logfile ${node_dir}/redis.log

cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000

appendonly yes
appendfilename appendonly.aof

aclfile ${node_dir}/users.acl

masteruser ${ACL_USER}
masterauth ${ACL_PASS}
EOF
  done
}

wait_node_ready() {
  local port="$1"

  for _ in $(seq 1 60); do
    if redis-cli --user "${ACL_USER}" -a "${ACL_PASS}" -p "${port}" ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done

  return 1
}

start_nodes() {
  log "启动 Redis 节点..."

  for port in "${PORTS[@]}"; do
    redis-server "${BASE_DIR}/${port}/redis.conf"
  done

  for port in "${PORTS[@]}"; do
    log "  - 等待 ${port} 就绪..."
    if ! wait_node_ready "${port}"; then
      log "节点 ${port} 启动失败，查看日志：${BASE_DIR}/${port}/redis.log"
      exit 1
    fi
  done
}

create_cluster() {
  log "创建集群..."

  local hosts=()
  for port in "${PORTS[@]}"; do
    hosts+=("127.0.0.1:${port}")
  done

  printf 'yes\n' | redis-cli --user "${ACL_USER}" -a "${ACL_PASS}" --cluster create "${hosts[@]}" --cluster-replicas 1
}

check_cluster() {
  log "检查集群状态..."
  redis-cli -c --user "${ACL_USER}" -a "${ACL_PASS}" -p 7001 cluster info
  echo
  redis-cli -c --user "${ACL_USER}" -a "${ACL_PASS}" -p 7001 cluster nodes
}

check_acl() {
  log "检查 ACL 用户..."
  redis-cli --user "${ACL_USER}" -a "${ACL_PASS}" -p 7001 ACL GETUSER "${ACL_USER}"
}

show_usage() {
  echo
  log "Redis 集群已就绪"
  log "ACL 用户: ${ACL_USER}"
  log "ACL 权限: allcommands + allkeys + allchannels"
  log "测试连接:"
  log "  redis-cli -c --user ${ACL_USER} -a '${ACL_PASS}' -p 7001"
  log "查看集群:"
  log "  redis-cli -c --user ${ACL_USER} -a '${ACL_PASS}' -p 7001 cluster info"
  log "查看 ACL:"
  log "  redis-cli --user ${ACL_USER} -a '${ACL_PASS}' -p 7001 ACL GETUSER ${ACL_USER}"
}

main() {
  validate_acl_config
  install_redis
  cleanup
  gen_config
  start_nodes
  create_cluster
  check_cluster
  check_acl
  show_usage
}

main "$@"