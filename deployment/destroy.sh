#!/usr/bin/env bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

stop_flow_visibility() {
  echo "=== Stopping Flow Visibility ==="
  kubectl delete -f ${THIS_DIR}/../cluster/flow-visibility-cluster.yml -n flow-visibility
  kubectl delete namespace flow-visibility
  kubectl delete -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle.yaml -n kube-system
}

stop_flow_visibility