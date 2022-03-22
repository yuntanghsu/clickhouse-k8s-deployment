#!/usr/bin/env bash

# Copyright 2022 Antrea Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

start_flow_visibility() {
  echo "=== Starting Flow Visibility ==="
  # install Clickhouse operator
  kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle.yaml
  kubectl create namespace flow-visibility
  kubectl create configmap clickhouse-mounted-configmap -n flow-visibility --from-file=${THIS_DIR}/datasources/
  kubectl apply -f ${THIS_DIR}/flow-visibility-pv.yml -n flow-visibility

  echo "=== Waiting for Clickhouse to be ready ==="
  sleep 15
  kubectl wait --for=condition=ready pod -l app=clickhouse-operator -n kube-system --timeout=60s
  kubectl wait --for=condition=ready pod -l app=clickhouse -n flow-visibility --timeout=120s

  CLICKHOUSE_HOST=$(kubectl get svc clickhouse-clickhouse -n flow-visibility -o jsonpath='{.spec.clusterIP}')
  echo "=== Clickhouse can be connected at ${CLICKHOUSE_HOST} ==="
}

start_flow_visibility
