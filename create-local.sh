#!/usr/bin/env bash

sudo fdisk /dev/sdc
n
p
1

+8GB
w
sudo mkfs -t ext4 /dev/sdc1
sudo mkdir -p /data/clickhouse
sudo mount -t ext4 /dev/sdc1 /data/clickhouse

./