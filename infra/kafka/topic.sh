#!/bin/bash
kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic makfleet.telemetry.raw --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic makfleet.telemetry.enriched --partitions 1 --replication-factor 1