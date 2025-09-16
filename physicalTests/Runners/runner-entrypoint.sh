#!/bin/sh
set -e

wait_for_service() {
  url="$1"; name="$2"; max_attempts=${3:-30}
  i=0
  while [ $i -lt $max_attempts ]; do
    i=$((i+1))
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "$name is ready"
      return 0
    fi
    sleep 2
  done
  echo "$name failed to start" >&2
  return 1
}

# Basic healthchecks
wait_for_service "http://schema-registry:8081/subjects" "Schema Registry" 60
wait_for_service "http://ksqldb-server:8088/healthcheck" "ksqlDB Server" 90

# ksqlDB internal state (3 consecutive OK)
consec=0
i=0
while [ $i -lt 60 ]; do
  i=$((i+1))
  resp=$(curl -fsS -H 'Content-Type: application/vnd.ksql+json' \
         -d '{"ksql":"SHOW QUERIES;"}' \
         http://ksqldb-server:8088/ksql 2>/dev/null || echo "")
  if [ -n "$resp" ] && \
     ! echo "$resp" | grep -qi "error\|exception" && \
     echo "$resp" | grep -Eq '^[[:space:]]*\['; then
    consec=$((consec+1))
    [ $consec -ge 3 ] && break
  else
    consec=0
  fi
  sleep 2
done

# Minimal stabilization
sleep 30
echo "ksqlDB is stable and ready for tests"

# Keep the current default test selection
dotnet test -c Release /src/physicalTests/Kafka.Ksql.Linq.Tests.Integration.csproj \
  --filter FullyQualifiedName~TimeBucketImportTumblingTests.Import_Ticks_Define_Tumbling_Query_Then_Extract_Bars_Via_TimeBucket \
  --logger 'trx;LogFileName=physical_runner.trx' \
  --results-directory /src/reports/physical

