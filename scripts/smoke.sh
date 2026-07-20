#!/bin/bash
# Smoke tests for the infra stacks

set -euo pipefail

COMPOSE="docker compose"

c_green() { printf "\033[0;32m%s\033[0m\n" "$*"; }
c_red()   { printf "\033[0;31m%s\033[0m\n" "$*" 1>&2; }
c_blue()  { printf "\033[0;34m%s\033[0m\n" "$*"; }

step() { c_blue ""; c_blue "==> $*"; }
ok()   { c_green "    OK: $*"; }
die()  { c_red   "    FAIL: $*"; exit 1; }


# SMOKE TEST FUNCTIONS

smoke_hdfs() {
    step "Smoke tests for HDFS: put / ls / cat / rm"

    $COMPOSE exec -T namenode bash -c '
      set -e
      echo "hello" > /tmp/smoke.txt
      hdfs dfs -mkdir -p /smoke
      hdfs dfs -put -f /tmp/smoke.txt /smoke/smoke.txt
      hdfs dfs -ls /smoke
      out=$(hdfs dfs -cat /smoke/smoke.txt)
      test "$out" = "hello" || { echo "Output: $out"; exit 1; }
      hdfs dfs -rm -r -skipTrash /smoke
    ' > /dev/null || die "HDFS smoke tests failed"
    ok "HDFS smoke tests passed"
}


smoke_spark() {
    step "Smoke tests for Spark: submit a job that reads from HDFS"

    $COMPOSE exec -T namenode bash -c '
      set -e
      printf "alpha bravo charlie\nalpha bravo\nalpha\n" > /tmp/words.txt
      hdfs dfs -mkdir -p /smoke
      hdfs dfs -put -f /tmp/words.txt /smoke/words.txt
    ' > /dev/null || die "Could not seed HDFS input"

    $COMPOSE exec -T spark-master bash -c '
      /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
          /opt/spark/work-dir/jobs/smoke/smoke_spark.py
    ' | tee /tmp/finpulse-smoke-spark.log > /dev/null || die "spark-submit failed (see /tmp/finpulse-smoke-spark.log)"

    grep -q "Total Words: 6. Distinct Words: 3" /tmp/finpulse-smoke-spark.log \
        || die "Spark job ran but produced wrong counts (see /tmp/finpulse-smoke-spark.log)"

    $COMPOSE exect -T namenode hdfs dfs -rm -r -skipTrash /smoke > /dev/null || true
    ok "HDFS + Spark integration works"
}


smoke_kafka() {
    step "Smoke tests for Kafka: create topic + produce + consume"

    TOPIC="smoke-$(date +%s)"
    $COMPOSE exec -T kafka bash -c "
        set -e
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 \
            --create --if-not-exists --topic ${TOPIC} --partitions 1 --replication-factor 1 >/dev/null
        
        for i in 1 2 3 4 5; do echo \"msg-\$i\"; done | \
            /opt/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server kafka:9094 --topic ${TOPIC} >/dev/null

        out=\$(/opt/kafka/bin/kafka-console-consumer.sh \
            --bootstrap-server kafka:9094 --topic ${TOPIC} \
            --from-beginning --max-messages 5 --timeout-ms 10000 2>/dev/null | wc -l | tr -d ' ')
        
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 \
            --delete --topic ${TOPIC} >/dev/null
        
        test \"\$out\" = \"5\" || { echo \"expected 5 messages, got \$out\"; exit 1; }
    " || die "Kafka produce/consume failed"
    ok "Kafka produce/consume works"
}


smoke_airflow() {
    echo ""
}

smoke_pinot() {
    step "Smoke tests for Pinot: controller + broker healthy, cluster registration"

    $COMPOSE exec -T pinot-controller bash -c "
        set -e
        curl -fsS http://pinot-controller:9000/health | grep -q OK
        curl -fsS http://pinot-broker:8099/health | grep -q OK
    " >/dev/null || die "Pinot controller + broker are not healthy"
    ok "Pinot controller + broker healthy"

    out=$($COMPOSE exec -T pinot-controller \
        curl -fsS http://pinot-controller:9000/instances 2>/dev/null)
    echo "$out" | grep -q "Broker_pinot-broker_8099" || die "Broker not registered: $out"
    echo "$out" | grep -q "Server_pinot-server_8098" || die "Server not registered: $out"
    ok "Pinot broker + server registered with controller"
}

smoke_trino() {
    step "Smoke tests for Trino: /v1/info + hive catalog + Spark<->HMS<->Trino round-trip"

    # 1. Whether Trino coordinator is ready
    $COMPOSE exec -T trino-coordinator bash -c '
      curl -fsS http://localhost:8080/v1/info | grep -q "\"starting\":false"
    ' >/dev/null || die "Trino coordinator is not ready"
    ok "Trino coordinator is ready"

    # 2. Whether the Hive catalog is missing
    out=$($COMPOSE exec -T trino-coordinator trino \
      --server localhost:8080 --execute 'SHOW CATALOGS' 2>/dev/null)
    echo "$out" | grep -q '^"hive"$' || die "Hive catalog is missing from Trino: $out"
    ok "Hive catalog registered"

    # 3. Spark<->HMS<->Trino round trip
    $COMPOSE exec -T spark-master bash -c '
      /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/work-dir/jobs/smoke/smoke_trino.py
    ' | tee /tmp/finpulse-smoke-trino.log >/dev/null \
        || die "Spark job smoke_trino.py failed (see /tmp/finpulse-smoke-trino.log)"
    grep -q 'smoke_trino OK' /tmp/finpulse-smoke-trino.log \
        || die "smoke_trino.py ran but did not log 'smoke_trino OK'"

    count=$($COMPOSE exec -T trino-coordinator trino \
      --catalog hive --schema default \
      --execute 'SELECT COUNT(*) FROM smoke_hms' 2>/dev/null \
      | tr -d '"' | tail -1)
    test "${count}" = "3" || die "Trino saw smoke_hms with $count rows, expected 3"
    ok "Spark<->HMS<->Presto round-trip works"
}

# Main case logic
case ${1:-all} in
	hdfs)  smoke_hdfs  ;;
	spark)  smoke_spark  ;;
    kafka)  smoke_kafka  ;;
    airflow)  smoke_airflow  ;;
    pinot)  smoke_pinot  ;;
    trino)  smoke_trino  ;;
    all)  smoke_hdfs; smoke_spark; smoke_kafka; smoke_airflow; smoke_pinot; smoke_trino  ;;
	*)  echo "Usage: $0 [hdfs|spark|kafka|airflow|all]"; exit 2 ;;
esac