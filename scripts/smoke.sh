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
    echo ""
}


smoke_airflow() {
    echo ""
}


# Main case logic
case ${1:-all} in
	hdfs)  smoke_hdfs  ;;
	spark)  smoke_spark  ;;
    kafka)  smoke_kafka  ;;
    airflow)  smoke_airflow  ;;
    all)  smoke_hdfs; smoke_spark; smoke_kafka; smoke_airflow
	*)  echo "Usage: $0 [hdfs|spark|kafka|airflow|all]"; exit 2 ;;
esac