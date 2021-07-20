#!/bin/bash

source /scripts/common.sh
source /scripts/bootstrap-helm.sh


run_tests() {
    echo Running tests...

    wait_pod_ready polkadot-account-monitoring
}

teardown() {
    helm delete polkadot-account-monitoring
}

main(){
    if [ -z "$KEEP_POLKADOT_MONITOR" ]; then
        trap teardown EXIT
    fi

    /scripts/build-helmfile.sh

    run_tests
}

main