#!/bin/bash

set -e
set -o pipefail
set -u

BUILD_VERSION="$(git rev-list --count master)-$(git rev-parse --short=7 HEAD)"
OPTIONS=""

if [ $# -eq 5 ]; then
    signing_secret_key_ring_file="$1"
    signing_key_id="$2"
    signing_key_password="$3"
    publish_user="$4"
    publish_password="$5"

    OPTIONS="publish -Psigning.secretKeyRingFile=${signing_secret_key_ring_file} -Psigning.keyId=${signing_key_id} -Psigning.password=${signing_key_password} -PpublishUser=${publish_user} -PpublishPassword=${publish_password}"
fi

./gradlew lintKotlin build --warning-mode=all -PbuildVersion=$BUILD_VERSION $OPTIONS
