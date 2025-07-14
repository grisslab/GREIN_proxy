#!/bin/bash

function check_error {
    STATUS_CODE=$1

    if [ $STATUS_CODE != 0 ]; then
        echo "Error: ${2}"
        exit 1
    fi
}

echo "+-----------------------------+"
echo "| GREIN Proxy - Image builder |"
echo "+-----------------------------+"
echo ""

echo -n "Tag: "
read IMAGE_TAG

# build the python image
cd ..
rm -rf dist
rm docker/*.whl
python3 -m build

check_error $? "Failed to build python package"

# copy the wheel package
cp dist/*.whl docker/
check_error $? "Failed to copy wheel file"

# add the test dataset
cp tests/grein_proxy.db docker/grein.db
check_error $? "Failed to copy test database"

# build the docker file
cd docker
podman build -t quay.io/grisslab/grein_proxy:${IMAGE_TAG} .
check_error $? "Failed to build image."

echo -n "Push to quay.io? [y/N]: "
read ANSWER

if [ "$ANSWER" == "y" ];  then 
    podman push quay.io/grisslab/grein_proxy:${IMAGE_TAG}
fi