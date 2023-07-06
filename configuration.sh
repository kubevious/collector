export SERVER_PORT=4003
export CONTAINER_NAME=kubevious-collector
export NETWORK_NAME=kubevious
export IMAGE_NAME=kubevious-collector
export IMAGE_NAME_UBI=${IMAGE_NAME}-ubi

export BACKEND_BASE_URL=http://localhost:4002

# export COLLECTOR_HISTORY_RETENTION_DAYS=5


source ../dependencies.git/runtime-configuration.sh
source ../dependencies.git/worldvious/configuration.sh