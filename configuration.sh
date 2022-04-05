export SERVER_PORT=4003
export CONTAINER_NAME=kubevious-collector
export NETWORK_NAME=kubevious
export IMAGE_NAME=kubevious-collector

export BACKEND_BASE_URL=http://localhost:4002

source ../dependencies.git/mysql/runtime-configuration.sh
source ../dependencies.git/redisearch/runtime-configuration.sh
source ../dependencies.git/worldvious/configuration.sh