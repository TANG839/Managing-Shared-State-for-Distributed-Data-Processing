REGISTRY_NAME="cbdpgroup34"
docker build --no-cache -f Dockerfile.build -t temp-builder-3 .  
docker cp $(docker create temp-builder-3):/build/build/coordinator ./cmake-build-debug/
docker cp $(docker create temp-builder-3):/build/build/worker ./cmake-build-debug/
docker build -t cbdp_coordinator --target coordinator .
docker build -t cbdp_worker --target worker .
docker tag cbdp_coordinator_new "$REGISTRY_NAME.azurecr.io"/cbdp_coordinator
docker push "$REGISTRY_NAME.azurecr.io"/cbdp_coordinator
docker tag cbdp_worker "$REGISTRY_NAME.azurecr.io"/cbdp_worker
docker push "$REGISTRY_NAME.azurecr.io"/cbdp_worker