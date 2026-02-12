export ORG="memalhot"
export IMAGE_NAME="nb-culler"
export TAG="latest"

docker build -t quay.io/$ORG/$IMAGE_NAME:$TAG .
docker push quay.io/$ORG/$IMAGE_NAME:$TAG
