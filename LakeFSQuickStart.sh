docker run --name lakefs --pull always \
             --rm --publish 8001:8000 \
             treeverse/lakefs:latest \
             run --quickstart

#docker exec -it lakefs lakectl config
