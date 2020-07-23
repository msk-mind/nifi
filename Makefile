## build nifi_ext docker image
build:
	docker build -t nifi_ext .


## clean nifi container
make clean:
	docker stop nifi_ext || true
	docker rm nifi_ext || true

## run nifi container
run: build clean
	mkdir -p app
	docker run -d --name nifi_ext \
	 -v $(shell pwd)/app:/app \
	-p 8083:8080 \
	-p 8082:8082 \
	-p 8888:8888 nifi_ext:latest


