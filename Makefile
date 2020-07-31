.PHONY: all test clean

## build nifi_ext docker image
build:
	docker build -t nifi_ext .


## clean nifi container
clean:
	docker stop nifi_ext || true
	docker rm nifi_ext || true

## run tests quickly with the default Python
test:
	pytest test

## run nifi container
run: build clean
	mkdir -p app
	chmod -R +x $(shell pwd)/src
	docker run -d --name nifi_ext \
	 -v $(shell pwd)/app:/app \
	 -v $(shell pwd)/src:/src \
	-p 8083:8080 \
	-p 8001:8001 \
	-p 8002:8002 nifi_ext:latest

