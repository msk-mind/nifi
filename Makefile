## build nifi_ext docker image
build:
	docker build -t nifi_ext .

## run nifi container
run:
	mkdir -p app
	docker run --name nifi_ext -v $(shell pwd)/app:/app -p 8080:8080 nifi_ext:latest

## clean nifi container
make clean:
	docker stop nifi_ext
	docker rm nifi_ext

