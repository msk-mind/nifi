.PHONY: all help build clean test run


help:
	@echo 'Usage: make <target>'
	@echo
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'


build:      ## build nifi_ext docker image
	docker build -t nifi_ext .


clean:      ## stop and delete nifi_ext container
	docker stop nifi_ext || true
	docker rm nifi_ext || true


clean-pyc:      ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +


clean-test:      ## remove test and coverage artifacts
	rm -fr .pytest_cache


test: clean-test clean-pyc      ## run tests with the default Python
	pytest test


run: build clean      ## launch nifi container
	mkdir -p app
	chmod -R +x $(shell pwd)/src
	docker run -d --name nifi_ext \
	 -v $(shell pwd)/app:/app \
	 -v $(shell pwd)/src:/src \
	-p 8083:8080 \
	-p 8001:8001 \
	-p 8002:8002 nifi_ext:latest

