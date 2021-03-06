.PHONY: all help build clean test run
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

try:
	from urllib import pathname2url
except:
	from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

build:      ## build nifi_ext docker image
	docker build -t nifi_ext .


clean:      ## stop and delete nifi_ext container
	docker stop nifi_ext || true
	docker rm nifi_ext || true

run: build clean      ## launch nifi container
	mkdir -p app
	chmod -R +x $(shell pwd)/src
	docker run -d --name nifi_ext \
	 -v $(shell pwd)/app:/app \
	 -v $(shell pwd)/src:/src \
	-p 8083:8080 \
	-p 8001:8001 \
	-p 8002:8002 nifi_ext:latest

