## build nifi_lib docker image
build:
	docker build -t nifi_lib .


## run nifi lib container and copy libs to host system
lib:
	docker run -d --name nifi_lib nifi_lib:latest
	docker cp nifi_lib:/nifi-dicom/build/libs/nifi-dicom-1.4.nar .
	docker stop nifi_lib
	docker rm nifi_lib