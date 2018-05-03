#!/usr/bin/env bash

# more bash-friendly output for jq
JQ="jq --raw-output --exit-status"

configure_aws_cli(){
	aws --version
	aws configure set default.region us-east-1
	aws configure set default.output json
}

push_ecr_image(){
	eval $(aws ecr get-login --region us-east-1 --no-include-email)
	docker push 000.dkr.ecr.us-east-1.amazonaws.com/load-to-redshift:latest
}

configure_aws_cli
push_ecr_image
