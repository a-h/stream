build:
	cd api && go build ./...
	cd cdk && go build

build-lambda:
	go run build-lambda.go

deploy: build-lambda
	cd cdk && cdk deploy
