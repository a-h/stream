#!/bin/sh
mkdir -p ./dynamodb/data
if ! [ -f ./dynamodb/DynamoDBLocal.jar ]; then
  curl https://s3.eu-central-1.amazonaws.com/dynamodb-local-frankfurt/dynamodb_local_latest.zip -o ./dynamodb/d.zip
  unzip ./dynamodb/d.zip -d ./dynamodb
  rm ./dynamodb/d.zip
fi
if command -v nix &> /dev/null
then
    echo "using nix..."
    nix-shell --packages jdk11 --run "java -jar ./dynamodb/DynamoDBLocal.jar -sharedDb -dbPath ./dynamodb/data"
    exit
fi
if command -v java &> /dev/null
then
    echo "using java..."
    java -jar ./dynamodb/DynamoDBLocal.jar -sharedDb -dbPath ./dynamodb/data
    exit
fi
