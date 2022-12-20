## Example

## Tasks

### deploy

Directory: cdk

```
cdk deploy
```

## Usage

Once you've deployed the stack, you can test it by:

Creating a machine:

```
curl -data="{}" https://2m3j3gok5k.execute-api.eu-west-2.amazonaws.com/prod/machine/1
```

Inserting a coin:

```
curl -data="{}" https://2m3j3gok5k.execute-api.eu-west-2.amazonaws.com/prod/machine/1/insertCoin
```

Pulling the handle:

```
curl -data="{}" https://2m3j3gok5k.execute-api.eu-west-2.amazonaws.com/prod/machine/1/pullHandle
```

When you've pulled the handle, the EventBridge message will be seen, since the stream handler will process it.
