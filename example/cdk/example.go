package main

import (
	"github.com/aws/aws-cdk-go/awscdk"
	"github.com/aws/aws-cdk-go/awscdk/awsapigateway"
	"github.com/aws/aws-cdk-go/awscdk/awsdynamodb"
	"github.com/aws/aws-cdk-go/awscdk/awslambda"
	"github.com/aws/aws-cdk-go/awscdk/awss3assets"
	"github.com/aws/constructs-go/constructs/v3"
	"github.com/aws/jsii-runtime-go"
)

type ExampleStackProps struct {
	awscdk.StackProps
}

func NewExampleStack(scope constructs.Construct, id string, props *ExampleStackProps) awscdk.Stack {
	var sprops awscdk.StackProps
	if props != nil {
		sprops = props.StackProps
	}
	stack := awscdk.NewStack(scope, &id, &sprops)

	// notFound.
	notFound := awslambda.NewFunction(stack, jsii.String("notFoundHandler"), &awslambda.FunctionProps{
		Runtime: awslambda.Runtime_GO_1_X(),
		Handler: jsii.String("lambdaHandler"),
		Code:    awslambda.Code_FromAsset(jsii.String("../api/notfound"), &awss3assets.AssetOptions{}),
	})

	// DynamoDB.
	slotMachineTable := awsdynamodb.NewTable(stack, jsii.String("slotMachineTable"), &awsdynamodb.TableProps{
		TableName:    jsii.String("slotMachine"),
		PartitionKey: &awsdynamodb.Attribute{Name: jsii.String("_pk"), Type: awsdynamodb.AttributeType_STRING},
		SortKey:      &awsdynamodb.Attribute{Name: jsii.String("_sk"), Type: awsdynamodb.AttributeType_STRING},
		BillingMode:  awsdynamodb.BillingMode_PAY_PER_REQUEST,
	})

	// POST /machine/id/insertCoin handler.
	insertCoinPost := awslambda.NewFunction(stack, jsii.String("insertCoinHandler"), &awslambda.FunctionProps{
		Runtime: awslambda.Runtime_GO_1_X(),
		Handler: jsii.String("lambdaHandler"),
		Code:    awslambda.Code_FromAsset(jsii.String("../api/machine/insertCoin"), &awss3assets.AssetOptions{}),
		Environment: &map[string]*string{
			"MACHINE_TABLE": slotMachineTable.TableName(),
		},
	})
	slotMachineTable.GrantReadWriteData(insertCoinPost)

	// POST /machine/id/pullHandle handler.
	pullHandlePost := awslambda.NewFunction(stack, jsii.String("pullHandleHandler"), &awslambda.FunctionProps{
		Runtime: awslambda.Runtime_GO_1_X(),
		Handler: jsii.String("lambdaHandler"),
		Code:    awslambda.Code_FromAsset(jsii.String("../api/machine/pullHandle"), &awss3assets.AssetOptions{}),
		Environment: &map[string]*string{
			"MACHINE_TABLE": slotMachineTable.TableName(),
		},
	})
	slotMachineTable.GrantReadWriteData(pullHandlePost)
	// GET /machine/id handler.
	machineGet := awslambda.NewFunction(stack, jsii.String("machineGetHandler"), &awslambda.FunctionProps{
		Runtime: awslambda.Runtime_GO_1_X(),
		Handler: jsii.String("lambdaHandler"),
		Code:    awslambda.Code_FromAsset(jsii.String("../api/machine/get"), &awss3assets.AssetOptions{}),
		Environment: &map[string]*string{
			"MACHINE_TABLE": slotMachineTable.TableName(),
		},
	})
	slotMachineTable.GrantReadData(machineGet)

	// Create API Gateway.
	api := awsapigateway.NewLambdaRestApi(stack, jsii.String("slotmachine-api"), &awsapigateway.LambdaRestApiProps{
		Handler: notFound,
		Proxy:   jsii.Bool(false),
	})
	apiResourceOpts := &awsapigateway.ResourceOptions{}
	apiLambdaOpts := &awsapigateway.LambdaIntegrationOptions{}

	// /machine/{id}
	machine := api.Root().AddResource(jsii.String("machine"), apiResourceOpts).AddResource(jsii.String("{id}"), apiResourceOpts)
	// GET
	machineGetIntegration := awsapigateway.NewLambdaIntegration(machineGet, apiLambdaOpts)
	machine.AddMethod(jsii.String("GET"), machineGetIntegration, &awsapigateway.MethodOptions{})

	// /machine/{id}/insertCoin
	insertCoin := machine.AddResource(jsii.String("insertCoin"), apiResourceOpts)
	// POST
	insertCoinPostIntegration := awsapigateway.NewLambdaIntegration(insertCoinPost, apiLambdaOpts)
	insertCoin.AddMethod(jsii.String("POST"), insertCoinPostIntegration, &awsapigateway.MethodOptions{})

	// /machine/{id}/pullHandle
	pullHandle := machine.AddResource(jsii.String("pullHandle"), apiResourceOpts)
	// POST
	pullHandlePostIntegration := awsapigateway.NewLambdaIntegration(pullHandlePost, apiLambdaOpts)
	pullHandle.AddMethod(jsii.String("POST"), pullHandlePostIntegration, &awsapigateway.MethodOptions{})

	return stack
}

func main() {
	app := awscdk.NewApp(nil)

	NewExampleStack(app, "stream-example", &ExampleStackProps{
		awscdk.StackProps{
			Env: env(),
		},
	})

	app.Synth(nil)
}

// env determines the AWS environment (account+region) in which our stack is to
// be deployed. For more information see: https://docs.aws.amazon.com/cdk/latest/guide/environments.html
func env() *awscdk.Environment {
	// If unspecified, this stack will be "environment-agnostic".
	// Account/Region-dependent features and context lookups will not work, but a
	// single synthesized template can be deployed anywhere.
	//---------------------------------------------------------------------------
	return nil

	// Uncomment if you know exactly what account and region you want to deploy
	// the stack to. This is the recommendation for production stacks.
	//---------------------------------------------------------------------------
	// return &awscdk.Environment{
	//  Account: jsii.String("123456789012"),
	//  Region:  jsii.String("us-east-1"),
	// }

	// Uncomment to specialize this stack for the AWS Account and Region that are
	// implied by the current CLI configuration. This is recommended for dev
	// stacks.
	//---------------------------------------------------------------------------
	// return &awscdk.Environment{
	//  Account: jsii.String(os.Getenv("CDK_DEFAULT_ACCOUNT")),
	//  Region:  jsii.String(os.Getenv("CDK_DEFAULT_REGION")),
	// }
}
