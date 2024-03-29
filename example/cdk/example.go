package main

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsapigateway"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsdynamodb"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsevents"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslogs"

	"github.com/aws/aws-cdk-go/awscdk/v2/awslambdaeventsources"
	awslambdago "github.com/aws/aws-cdk-go/awscdklambdagoalpha/v2"
	"github.com/aws/constructs-go/constructs/v10"
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

	// Go build options.
	bundlingOptions := &awslambdago.BundlingOptions{
		GoBuildFlags: &[]*string{jsii.String(`-ldflags "-s -w"`)},
	}
	// notFound.
	notFound := awslambdago.NewGoFunction(stack, jsii.String("notFoundHandler"), &awslambdago.GoFunctionProps{
		Runtime:      awslambda.Runtime_PROVIDED_AL2(),
		Architecture: awslambda.Architecture_ARM_64(),
		Entry:        jsii.String("../api/notfound"),
		Bundling:     bundlingOptions,
		Tracing:      awslambda.Tracing_ACTIVE,
		Timeout:      awscdk.Duration_Seconds(jsii.Number(30)),
		LogRetention: awslogs.RetentionDays_ONE_YEAR,
	})

	// DynamoDB.
	slotMachineTable := awsdynamodb.NewTable(stack, jsii.String("slotMachineTable"), &awsdynamodb.TableProps{
		TableName:    jsii.String("slotMachine"),
		PartitionKey: &awsdynamodb.Attribute{Name: jsii.String("_pk"), Type: awsdynamodb.AttributeType_STRING},
		SortKey:      &awsdynamodb.Attribute{Name: jsii.String("_sk"), Type: awsdynamodb.AttributeType_STRING},
		BillingMode:  awsdynamodb.BillingMode_PAY_PER_REQUEST,
		Stream:       awsdynamodb.StreamViewType_NEW_IMAGE,
	})

	// Create an event bus.
	eventBus := awsevents.NewEventBus(stack, jsii.String("slotMachineEventBus"), &awsevents.EventBusProps{
		EventBusName: jsii.String("slotMachineEventBus"),
	})

	// Process streams.
	streamHandler := awslambdago.NewGoFunction(stack, jsii.String("streamHandler"), &awslambdago.GoFunctionProps{
		Runtime:      awslambda.Runtime_PROVIDED_AL2(),
		Architecture: awslambda.Architecture_ARM_64(),
		Entry:        jsii.String("../api/streamhandler/"),
		Bundling:     bundlingOptions,
		Tracing:      awslambda.Tracing_ACTIVE,
		Environment: &map[string]*string{
			"EVENT_BUS_NAME":    eventBus.EventBusName(),
			"EVENT_SOURCE_NAME": jsii.String("slot-machine"),
		},
		Timeout:      awscdk.Duration_Minutes(jsii.Number(15)),
		LogRetention: awslogs.RetentionDays_ONE_YEAR,
	})
	slotMachineTable.GrantReadData(streamHandler)
	eventBus.GrantPutEventsTo(streamHandler)

	filters := []*map[string]any{
		awslambda.FilterCriteria_Filter(&map[string]any{
			"eventName": awslambda.FilterRule_IsEqual("INSERT"),
			"dynamodb": &map[string]any{
				"NewImage": &map[string]any{
					"_sk": &map[string]any{
						"S": awslambda.FilterRule_BeginsWith(jsii.String("OUTBOUND/")),
					},
				},
			},
		}),
	}
	streamHandler.AddEventSource(awslambdaeventsources.NewDynamoEventSource(slotMachineTable, &awslambdaeventsources.DynamoEventSourceProps{
		StartingPosition: awslambda.StartingPosition_LATEST,
		Enabled:          jsii.Bool(true),
		Filters:          &filters,
	}))

	// POST /machine/id/insertCoin handler.
	insertCoinPost := awslambdago.NewGoFunction(stack, jsii.String("insertCoinHandler"), &awslambdago.GoFunctionProps{
		Runtime:      awslambda.Runtime_PROVIDED_AL2(),
		Architecture: awslambda.Architecture_ARM_64(),
		Entry:        jsii.String("../api/machine/insertcoin/post"),
		Bundling:     bundlingOptions,
		Tracing:      awslambda.Tracing_ACTIVE,
		Environment: &map[string]*string{
			"MACHINE_TABLE": slotMachineTable.TableName(),
		},
		Timeout:      awscdk.Duration_Seconds(jsii.Number(30)),
		LogRetention: awslogs.RetentionDays_ONE_YEAR,
	})
	slotMachineTable.GrantReadWriteData(insertCoinPost)

	// POST /machine/id/pullHandle handler.
	pullHandlePost := awslambdago.NewGoFunction(stack, jsii.String("pullHandleHandler"), &awslambdago.GoFunctionProps{
		Runtime:      awslambda.Runtime_PROVIDED_AL2(),
		Architecture: awslambda.Architecture_ARM_64(),
		Entry:        jsii.String("../api/machine/pullhandle/post"),
		Bundling:     bundlingOptions,
		Tracing:      awslambda.Tracing_ACTIVE,
		Environment: &map[string]*string{
			"MACHINE_TABLE": slotMachineTable.TableName(),
		},
		Timeout:      awscdk.Duration_Seconds(jsii.Number(30)),
		LogRetention: awslogs.RetentionDays_ONE_YEAR,
	})
	slotMachineTable.GrantReadWriteData(pullHandlePost)
	// POST /machine/id handler.
	machinePost := awslambdago.NewGoFunction(stack, jsii.String("machinePostHandler"), &awslambdago.GoFunctionProps{
		Runtime:      awslambda.Runtime_PROVIDED_AL2(),
		Architecture: awslambda.Architecture_ARM_64(),
		Entry:        jsii.String("../api/machine/post"),
		Bundling:     bundlingOptions,
		Tracing:      awslambda.Tracing_ACTIVE,
		Environment: &map[string]*string{
			"MACHINE_TABLE": slotMachineTable.TableName(),
		},
		Timeout:      awscdk.Duration_Seconds(jsii.Number(30)),
		LogRetention: awslogs.RetentionDays_ONE_YEAR,
	})
	slotMachineTable.GrantReadWriteData(machinePost)
	// GET /machine/id handler.
	machineGet := awslambdago.NewGoFunction(stack, jsii.String("machineGetHandler"), &awslambdago.GoFunctionProps{
		Runtime:      awslambda.Runtime_PROVIDED_AL2(),
		Architecture: awslambda.Architecture_ARM_64(),
		Entry:        jsii.String("../api/machine/get"),
		Bundling:     bundlingOptions,
		Tracing:      awslambda.Tracing_ACTIVE,
		Environment: &map[string]*string{
			"MACHINE_TABLE": slotMachineTable.TableName(),
		},
		Timeout:      awscdk.Duration_Seconds(jsii.Number(30)),
		LogRetention: awslogs.RetentionDays_ONE_YEAR,
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
	// POST
	machinePostIntegration := awsapigateway.NewLambdaIntegration(machinePost, apiLambdaOpts)
	machine.AddMethod(jsii.String("POST"), machinePostIntegration, &awsapigateway.MethodOptions{})
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
