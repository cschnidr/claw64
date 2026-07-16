package llm

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
)

// BedrockClient talks to AWS Bedrock via the Converse API.
type BedrockClient struct {
	Model   string
	Region  string
	Profile string

	client *bedrockruntime.Client
}

// NewBedrock creates a Bedrock client. Call Init before Complete.
func NewBedrock(region, profile, model string) *BedrockClient {
	if model == "" {
		model = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
	}
	return &BedrockClient{
		Model:   model,
		Region:  region,
		Profile: profile,
	}
}

// Init loads the AWS config and creates the underlying SDK client.
func (c *BedrockClient) Init(ctx context.Context) error {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(c.Region),
	}
	if c.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(c.Profile))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	c.client = bedrockruntime.NewFromConfig(cfg)
	return nil
}

// DescribeRequest returns the Converse API endpoint and request body.
func (c *BedrockClient) DescribeRequest(messages []Message, tools []Tool) (string, []byte, error) {
	input, err := c.buildInput(messages, tools)
	if err != nil {
		return "", nil, err
	}

	body, err := json.MarshalIndent(input, "", "  ")
	if err != nil {
		return "", nil, err
	}

	url := fmt.Sprintf("https://bedrock-runtime.%s.amazonaws.com/model/%s/converse", c.Region, c.Model)
	return url, body, nil
}

// Complete sends the conversation to Bedrock and returns the assistant reply.
func (c *BedrockClient) Complete(ctx context.Context, messages []Message, tools []Tool) (Message, error) {
	if c.client == nil {
		if err := c.Init(ctx); err != nil {
			return Message{}, err
		}
	}

	input, err := c.buildInput(messages, tools)
	if err != nil {
		return Message{}, fmt.Errorf("build input: %w", err)
	}

	output, err := c.client.Converse(ctx, input)
	if err != nil {
		return Message{}, fmt.Errorf("converse: %w", err)
	}

	return c.parseOutput(output)
}

// buildInput constructs the ConverseInput from our unified message types.
func (c *BedrockClient) buildInput(messages []Message, tools []Tool) (*bedrockruntime.ConverseInput, error) {
	var system []types.SystemContentBlock
	var convMsgs []types.Message

	for _, m := range messages {
		switch {
		case m.Role == "system":
			system = append(system, &types.SystemContentBlockMemberText{Value: m.Content})

		case m.Role == "assistant" && len(m.ToolCalls) > 0:
			var blocks []types.ContentBlock
			if m.Content != "" {
				blocks = append(blocks, &types.ContentBlockMemberText{Value: m.Content})
			}
			for _, tc := range m.ToolCalls {
				input, err := jsonToBedrockDoc(tc.Function.Arguments)
				if err != nil {
					return nil, fmt.Errorf("tool call input: %w", err)
				}
				blocks = append(blocks, &types.ContentBlockMemberToolUse{
					Value: types.ToolUseBlock{
						ToolUseId: aws.String(tc.ID),
						Name:      aws.String(tc.Function.Name),
						Input:     input,
					},
				})
			}
			convMsgs = append(convMsgs, types.Message{
				Role:    types.ConversationRoleAssistant,
				Content: blocks,
			})

		case m.Role == "tool":
			// Tool results are sent as user messages with ToolResult content blocks.
			block := &types.ContentBlockMemberToolResult{
				Value: types.ToolResultBlock{
					ToolUseId: aws.String(m.ToolCallID),
					Content: []types.ToolResultContentBlock{
						&types.ToolResultContentBlockMemberText{Value: m.Content},
					},
				},
			}
			convMsgs = append(convMsgs, types.Message{
				Role:    types.ConversationRoleUser,
				Content: []types.ContentBlock{block},
			})

		case m.Role == "assistant":
			convMsgs = append(convMsgs, types.Message{
				Role:    types.ConversationRoleAssistant,
				Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: m.Content}},
			})

		default:
			// user or any other role
			convMsgs = append(convMsgs, types.Message{
				Role:    types.ConversationRoleUser,
				Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: m.Content}},
			})
		}
	}

	input := &bedrockruntime.ConverseInput{
		ModelId:  aws.String(c.Model),
		Messages: convMsgs,
	}
	if len(system) > 0 {
		input.System = system
	}

	// convert tools to Bedrock format
	if len(tools) > 0 {
		var bedrockTools []types.Tool
		for _, t := range tools {
			schema := toolParamsToBedrockDoc(t.Function.Parameters)
			bedrockTools = append(bedrockTools, &types.ToolMemberToolSpec{
				Value: types.ToolSpecification{
					Name:        aws.String(t.Function.Name),
					Description: aws.String(t.Function.Description),
					InputSchema: &types.ToolInputSchemaMemberJson{Value: schema},
				},
			})
		}
		input.ToolConfig = &types.ToolConfiguration{Tools: bedrockTools}
	}

	return input, nil
}

// parseOutput converts the Bedrock ConverseOutput to a unified Message.
func (c *BedrockClient) parseOutput(output *bedrockruntime.ConverseOutput) (Message, error) {
	responseMsg, ok := output.Output.(*types.ConverseOutputMemberMessage)
	if !ok {
		return Message{}, fmt.Errorf("unexpected output type: %T", output.Output)
	}

	msg := Message{Role: "assistant"}
	for _, block := range responseMsg.Value.Content {
		switch v := block.(type) {
		case *types.ContentBlockMemberText:
			if msg.Content != "" {
				msg.Content += "\n"
			}
			msg.Content += v.Value

		case *types.ContentBlockMemberToolUse:
			args, err := bedrockDocToJSON(v.Value.Input)
			if err != nil {
				return Message{}, fmt.Errorf("tool use input: %w", err)
			}
			msg.ToolCalls = append(msg.ToolCalls, ToolCall{
				ID:   aws.ToString(v.Value.ToolUseId),
				Type: "function",
				Function: FunctionCall{
					Name:      aws.ToString(v.Value.Name),
					Arguments: args,
				},
			})
		}
	}
	return msg, nil
}

// jsonToBedrockDoc converts a JSON string to a Bedrock document.Interface.
func jsonToBedrockDoc(jsonStr string) (document.Interface, error) {
	if jsonStr == "" || jsonStr == "{}" {
		return document.NewLazyDocument(map[string]interface{}{}), nil
	}
	var raw interface{}
	if err := json.Unmarshal([]byte(jsonStr), &raw); err != nil {
		return nil, err
	}
	return document.NewLazyDocument(raw), nil
}

// bedrockDocToJSON converts a Bedrock document.Interface to a JSON string.
func bedrockDocToJSON(doc document.Interface) (string, error) {
	if doc == nil {
		return "{}", nil
	}
	var raw interface{}
	if err := doc.UnmarshalSmithyDocument(&raw); err != nil {
		return "", err
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// toolParamsToBedrockDoc converts our Parameters struct to a Bedrock document.
func toolParamsToBedrockDoc(params Parameters) document.Interface {
	// Build a JSON Schema object matching what Bedrock expects.
	schema := map[string]interface{}{
		"type": params.Type,
	}
	if len(params.Properties) > 0 {
		props := make(map[string]interface{})
		for name, prop := range params.Properties {
			props[name] = map[string]interface{}{
				"type":        prop.Type,
				"description": prop.Description,
			}
		}
		schema["properties"] = props
	}
	if len(params.Required) > 0 {
		req := make([]interface{}, len(params.Required))
		for i, r := range params.Required {
			req[i] = r
		}
		schema["required"] = req
	}
	return document.NewLazyDocument(schema)
}
