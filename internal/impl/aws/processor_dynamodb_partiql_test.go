// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aws

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type mockProcDynamoDB struct {
	dynamoDBAPI
	pbatchFn func(context.Context, *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error)
	pexecFn  func(context.Context, *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error)
}

func (m *mockProcDynamoDB) BatchExecuteStatement(ctx context.Context, params *dynamodb.BatchExecuteStatementInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchExecuteStatementOutput, error) {
	return m.pbatchFn(ctx, params)
}

func (m *mockProcDynamoDB) ExecuteStatement(ctx context.Context, params *dynamodb.ExecuteStatementInput, _ ...func(*dynamodb.Options)) (*dynamodb.ExecuteStatementOutput, error) {
	return m.pexecFn(ctx, params)
}

func assertBatchMatches(t *testing.T, exp service.MessageBatch, act []service.MessageBatch) {
	t.Helper()

	require.Len(t, act, 1)
	require.Len(t, act[0], len(exp))
	for i, m := range exp {
		expBytes, _ := m.AsBytes()
		actBytes, _ := act[0][i].AsBytes()
		assert.Equal(t, string(expBytes), string(actBytes))
	}
}

func TestDynamoDBPartiqlWrite(t *testing.T) {
	query := `INSERT INTO "FooTable" VALUE {'id':'?','content':'?'}`
	mapping, err := bloblang.Parse(`
root = []
root."-".S = json("id")
root."-".S = json("content")
`)
	require.NoError(t, err)

	var request []types.BatchStatementRequest
	client := &mockProcDynamoDB{
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error) {
			request = input.Statements
			return &dynamodb.BatchExecuteStatementOutput{}, nil
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping, false)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"content":"foo stuff","id":"foo"}`)),
		service.NewMessage([]byte(`{"content":"bar stuff","id":"bar"}`)),
	}

	resBatch, err := db.ProcessBatch(t.Context(), reqBatch)
	require.NoError(t, err)
	assertBatchMatches(t, reqBatch, resBatch)

	expected := []types.BatchStatementRequest{
		{
			Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
			Parameters: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "foo"},
				&types.AttributeValueMemberS{Value: "foo stuff"},
			},
		},
		{
			Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
			Parameters: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "bar"},
				&types.AttributeValueMemberS{Value: "bar stuff"},
			},
		},
	}

	assert.Equal(t, expected, request)
}

func TestDynamoDBPartiqlRead(t *testing.T) {
	query := `SELECT * FROM Orders WHERE OrderID = ?`
	mapping, err := bloblang.Parse(`
root = []
root."-".S = json("id")
`)
	require.NoError(t, err)

	var request []types.BatchStatementRequest
	client := &mockProcDynamoDB{
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (*dynamodb.BatchExecuteStatementOutput, error) {
			request = input.Statements
			return &dynamodb.BatchExecuteStatementOutput{
				Responses: []types.BatchStatementResponse{
					{
						Item: map[string]types.AttributeValue{
							"meow":  &types.AttributeValueMemberS{Value: "meow1"},
							"meow2": &types.AttributeValueMemberS{Value: "meow2"},
						},
					},
					{
						Item: map[string]types.AttributeValue{
							"meow":  &types.AttributeValueMemberS{Value: "meow1"},
							"meow2": &types.AttributeValueMemberS{Value: "meow2"},
						},
					},
				},
			}, nil
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping, false)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
		service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
	}
	expBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"meow":{"S":"meow1"},"meow2":{"S":"meow2"}}`)),
		service.NewMessage([]byte(`{"meow":{"S":"meow1"},"meow2":{"S":"meow2"}}`)),
	}

	resBatch, err := db.ProcessBatch(t.Context(), reqBatch)
	require.NoError(t, err)
	assertBatchMatches(t, expBatch, resBatch)

	err = resBatch[0][0].GetError()
	assert.NoError(t, err)

	err = resBatch[0][1].GetError()
	assert.NoError(t, err)

	expected := []types.BatchStatementRequest{
		{
			Statement: aws.String("SELECT * FROM Orders WHERE OrderID = ?"),
			Parameters: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "foo"},
			},
		},
		{
			Statement: aws.String("SELECT * FROM Orders WHERE OrderID = ?"),
			Parameters: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "bar"},
			},
		},
	}

	assert.Equal(t, expected, request)
}

func TestDynamoDBPartiqlSadToGoodBatch(t *testing.T) {
	t.Parallel()

	query := `INSERT INTO "FooTable" VALUE {'id':'?','content':'?'}`
	mapping, err := bloblang.Parse(`
root = []
root."-".S = json("id")
root."-".S = json("content")
`)
	require.NoError(t, err)

	var requests [][]types.BatchStatementRequest
	client := &mockProcDynamoDB{
		pbatchFn: func(_ context.Context, input *dynamodb.BatchExecuteStatementInput) (output *dynamodb.BatchExecuteStatementOutput, err error) {
			if len(requests) == 0 {
				output = &dynamodb.BatchExecuteStatementOutput{
					Responses: make([]types.BatchStatementResponse, len(input.Statements)),
				}
				for i, stmt := range input.Statements {
					res := types.BatchStatementResponse{}
					if stmt.Parameters[0].(*types.AttributeValueMemberS).Value == "bar" {
						res.Error = &types.BatchStatementError{
							Message: aws.String("it all went wrong"),
						}
					}
					output.Responses[i] = res
				}
			} else {
				output = &dynamodb.BatchExecuteStatementOutput{}
			}
			stmts := make([]types.BatchStatementRequest, len(input.Statements))
			copy(stmts, input.Statements)
			requests = append(requests, stmts)
			return
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping, false)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"content":"foo stuff","id":"foo"}`)),
		service.NewMessage([]byte(`{"content":"bar stuff","id":"bar"}`)),
		service.NewMessage([]byte(`{"content":"baz stuff","id":"baz"}`)),
	}

	resBatch, err := db.ProcessBatch(t.Context(), reqBatch)
	require.NoError(t, err)
	assertBatchMatches(t, reqBatch, resBatch)

	err = resBatch[0][1].GetError()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "it all went wrong")

	err = resBatch[0][0].GetError()
	require.NoError(t, err)

	err = resBatch[0][2].GetError()
	require.NoError(t, err)

	expected := [][]types.BatchStatementRequest{
		{
			{
				Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
				Parameters: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: "foo"},
					&types.AttributeValueMemberS{Value: "foo stuff"},
				},
			},
			{
				Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
				Parameters: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: "bar"},
					&types.AttributeValueMemberS{Value: "bar stuff"},
				},
			},
			{
				Statement: aws.String("INSERT INTO \"FooTable\" VALUE {'id':'?','content':'?'}"),
				Parameters: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: "baz"},
					&types.AttributeValueMemberS{Value: "baz stuff"},
				},
			},
		},
	}

	assert.Equal(t, expected, requests)
}

func TestDynamoDBPartiqlExecuteStatementRead(t *testing.T) {
	query := `SELECT * FROM Orders.OrdersByCustomer WHERE CustomerID = ?`
	mapping, err := bloblang.Parse(`
root = []
root."-".S = json("id")
`)
	require.NoError(t, err)

	var requests []*dynamodb.ExecuteStatementInput
	client := &mockProcDynamoDB{
		pexecFn: func(_ context.Context, input *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
			cp := *input
			requests = append(requests, &cp)
			return &dynamodb.ExecuteStatementOutput{
				Items: []map[string]types.AttributeValue{
					{"meow": &types.AttributeValueMemberS{Value: "meow1"}},
					{"meow": &types.AttributeValueMemberS{Value: "meow2"}},
				},
			}, nil
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping, true)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"cust1"}`)),
		service.NewMessage([]byte(`{"id":"cust2"}`)),
	}

	resBatch, err := db.ProcessBatch(t.Context(), reqBatch)
	require.NoError(t, err)
	require.Len(t, resBatch, 1)
	require.Len(t, resBatch[0], 2)

	structured, err := resBatch[0][0].AsStructured()
	require.NoError(t, err)
	items, ok := structured.([]any)
	require.True(t, ok, "expected result to be array")
	assert.Len(t, items, 2)

	assert.Len(t, requests, 2)
	assert.Equal(t, query, *requests[0].Statement)
	assert.Equal(t, query, *requests[1].Statement)
}

func TestDynamoDBPartiqlExecuteStatementError(t *testing.T) {
	t.Parallel()

	query := `SELECT * FROM Orders.OrdersByCustomer WHERE CustomerID = ?`
	mapping, err := bloblang.Parse(`
root = []
root."-".S = json("id")
`)
	require.NoError(t, err)

	callCount := 0
	client := &mockProcDynamoDB{
		pexecFn: func(_ context.Context, _ *dynamodb.ExecuteStatementInput) (*dynamodb.ExecuteStatementOutput, error) {
			callCount++
			if callCount == 2 {
				return nil, fmt.Errorf("simulated error")
			}
			return &dynamodb.ExecuteStatementOutput{
				Items: []map[string]types.AttributeValue{
					{"id": &types.AttributeValueMemberS{Value: "found"}},
				},
			}, nil
		},
	}

	db := newDynamoDBPartiQL(nil, client, query, nil, mapping, true)

	reqBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"cust1"}`)),
		service.NewMessage([]byte(`{"id":"cust2"}`)),
	}

	resBatch, err := db.ProcessBatch(t.Context(), reqBatch)
	require.NoError(t, err)
	require.Len(t, resBatch, 1)

	require.NoError(t, resBatch[0][0].GetError())

	require.Error(t, resBatch[0][1].GetError())
	assert.Contains(t, resBatch[0][1].GetError().Error(), "simulated error")
}
