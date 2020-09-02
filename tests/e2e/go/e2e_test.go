package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/feast-dev/feast/sdk/go"
	"github.com/feast-dev/feast/sdk/go/protos/feast/serving"
	"github.com/feast-dev/feast/sdk/go/protos/feast/types"
	"github.com/google/go-cmp/cmp"
)

type TestParams struct {
	servingHost string
	servingPort int
	useAuth     bool
}

// Parse test params from environment variables
// SERVING_HOST - hostname of the Feast Serving instance to connect to.
// SERVING_PORT - port of Feast Serving instance to connect to.
// USE_AUTH - whether to use authentication when connect to Feast Serving.f
func parseParams() (*TestParams, error) {
	port, err := strconv.Atoi(os.Getenv("SERVING_PORT"))
	if err != nil {
		return nil, fmt.Errorf("Could not parse SERVING_PORT as a int")
	}
	useAuth, err := strconv.ParseBool(os.Getenv("USE_AUTH"))
	if err != nil {
		return nil, fmt.Errorf("Could not parse USE_AUTH as a boolean")
	}
	return &TestParams{
		servingHost: os.Getenv("SERVING_HOST"),
		servingPort: port,
		useAuth:     useAuth,
	}, nil
}

// Configure and return a Feast Client based on the given test params.
func configClient(params *TestParams) (*feast.GrpcClient, error) {
	if params.useAuth {
		cred, err := feast.NewGoogleCredential("localhost:6566")
		if err != nil {
			return nil, err
		}
		return feast.NewAuthGrpcClient(params.servingHost, params.servingPort, feast.SecurityConfig{
			Credential: cred,
		})
	} else {
		return feast.NewGrpcClient(params.servingHost, params.servingPort)
	}
}

// E2E Test Retrieval using Go SDK with Feast. Expects Feast to have a registered Feature Set
// 'customer_transactions' that is already ingested with data and ready for retrieval. See
// tests/e2e/python/redis/basic-ingest-redis-serving.py for test setup code.
func TestGetOnlineFeatures(t *testing.T) {
	// setup test client
	params, err := parseParams()
	if err != nil {
		t.Error(err)
	}
	cli, err := configClient(params)
	if err != nil {
		t.Error(err)
	}

	tt := []struct {
		name string
		req  *feast.OnlineFeaturesRequest
		want *feast.OnlineFeaturesResponse
	}{
		{
			name: "Valid Get OnlineFeaturesRequest",
			req: &feast.OnlineFeaturesRequest{
				Features: []string{
					"daily_transactions",
					"customer_transactions:total_transactions",
				},
				Entities: []feast.Row{
					{"customer_id": feast.Int64Val(1)},
				},
			},
			want: &feast.OnlineFeaturesResponse{
				RawResponse: &serving.GetOnlineFeaturesResponse{
					FieldValues: []*serving.GetOnlineFeaturesResponse_FieldValues{
						{
							Fields: map[string]*types.Value{
								"customer_id":        feast.Int64Val(1),
								"daily_transactions": feast.FloatVal(1.0),
								"customer_transactions:total_transactions": feast.FloatVal(1.0),
							},
							Statuses: map[string]serving.GetOnlineFeaturesResponse_FieldStatus{
								"customer_id":        serving.GetOnlineFeaturesResponse_PRESENT,
								"daily_transactions": serving.GetOnlineFeaturesResponse_PRESENT,
								"customer_transactions:total_transactions": serving.GetOnlineFeaturesResponse_PRESENT,
							},
						},
					},
				},
			},
		},
	}

	ctx := context.Background()
	for _, tc := range tt {
		resp, err := cli.GetOnlineFeatures(ctx, tc.req)
		if err != nil {
			t.Error(err)
		}

		if !cmp.Equal(resp.RawResponse.String(), tc.want.RawResponse.String()) {
			t.Errorf("got: \n%v\nwant:\n%v", resp.RawResponse.String(), tc.want.RawResponse.String())
		}
	}
}
