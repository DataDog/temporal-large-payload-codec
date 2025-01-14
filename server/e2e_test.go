// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package server_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DataDog/temporal-large-payload-codec/codec"
	"github.com/DataDog/temporal-large-payload-codec/server"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
	"go.temporal.io/api/enums/v1"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/temporalite/temporaltest"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	largePayloadSize = 5_000_000
	letterBytes      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	taskQueue        = "large_payloads"
)

func Workflow(ctx workflow.Context) error {
	var result LargePayloadActivityResponse

	bigData := make([]byte, largePayloadSize)
	for i := range bigData {
		bigData[i] = letterBytes[rand.Intn(len(letterBytes))]
	}

	request := LargePayloadActivityRequest{
		Data: string(bigData),
	}

	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: time.Second * 5,
	}), "LargePayloadActivity", request).Get(ctx, &result); err != nil {
		return err
	}

	if request.Data != result.Data {
		return fmt.Errorf("unexpected activity result: %q", result.Data)
	}

	return nil
}

type LargePayloadActivityRequest struct {
	Data string
}

type LargePayloadActivityResponse struct {
	Data string
}

func LargePayloadActivity(_ context.Context, req LargePayloadActivityRequest) (LargePayloadActivityResponse, error) {
	return LargePayloadActivityResponse{
		Data: req.Data,
	}, nil
}

func TestWorker(t *testing.T) {
	// Create test remote codec service
	testCodecServer := httptest.NewServer(server.NewHttpHandler(&memory.Driver{}))
	defer testCodecServer.Close()
	// Create test codec (to be used from Go SDK)
	testCodec, err := codec.New(
		codec.WithURL(testCodecServer.URL),
		codec.WithNamespace("e2e-test"),
		codec.WithHTTPClient(testCodecServer.Client()),
		codec.WithMinBytes(1_000_000),
	)
	if err != nil {
		t.Fatal(err)
	}
	testDataConverter := converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), testCodec)

	// Create test Temporal server and client
	ts := temporaltest.NewServer(temporaltest.WithT(t))
	testClient := ts.NewClientWithOptions(client.Options{
		DataConverter: testDataConverter,
	})

	// Register a new worker
	testWorker := worker.New(testClient, taskQueue, worker.Options{})
	defer testWorker.Stop()

	testWorker.RegisterWorkflow(Workflow)
	testWorker.RegisterActivity(LargePayloadActivity)
	err = testWorker.Start()
	require.NoError(t, err)

	// Start a workflow that executes an activity with a "large" response payload
	wfr, err := testClient.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: time.Second * 60,
	}, Workflow)
	require.NoError(t, err)

	// Wait for workflow to complete and fail test if workflow errors.
	err = wfr.Get(context.Background(), nil)
	require.NoError(t, err)

	// Validate that activity result was encoded via remote codec
	wfHistory := testClient.GetWorkflowHistory(context.Background(), wfr.GetID(), wfr.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for wfHistory.HasNext() {
		event, err := wfHistory.Next()
		require.NoError(t, err)
		t.Log(event.GetEventType().String())
		if event.GetEventType() == enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			payload := event.GetActivityTaskCompletedEventAttributes().GetResult().GetPayloads()[0]
			_, ok := payload.GetMetadata()["temporal.io/remote-codec"]
			require.True(t, ok)
		}
	}
}
