package e2etest

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

func Workflow(ctx workflow.Context) error {
	var result LargePayloadActivityResponse
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: time.Second * 5,
	}), LargePayloadActivity).Get(ctx, &result); err != nil {
		return err
	}

	if result.Data != "hello world" {
		return fmt.Errorf("unexpected activity result: %q", result.Data)
	}

	return nil
}

type LargePayloadActivityResponse struct {
	Data string
}

func LargePayloadActivity(ctx context.Context) (LargePayloadActivityResponse, error) {
	return LargePayloadActivityResponse{
		Data: "hello world",
	}, nil
}
