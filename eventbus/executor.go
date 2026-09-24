package eventbus

import (
	"context"
	"log"
)

var (
	_ executor = (*syncExecutor)(nil)
	_ executor = (*asyncExecutor)(nil)
)

type executor interface {
	executeTask(ctx context.Context, args []string)
}

type syncExecutor struct {
}

func (s *syncExecutor) executeTask(ctx context.Context, args []string) {
	log.Printf("ctx: %#v, args: %#v\n", ctx, args)
}

type asyncExecutor struct {
}

func (s *asyncExecutor) executeTask(ctx context.Context, args []string) {
	log.Printf("ctx: %#v, args: %#v\n", ctx, args)
}
