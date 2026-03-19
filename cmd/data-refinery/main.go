package main

import (
	"context"
	"os"

	"github.com/benjaminwestern/data-refinery/internal/app"
)

func main() {
	os.Exit(app.Run(context.Background(), os.Args[1:]))
}
