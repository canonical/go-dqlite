package logging_test

import (
	"testing"

	"github.com/canonical/go-dqlite/logging"
)

func Test_TestFunc(t *testing.T) {
	f := logging.Test(t)
	f(logging.Info, "hello")
}
