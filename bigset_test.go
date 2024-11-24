package bigset_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/nicois/bigset"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	_logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("can't initialize zap logger: %v\n", err)
		os.Exit(1)
	}
	logger = _logger
}

func TestNames(t *testing.T) {
	ctx := context.Background()
	b, err := bigset.Create[int](logger)
	require.Nil(t, err)

	// disallow a name with double-quotes
	n, err := b.Add(ctx, "fo\"o")
	require.Error(t, err)
	require.Equal(t, n, int64(-1))
}

func TestSomething(t *testing.T) {
	ctx := context.Background()
	b, err := bigset.Create[int](logger)
	require.Nil(t, err)

	// create an empty set
	n, err := b.Add(ctx, "foo")
	require.Nil(t, err)
	require.Equal(t, n, int64(0))

	// add one element
	n, err = b.Add(ctx, "foo", 10)
	require.Nil(t, err)
	require.Equal(t, n, int64(1))

	// add the same element
	n, err = b.Add(ctx, "foo", 10)
	require.Nil(t, err)
	require.Equal(t, n, int64(0))

	// create a second set with an element
	n, err = b.Add(ctx, "bar", 10)
	require.Nil(t, err)
	require.Equal(t, n, int64(1))

	// add multiple elements to the second set
	n, err = b.Add(ctx, "bar", 9, 10, 11, 12, 13)
	require.Nil(t, err)
	require.Equal(t, n, int64(4))

	// generate and check the intersection
	n, err = b.Intersection(ctx, "i", "foo", "bar")
	require.Nil(t, err)
	require.Equal(t, n, int64(1))

	nums, err := b.Get(ctx, "i")
	require.Nil(t, err)
	require.Equal(t, 10, (*nums)[0])
	require.Equal(t, &[]int{10}, nums)

	// generate and check the union
	n, err = b.Union(ctx, "u", "foo", "bar")
	require.Nil(t, err)
	require.Equal(t, n, int64(5))

	nums, err = b.Get(ctx, "u")
	require.Nil(t, err)
	require.Contains(t, *nums, 10)
	require.Contains(t, *nums, 13)
	require.NotContains(t, *nums, 23)

	// discard some elements
	n, err = b.Discard(ctx, "u", 10, 13)
	require.Nil(t, err)
	require.Equal(t, n, int64(2))

	n, err = b.Discard(ctx, "u", 10)
	require.Nil(t, err)
	require.Equal(t, n, int64(0))

	// remove all elements from one set which are in at least
	// one of the other sets
	n, err = b.Subtract(ctx, "u", "foo", "bar")
	require.Nil(t, err)
	require.Equal(t, n, int64(3))

	nums, err = b.Get(ctx, "u")
	require.Nil(t, err)
	require.Len(t, *nums, 0)
}
