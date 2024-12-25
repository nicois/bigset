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

func TestSupersede(t *testing.T) {
	ctx := context.Background()
	// books with the same name are considered the "same"
	keyFunction := func(b *Book) []byte {
		return []byte(fmt.Sprintf("%v", b.Name))
	}
	b, err := bigset.Create[Book](logger, bigset.WithKeyFunction(keyFunction))
	require.Nil(t, err)

	n, err := b.Add(
		ctx,
		"foo",
		Book{Name: "y", Pages: 5},
		Book{Name: "x", Pages: 10},
		Book{Name: "x", Pages: 20},
		Book{Name: "x", Pages: 30},
		Book{Name: "y", Pages: 8},
	)
	require.Equal(t, int64(2), n)
	require.Nil(t, err)

	var buffer Book
	err = b.Each(ctx, "foo", &buffer, func(ctx context.Context) error {
		fmt.Printf("%+v\n", buffer.Name)
		return nil
	})
	require.Nil(t, err)

	book, err := b.RetrieveIfExists(ctx, "foo", Book{Name: "x"})
	require.Nil(t, err)
	require.NotNil(t, book)
	require.Equal(t, 10, book.Pages)

	// this should displace the previously-existing values
	// for x and y, and insert z
	n, err = b.Supersede(
		ctx,
		"foo",
		Book{Name: "x", Pages: 40},
		Book{Name: "y", Pages: 12},
		Book{Name: "z", Pages: 100},
	)
	require.Nil(t, err)
	require.Equal(t, int64(3), n)

	book, err = b.RetrieveIfExists(ctx, "foo", Book{Name: "x"})
	require.Nil(t, err)
	require.NotNil(t, book)
	require.Equal(t, 40, book.Pages)
}

func TestKeyFunction(t *testing.T) {
	ctx := context.Background()
	// 1, 11, 21, 31, 41, etc should all be consided the same
	// object for the purposes of deduplication
	keyFunction := func(i *int) []byte {
		return []byte(fmt.Sprintf("%v", (*i)%10))
	}
	b, err := bigset.Create[int](logger, bigset.WithKeyFunction(keyFunction))
	require.Nil(t, err)

	n, err := b.Add(ctx, "foo", 1, 2, 3, 11, 12, 13, 21, 22, 23, 31, 32, 33)
	require.Equal(t, n, int64(3))
	require.Nil(t, err)
}

func TestFilename(t *testing.T) {
	tempfile, err := os.CreateTemp("", "bigset-test")
	require.Nil(t, err)
	require.Nil(t, tempfile.Close())

	filename := tempfile.Name()
	ctx := context.Background()
	b, err := bigset.Create[int](logger, bigset.WithFilename[int](filename))
	require.Nil(t, err)

	// add two elements
	n, err := b.Add(ctx, "foo", 10, 20)
	require.Nil(t, err)
	require.Equal(t, n, int64(2))

	require.Nil(t, b.Close())

	b2, err := bigset.Create[int](logger, bigset.WithFilename[int](filename))
	require.Nil(t, err)
	// add 3 elements, one of them new
	n, err = b2.Add(ctx, "foo", 10, 20, 30)
	require.Nil(t, err)
	require.Equal(t, n, int64(1))
	require.Nil(t, b2.Close())
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

	require.Nil(t, b.Close())
}

type Book struct {
	Name      string
	Pages     int
	Favourite bool
}

func TestStructAndEach(t *testing.T) {
	martin := Book{Name: "Martin the Warrior", Pages: 375, Favourite: true}
	mossflower := Book{Name: "Mossflower", Pages: 420, Favourite: true}
	salamandastron := Book{Name: "Salamandastron", Pages: 336, Favourite: true}

	ctx := context.Background()
	b, err := bigset.Create[Book](logger)
	require.Nil(t, err)

	n, err := b.Add(ctx, "m books", martin, mossflower)
	require.Nil(t, err)
	require.Equal(t, n, int64(2))

	n, err = b.Add(ctx, "s books", salamandastron)
	require.Nil(t, err)
	require.Equal(t, n, int64(1))

	n, err = b.Union(ctx, "all books", "s books", "m books")
	require.Nil(t, err)
	require.Equal(t, n, int64(3))

	var buffer Book
	booksByName := make(map[string]Book)
	err = b.Each(ctx, "all books", &buffer, func(ctx context.Context) error {
		booksByName[buffer.Name] = buffer
		return nil
	})
	require.Nil(t, err)
	require.Equal(
		t,
		booksByName,
		map[string]Book{
			"Martin the Warrior": martin,
			"Mossflower":         mossflower,
			"Salamandastron":     salamandastron,
		},
	)

	require.Nil(t, b.Close())
}
