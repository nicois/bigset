
[![Go Reference](https://pkg.go.dev/badge/github.com/nicois/bigset.svg)](https://pkg.go.dev/github.com/nicois/bigset)
[![Build Status](https://github.com/nicois/bigset/actions/workflows/golangci-lint.yaml/badge.svg)](https://github.com/nicois/bigset/actions/workflows/golangci-lint.yaml)
[![Build Status](https://github.com/nicois/bigset/actions/workflows/test.yaml/badge.svg)](https://github.com/nicois/bigset/actions/workflows/test.yaml)

# bigset

Bigset allows sets of json-encodable structures to be manipulated
on disk via sqlite. This reduces memory usage significantly when dealing
with large collections of objects.

Note that objects are serialised using their JSON representation, and that
the serialized form is used to determine uniqueness. It is therefore not
appropriate to use Bigset for objects which contain state which cannot be serialised.

```go
alice := Example{Age: 20, Name: "Alice"}
bob := Example{Age: 28, Name: "Bob"}
charlie := Example{Age: 58, Name: "Charlie"}
anotherCharlie := Example{Age: 56, Name: "Charlie"}
eve := Example{Age: 50, Name: "Eve"}

ctx := context.Background()
b, err := bigset.Create[Example](logger)

// define and populate some sets
// returns the number of elements added to the set
n, err := b.Add(ctx, "males", bob, charlie, anotherCharlie)
if err != nil {
    panic(err)
}
fmt.Println(n)
n, err = b.Add(ctx, "females", alice, eve)

// Retrieve the contents of a set as a pointer to an  array
contents, err := b.Get(ctx, "males")
fmt.Println(contents)

// More efficiently, iterate over a set:
var buffer Example
err = b.Each(ctx, "males", &buffer, func(ctx context.Context) error {
    fmt.Println(buffer)
    return nil
})

// Perform set operations, adding the results to the target
// set, creating it if required
n, err = b.Intersection(ctx, "androgenous", "males", "females")
n, err = b.Union(ctx, "everyone", "males", "females")

// discard some elements
n, err = b.Discard(ctx, "everyone", bob, eve)

// remove all elements from one set which are in at least
// one of the other sets
n, err = b.Subtract(ctx, "everyone", "males", "androgenous")
```
