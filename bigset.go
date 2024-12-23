package bigset

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/nicois/fastdb"
	"go.uber.org/zap"
)

// Bigset allows sets of json-encodable structures to be manipulated
// on disk via sqlite. This reduces memory usage significantly when dealing
// with large collections of objects.
type Bigset[T comparable] struct {
	logger   *zap.Logger
	filename string
	db       fastdb.FastDB
	names    map[string]struct{}
}

func (b *Bigset[T]) initialise(ctx context.Context, name string) error {
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%v\" (v TEXT UNIQUE);", name)
	_, err := b.db.Writer().ExecContext(ctx, sql)
	if err == nil {
		b.names[name] = struct{}{}
	}
	return err
}

// Cardinality returns the number of items in a set.
func (b *Bigset[T]) Cardinality(ctx context.Context, name string) (int64, error) {
	if err := verifyNames(name); err != nil {
		return -1, err
	}
	sql := fmt.Sprintf("SELECT COUNT(*) FROM \"%v\"", name)
	var result int64
	err := b.db.Reader().QueryRowContext(ctx, sql, name).Scan(&result)
	if err != nil {
		return -1, err
	}
	return result, nil
}

// Each executes the provided function on each item of the set in turn.
// During each iteration, the `buffer` is populated with a different value.
func (b *Bigset[T]) Each(ctx context.Context, name string, buffer *T, f func(ctx context.Context) error) error {
	if err := verifyNames(name); err != nil {
		return err
	}
	rows, err := b.db.Reader().QueryContext(ctx, fmt.Sprintf("SELECT v FROM \"%v\"", name))
	if err != nil {
		return err
	}
	defer rows.Close()
	rawRow := sql.RawBytes{}
	for rows.Next() {
		err = rows.Scan(&rawRow)
		if err != nil {
			return err
		}
		err = json.Unmarshal(rawRow, buffer)
		if err != nil {
			return err
		}
		err = f(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Get returns a pointer to a list of all the items in a set
func (b *Bigset[T]) Get(ctx context.Context, name string) (*[]T, error) {
	if err := verifyNames(name); err != nil {
		return nil, err
	}
	size, err := b.Cardinality(ctx, name)
	if err != nil {
		return nil, err
	}
	result := make([]T, 0, size)
	var buffer T

	err = b.Each(ctx, name, &buffer, func(ctx context.Context) error {
		result = append(result, buffer)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if int64(len(result)) > size {
		b.logger.Warn("Set has grown during read, leading to less efficient memory usage", zap.String("set name", name), zap.Int64("expected size", size), zap.Int("actual size", len(result)))
	}
	return &result, nil
}

// Union adds every element of each source set to the target set.
// The `target` set retains any additional items it originally contained.
// It returns the number of inserted elements.
func (b *Bigset[T]) Union(ctx context.Context, target string, source ...string) (int64, error) {
	if err := verifyNames(target, source...); err != nil {
		return -1, err
	}
	if _, exists := b.names[target]; !exists {
		if err := b.initialise(ctx, target); err != nil {
			return -1, err
		}
	}
	if len(source) < 1 {
		return 0, nil
	}
	sqlArray := make([]string, 0, 1+len(source))
	sqlArray = append(sqlArray, fmt.Sprintf("INSERT INTO \"%v\" ", target))
	sqlArray = append(sqlArray, fmt.Sprintf("SELECT v FROM \"%v\" ", source[0]))
	for _, sTable := range source[1:] {
		sqlArray = append(sqlArray, fmt.Sprintf("UNION SELECT v FROM \"%v\"", sTable))
	}
	return b.apply(ctx, sqlArray...)
}

// Subtract removes any items from `target` which are present in at least one
// of the `source` sets.
// It returns the number of removed elements.
func (b *Bigset[T]) Subtract(ctx context.Context, target string, source ...string) (int64, error) {
	if err := verifyNames(target, source...); err != nil {
		return -1, err
	}
	if _, exists := b.names[target]; !exists {
		if err := b.initialise(ctx, target); err != nil {
			return -1, err
		}
		return 0, nil
	}
	if len(source) < 1 {
		return 0, nil
	}
	var result int64
	for _, sTable := range source {
		sql := fmt.Sprintf("DELETE FROM \"%v\" WHERE v IN (SELECT v FROM \"%v\")", target, sTable)
		n, err := b.apply(ctx, sql)
		if err != nil {
			return -1, err
		}
		result += n
	}
	return result, nil
}

// Intersection adds elements to `target` which are present in every source set.
// Any elements already present in `target` are retained, regardless of whether they
// are also in the source sets.
// Returns the number of added elements.
func (b *Bigset[T]) Intersection(ctx context.Context, target string, source ...string) (int64, error) {
	if err := verifyNames(target, source...); err != nil {
		return -1, err
	}
	if _, exists := b.names[target]; !exists {
		if err := b.initialise(ctx, target); err != nil {
			return -1, err
		}
	}
	if len(source) < 1 {
		return 0, nil
	}
	sqlArray := make([]string, 0, len(source))
	sqlArray = append(sqlArray, fmt.Sprintf("INSERT INTO \"%v\" SELECT v FROM \"%v\" ", target, source[0]))
	for _, sTable := range source[1:] {
		sqlArray = append(sqlArray, fmt.Sprintf("INNER JOIN \"%v\" USING (v)", sTable))
	}
	return b.apply(ctx, sqlArray...)
}

func (b *Bigset[T]) apply(ctx context.Context, sqlArray ...string) (int64, error) {
	sql := strings.Join(sqlArray, "")
	result, err := b.db.Writer().ExecContext(ctx, sql)
	if err != nil {
		return -1, err
	}
	ra, err := result.RowsAffected()
	if err != nil {
		return -1, err
	}
	return ra, nil
}

// Discard removes elements from a set, if present.
// Returns the number of elements actually removed.
func (b *Bigset[T]) Discard(ctx context.Context, name string, values ...T) (int64, error) {
	if err := verifyNames(name); err != nil {
		return -1, err
	}
	if _, exists := b.names[name]; !exists {
		if err := b.initialise(ctx, name); err != nil {
			return -1, err
		}
	}
	sql := fmt.Sprintf("DELETE FROM \"%v\" WHERE v = ?", name)
	stmt, err := b.db.Writer().PrepareContext(ctx, sql)
	if err != nil {
		return -1, err
	}
	result := int64(0)
	for _, value := range values {
		m, err := json.Marshal(value)
		if err != nil {
			return -1, err
		}
		execResult, err := stmt.ExecContext(ctx, m)
		if err != nil {
			return -1, err
		}
		ra, err := execResult.RowsAffected()
		if err != nil {
			return -1, err
		}
		result += ra
	}
	return result, nil
}

// Add inserts elements into a set, if not already present.
// Returns the number of elements actually added.
func (b *Bigset[T]) Add(ctx context.Context, name string, values ...T) (int64, error) {
	if err := verifyNames(name); err != nil {
		return -1, err
	}
	if _, exists := b.names[name]; !exists {
		if err := b.initialise(ctx, name); err != nil {
			return -1, err
		}
	}
	sql := fmt.Sprintf("INSERT INTO \"%v\"(v) VALUES (?) ON CONFLICT (v) DO NOTHING;", name)
	stmt, err := b.db.Writer().PrepareContext(ctx, sql)
	if err != nil {
		return -1, err
	}
	result := int64(0)
	for _, value := range values {
		m, err := json.Marshal(value)
		if err != nil {
			return -1, err
		}
		execResult, err := stmt.ExecContext(ctx, m)
		if err != nil {
			return -1, err
		}
		ra, err := execResult.RowsAffected()
		if err != nil {
			return -1, err
		}
		result += ra
	}
	return result, nil
}

func verifyNames(name string, names ...string) error {
	if strings.Contains(name, "\"") {
		return fmt.Errorf("%v is not an allowable name as it contains double quotes.", name)
	}
	for _, name := range names {
		if strings.Contains(name, "\"") {
			return fmt.Errorf("%v is not an allowable name as it contains double quotes.", name)
		}
	}
	return nil
}

// Close frees up resources used by Bigset.
// It must not be used after being closed.
func (b *Bigset[T]) Close() error {
	if err := b.db.Close(); err != nil {
		return err
	}
	b.db = nil
	return os.Remove(b.filename)
}

// Create creates a new Bigset.
func Create[T comparable](logger *zap.Logger) (*Bigset[T], error) {
	tempfile, err := os.CreateTemp("", "bigset")
	if err != nil {
		return nil, err
	}
	if err = tempfile.Close(); err != nil {
		return nil, err
	}
	db, err := fastdb.Open(tempfile.Name())
	if err != nil {
		return nil, err
	}
	return &Bigset[T]{filename: tempfile.Name(), db: db, logger: logger, names: make(map[string]struct{}, 0)}, nil
}
