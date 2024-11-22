package gomongodb

import (
	"context"

	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// CollectionWrapperGeneric 基于泛型对Find、Insert类操作进行重写
type CollectionWrapperGeneric[T any] interface {
	CollectionWrapperBase

	// Find
	Find(ctx context.Context, filter interface{}, sort []string, skip, limit int64, opts ...*options.FindOptions) (result []T, err error)

	// FindOne
	FindOne(ctx context.Context, filter interface{}, sort []string, skip int64, opts ...*options.FindOneOptions) (result T, has bool, err error)

	// FindID
	FindID(ctx context.Context, ID interface{}, opts ...*options.FindOneOptions) (result T, has bool, err error)

	// FindOneAndUpdate
	FindOneAndUpdate(ctx context.Context, filter, update interface{}, sort []string, upsert, returnNew bool, opts ...*options.FindOneAndUpdateOptions) (result T, has bool, err error)

	// FindOneAndReplace
	FindOneAndReplace(ctx context.Context, filter, replacement interface{}, sort []string, upsert, returnNew bool, opts ...*options.FindOneAndReplaceOptions) (result T, has bool, err error)

	// FindOneAndDelete
	FindOneAndDelete(ctx context.Context, filter interface{}, sort []string, opts ...*options.FindOneAndDeleteOptions) (result T, has bool, err error)

	// InsertOne
	InsertOne(ctx context.Context, document T, opts ...*options.InsertOneOptions) (insertedID interface{}, err error)

	// InsertMany
	InsertMany(ctx context.Context, document []T, opts ...*options.InsertManyOptions) (insertedIDs []interface{}, err error)
}

type collectionWrapperGeneric[T any] struct {
	*collectionWrapper
}

func (c *collectionWrapperGeneric[T]) Find(ctx context.Context, filter interface{},
	sort []string, skip, limit int64, opts ...*options.FindOptions) (result []T, err error) {
	err = c.collectionWrapper.Find(ctx, filter, &result, sort, skip, limit, opts...)
	return
}

func (c *collectionWrapperGeneric[T]) FindOne(ctx context.Context, filter interface{},
	sort []string, skip int64, opts ...*options.FindOneOptions) (result T, has bool, err error) {
	has, err = c.collectionWrapper.FindOne(ctx, filter, &result, sort, skip, opts...)
	return
}

func (c *collectionWrapperGeneric[T]) FindID(ctx context.Context, ID interface{},
	opts ...*options.FindOneOptions) (result T, has bool, err error) {
	has, err = c.collectionWrapper.FindID(ctx, ID, &result, opts...)
	return
}

func (c *collectionWrapperGeneric[T]) FindOneAndUpdate(ctx context.Context, filter, update interface{},
	sort []string, upsert, returnNew bool, opts ...*options.FindOneAndUpdateOptions) (result T, has bool, err error) {
	has, err = c.collectionWrapper.FindOneAndUpdate(ctx, filter, update, &result, sort, upsert, returnNew, opts...)
	return
}

func (c *collectionWrapperGeneric[T]) FindOneAndReplace(ctx context.Context, filter, replacement interface{},
	sort []string, upsert, returnNew bool, opts ...*options.FindOneAndReplaceOptions) (result T, has bool, err error) {
	has, err = c.collectionWrapper.FindOneAndReplace(ctx, filter, replacement, &result, sort, upsert, returnNew, opts...)
	return
}

func (c *collectionWrapperGeneric[T]) FindOneAndDelete(ctx context.Context, filter interface{},
	sort []string, opts ...*options.FindOneAndDeleteOptions) (result T, has bool, err error) {
	has, err = c.collectionWrapper.FindOneAndDelete(ctx, filter, &result, sort, opts...)
	return
}

func (c *collectionWrapperGeneric[T]) InsertOne(ctx context.Context, document T,
	opts ...*options.InsertOneOptions) (insertedID interface{}, err error) {
	return c.collectionWrapper.InsertOne(ctx, document, opts...)
}

func (c *collectionWrapperGeneric[T]) InsertMany(ctx context.Context, document []T,
	opts ...*options.InsertManyOptions) (insertedIDs []interface{}, err error) {
	return c.collectionWrapper.InsertMany(ctx, lo.ToAnySlice(document), opts...)
}
