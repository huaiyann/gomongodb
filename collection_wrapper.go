package gomongodb

import (
	"context"
	"runtime"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CollectionWrapperBase interface {
	// Collection get official mongo.Collection
	Collection() *mongo.Collection

	// GenSortBson translate sort keys like [-_id, cnt, +ut] to bson.D
	GenSortBson(sort []string) (result bson.D)

	// FindCursor 返回官方的cursor，注意通过这个cursor读取数据，会脱离metrics监控。只有当需要读取大量数据，Find会超时时，才用FindCursor。
	FindCursor(ctx context.Context, filter interface{}, sort []string, skip, limit int64, opts ...*options.FindOptions) (cursor *mongo.Cursor, err error)

	// UpdateOne
	UpdateOne(ctx context.Context, filter, update interface{}, upsert bool, opts ...*options.UpdateOptions) (result *mongo.UpdateResult, err error)

	// UpdateID
	UpdateID(ctx context.Context, ID, update interface{}, upsert bool, opts ...*options.UpdateOptions) (result *mongo.UpdateResult, err error)

	// UpdateMany
	UpdateMany(ctx context.Context, filter, update interface{}, upsert bool, opts ...*options.UpdateOptions) (result *mongo.UpdateResult, err error)

	// Counts
	Count(ctx context.Context, filter interface{}, skip, limit int64, opts ...*options.CountOptions) (count int64, err error)

	// EstimatedCount，For a fast count of the documents in the collection
	EstimatedCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (count int64, err error)

	// DeleteOne
	DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (has bool, err error)

	// DeleteID
	DeleteID(ctx context.Context, ID interface{}, opts ...*options.DeleteOptions) (has bool, err error)

	// DeleteMany
	DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (deletedCnt int64, err error)

	// Distinct
	Distinct(ctx context.Context, filedName string, filter interface{}, opts ...*options.DistinctOptions) (result []interface{}, err error)

	// BulkWrite
	BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (result *mongo.BulkWriteResult, err error)

	// Aggregate
	// pipeline: a slice of aggragate commands
	// result: a slice address
	Aggregate(ctx context.Context, pipeline, result interface{}, opts ...*options.AggregateOptions) (err error)

	/*UseSession provides the ability of transaction.
	* fn is a closure, all the action in it should use SessionContext as context param.
	* Use OfficialClient.GenSessionWrapper() is a good idea to simple your code.
	 */
	UseSession(ctx context.Context, fn func(mongo.SessionContext) error, opts ...*options.SessionOptions) (err error)
}

// CollectionWrapper declares a wrapper of mongo collection operators.
type CollectionWrapper interface {
	CollectionWrapperBase

	// Find
	Find(ctx context.Context, filter interface{}, result interface{}, sort []string, skip, limit int64, opts ...*options.FindOptions) (err error)

	// FindOne
	FindOne(ctx context.Context, filter interface{}, result interface{}, sort []string, skip int64, opts ...*options.FindOneOptions) (has bool, err error)

	// FindID
	FindID(ctx context.Context, ID interface{}, result interface{}, opts ...*options.FindOneOptions) (has bool, err error)

	// FindOneAndUpdate
	FindOneAndUpdate(ctx context.Context, filter, update, result interface{}, sort []string, upsert, returnNew bool, opts ...*options.FindOneAndUpdateOptions) (has bool, err error)

	// FindOneAndReplace
	FindOneAndReplace(ctx context.Context, filter, replacement, result interface{}, sort []string, upsert, returnNew bool, opts ...*options.FindOneAndReplaceOptions) (has bool, err error)

	// FindOneAndDelete
	FindOneAndDelete(ctx context.Context, filter, result interface{}, sort []string, opts ...*options.FindOneAndDeleteOptions) (has bool, err error)

	// InsertOne
	InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (insertedID interface{}, err error)

	// InsertMany
	InsertMany(ctx context.Context, document []interface{}, opts ...*options.InsertManyOptions) (insertedIDs []interface{}, err error)
}

var _ CollectionWrapper = &collectionWrapper{}

type collectionWrapper struct {
	client     *Client
	database   string
	collection string
}

func (c *collectionWrapper) GenSortBson(sort []string) (result bson.D) {
	result = bson.D{}
	for _, v := range sort {
		if strings.HasPrefix(v, "-") {
			v = strings.TrimPrefix(v, "-")
			result = append(result, bson.E{Key: v, Value: -1})
		} else {
			v = strings.TrimPrefix(v, "+")
			result = append(result, bson.E{Key: v, Value: 1})
		}
	}
	return
}

func (c *collectionWrapper) Collection() *mongo.Collection {
	return c.client.Client().Database(c.database).Collection(c.collection)
}

func (c *collectionWrapper) FindCursor(ctx context.Context, filter interface{},
	sort []string, skip, limit int64, opts ...*options.FindOptions) (cursor *mongo.Cursor, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	opt := options.Find().SetSkip(skip).SetLimit(limit)
	if len(sort) > 0 {
		opt.SetSort(c.GenSortBson(sort))
	}
	opts = append(opts, opt)

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "find_cursor")
	defer span.End()

	cursor, err = conn.Database(c.database).Collection(c.collection).Find(ctx, filter, opts...)
	if err != nil {
		return
	}
	return
}

func (c *collectionWrapper) Find(ctx context.Context, filter interface{}, result interface{},
	sort []string, skip, limit int64, opts ...*options.FindOptions) (err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	opt := options.Find().SetSkip(skip).SetLimit(limit)
	if len(sort) > 0 {
		opt.SetSort(c.GenSortBson(sort))
	}
	opts = append(opts, opt)

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "find")
	defer span.End()

	cursor, err := conn.Database(c.database).Collection(c.collection).Find(ctx, filter, opts...)
	if err != nil {
		return
	}
	err = c.client.ScanCursor(ctx, cursor, result)
	if err != nil {
		return
	}
	return
}

func (c *collectionWrapper) FindOne(ctx context.Context, filter interface{}, result interface{},
	sort []string, skip int64, opts ...*options.FindOneOptions) (has bool, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	return c.findOne(ctx, filter, result, sort, skip, opts...)
}

func (c *collectionWrapper) findOne(ctx context.Context, filter interface{}, result interface{},
	sort []string, skip int64, opts ...*options.FindOneOptions) (has bool, err error) {

	opt := options.FindOne().SetSkip(skip)
	if len(sort) > 0 {
		opt.SetSort(c.GenSortBson(sort))
	}
	opts = append(opts, opt)

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "findOne")
	defer span.End()

	err = conn.Database(c.database).Collection(c.collection).FindOne(ctx, filter, opts...).Decode(result)
	if err == mongo.ErrNoDocuments {
		has, err = false, nil
		return
	}
	if err != nil {
		return
	}
	has = true
	return
}

func (c *collectionWrapper) FindID(ctx context.Context, ID interface{}, result interface{},
	opts ...*options.FindOneOptions) (has bool, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	filter := bson.M{"_id": ID}
	return c.findOne(ctx, filter, result, nil, 0, opts...)
}

func (c *collectionWrapper) FindOneAndUpdate(ctx context.Context, filter, update, result interface{},
	sort []string, upsert, returnNew bool, opts ...*options.FindOneAndUpdateOptions) (has bool, err error) {

	if err := updateSafeCheck(update); err != nil {
		return false, err
	}

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	opt := options.FindOneAndUpdate()
	if len(sort) > 0 {
		opt.SetSort(c.GenSortBson(sort))
	}
	opt.SetUpsert(upsert)
	if returnNew {
		opt.SetReturnDocument(options.After)
	} else {
		opt.SetReturnDocument(options.Before)
	}
	opts = append(opts, opt)

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "FindOneAndUpdate")
	defer span.End()

	err = conn.Database(c.database).Collection(c.collection).FindOneAndUpdate(ctx, filter, update, opts...).Decode(result)
	if err == mongo.ErrNoDocuments {
		has, err = false, nil
		return
	}
	if err != nil {
		return
	}
	has = true
	return
}

func (c *collectionWrapper) FindOneAndReplace(ctx context.Context, filter, replacement, result interface{},
	sort []string, upsert, returnNew bool, opts ...*options.FindOneAndReplaceOptions) (has bool, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	opt := options.FindOneAndReplace()
	if len(sort) > 0 {
		opt.SetSort(c.GenSortBson(sort))
	}
	opt.SetUpsert(upsert)
	if returnNew {
		opt.SetReturnDocument(options.After)
	} else {
		opt.SetReturnDocument(options.Before)
	}
	opts = append(opts, opt)

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "FindOneAndReplace")
	defer span.End()

	err = conn.Database(c.database).Collection(c.collection).FindOneAndReplace(ctx, filter, replacement, opts...).Decode(result)
	if err == mongo.ErrNoDocuments {
		has, err = false, nil
		return
	}
	if err != nil {
		return
	}
	has = true
	return
}

func (c *collectionWrapper) FindOneAndDelete(ctx context.Context, filter, result interface{},
	sort []string, opts ...*options.FindOneAndDeleteOptions) (has bool, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	opt := options.FindOneAndDelete()
	if len(sort) > 0 {
		opt.SetSort(c.GenSortBson(sort))
	}
	opts = append(opts, opt)

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "FindOneAndDelete")
	defer span.End()

	err = conn.Database(c.database).Collection(c.collection).FindOneAndDelete(ctx, filter, opts...).Decode(result)
	if err == mongo.ErrNoDocuments {
		has, err = false, nil
		return
	}
	if err != nil {
		return
	}
	has = true
	return
}

func (c *collectionWrapper) InsertOne(ctx context.Context, document interface{},
	opts ...*options.InsertOneOptions) (insertedID interface{}, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "InsertOne")
	defer span.End()

	result, err := conn.Database(c.database).Collection(c.collection).InsertOne(ctx, document, opts...)
	if err != nil {
		return
	}
	insertedID = result.InsertedID
	return
}

func (c *collectionWrapper) InsertMany(ctx context.Context, document []interface{},
	opts ...*options.InsertManyOptions) (insertedIDs []interface{}, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "InsertMany")
	defer span.End()

	result, err := conn.Database(c.database).Collection(c.collection).InsertMany(ctx, document, opts...)
	if err != nil {
		return
	}
	insertedIDs = result.InsertedIDs
	return
}

func (c *collectionWrapper) UpdateOne(ctx context.Context, filter, update interface{},
	upsert bool, opts ...*options.UpdateOptions) (result *mongo.UpdateResult, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	return c.updateOne(ctx, filter, update, upsert, opts...)
}

func (c *collectionWrapper) UpdateID(ctx context.Context, ID, update interface{},
	upsert bool, opts ...*options.UpdateOptions) (result *mongo.UpdateResult, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	filter := bson.M{"_id": ID}
	return c.updateOne(ctx, filter, update, upsert, opts...)
}

func (c *collectionWrapper) updateOne(ctx context.Context, filter, update interface{},
	upsert bool, opts ...*options.UpdateOptions) (result *mongo.UpdateResult, err error) {

	if err := updateSafeCheck(update); err != nil {
		return nil, err
	}

	opt := options.Update()
	opt.SetUpsert(upsert)
	opts = append(opts, opt)

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "UpdateOne")
	defer span.End()

	resultOri, err := conn.Database(c.database).Collection(c.collection).UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		return
	}

	result = &mongo.UpdateResult{
		MatchedCount:  resultOri.MatchedCount,
		ModifiedCount: resultOri.ModifiedCount,
		UpsertedCount: resultOri.UpsertedCount,
		UpsertedID:    resultOri.UpsertedID,
	}
	return
}

func (c *collectionWrapper) UpdateMany(ctx context.Context, filter, update interface{},
	upsert bool, opts ...*options.UpdateOptions) (result *mongo.UpdateResult, err error) {

	if err := updateSafeCheck(update); err != nil {
		return nil, err
	}

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	opt := options.Update()
	opt.SetUpsert(upsert)
	opts = append(opts, opt)

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "UpdateMany")
	defer span.End()

	resultOri, err := conn.Database(c.database).Collection(c.collection).UpdateMany(ctx, filter, update, opts...)
	if err != nil {
		return
	}

	result = &mongo.UpdateResult{
		MatchedCount:  resultOri.MatchedCount,
		ModifiedCount: resultOri.ModifiedCount,
		UpsertedCount: resultOri.UpsertedCount,
		UpsertedID:    resultOri.UpsertedID,
	}
	return
}

func (c *collectionWrapper) Count(ctx context.Context, filter interface{}, skip, limit int64,
	opts ...*options.CountOptions) (count int64, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	opt := options.Count().SetSkip(skip)
	if limit > 0 {
		opt.SetLimit(limit)
	}
	opts = append(opts, opt)

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "Count")
	defer span.End()

	count, err = conn.Database(c.database).Collection(c.collection).CountDocuments(ctx, filter, opts...)
	if err != nil {
		return
	}
	return
}

func (c *collectionWrapper) EstimatedCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (count int64, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "EstimatedCount")
	defer span.End()

	count, err = conn.Database(c.database).Collection(c.collection).EstimatedDocumentCount(ctx, opts...)
	if err != nil {
		return
	}
	return
}

func (c *collectionWrapper) deleteOne(ctx context.Context, filter interface{},
	opts ...*options.DeleteOptions) (has bool, err error) {
	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "deleteOne")
	defer span.End()

	result, err := conn.Database(c.database).Collection(c.collection).DeleteOne(ctx, filter, opts...)
	if err != nil {
		return
	}
	has = result.DeletedCount > 0
	return
}

func (c *collectionWrapper) DeleteOne(ctx context.Context, filter interface{},
	opts ...*options.DeleteOptions) (has bool, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	return c.deleteOne(ctx, filter, opts...)
}

func (c *collectionWrapper) DeleteID(ctx context.Context, ID interface{},
	opts ...*options.DeleteOptions) (has bool, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	filter := bson.M{"_id": ID}
	return c.deleteOne(ctx, filter, opts...)
}

func (c *collectionWrapper) DeleteMany(ctx context.Context, filter interface{},
	opts ...*options.DeleteOptions) (deletedCnt int64, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "DeleteMany")
	defer span.End()

	result, err := conn.Database(c.database).Collection(c.collection).DeleteMany(ctx, filter, opts...)
	if err != nil {
		return
	}
	deletedCnt = result.DeletedCount
	return
}

func (c *collectionWrapper) Distinct(ctx context.Context, filedName string, filter interface{},
	opts ...*options.DistinctOptions) (result []interface{}, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "Distinct")
	defer span.End()

	result, err = conn.Database(c.database).Collection(c.collection).Distinct(ctx, filedName, filter, opts...)
	if err != nil {
		return
	}
	return
}

func (c *collectionWrapper) Aggregate(ctx context.Context, pipeline, result interface{},
	opts ...*options.AggregateOptions) (err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "Aggregate")
	defer span.End()

	cursor, err := conn.Database(c.database).Collection(c.collection).Aggregate(ctx, pipeline, opts...)
	if err != nil {
		return
	}
	err = c.client.ScanCursor(ctx, cursor, result)
	if err != nil {
		return
	}
	return
}

func (c *collectionWrapper) UseSession(ctx context.Context, fn func(mongo.SessionContext) error,
	opts ...*options.SessionOptions) (err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "UseSession")
	defer span.End()

	err = conn.UseSessionWithOptions(ctx, options.MergeSessionOptions(opts...), fn)
	if err != nil {
		return
	}
	return
}

func (c *collectionWrapper) BulkWrite(ctx context.Context, models []mongo.WriteModel,
	opts ...*options.BulkWriteOptions) (result *mongo.BulkWriteResult, err error) {

	metric := c.startMetric()
	defer func() {
		c.endMetric(metric, err)
	}()

	conn := c.client.Client()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, c.client.Timeout())
	defer cancel()
	ctx, span := traceMongo(ctx, c.database, c.collection, "BulkWrite")
	defer span.End()

	oriResult, err := conn.Database(c.database).Collection(c.collection).BulkWrite(ctx, models, options.MergeBulkWriteOptions(opts...))
	if err != nil {
		return
	}

	result = &mongo.BulkWriteResult{
		InsertedCount: oriResult.InsertedCount,
		MatchedCount:  oriResult.MatchedCount,
		ModifiedCount: oriResult.ModifiedCount,
		DeletedCount:  oriResult.DeletedCount,
		UpsertedCount: oriResult.UpsertedCount,
		UpsertedIDs:   oriResult.UpsertedIDs,
	}
	return
}

type commandMetricInfo struct {
	st         time.Time
	callerName string
}

func (c *collectionWrapper) startMetric() commandMetricInfo {
	pc := make([]uintptr, 2)
	runtime.Callers(2, pc)
	caller := runtime.FuncForPC(pc[0])
	if caller == nil {
		return commandMetricInfo{}
	}

	callerName := caller.Name()
	if idx := strings.LastIndex(callerName, "."); idx > 0 {
		callerName = callerName[idx+1:]
	}
	return commandMetricInfo{
		st:         time.Now(),
		callerName: callerName,
	}
}

func (c *collectionWrapper) endMetric(info commandMetricInfo, err error) {
	if info == (commandMetricInfo{}) {
		return
	}
	labels := prometheus.Labels{
		"target":     c.client.metricTarget,
		"command":    info.callerName,
		"db":         c.client.convertMetricsLabel(c.database),
		"collection": c.client.convertMetricsLabel(c.collection),
	}
	if err != nil {
		metricError.With(labels).Add(1)
	}
	metricLatency.With(labels).Observe(float64(time.Since(info.st).Milliseconds()))
}

func traceMongo(parent context.Context, db, collection, command string) (ctx context.Context, span trace.Span) {
	tr := otel.GetTracerProvider().Tracer("mongo")
	ctx, span = tr.Start(parent, "db|"+db+"."+collection)
	span.SetAttributes(attribute.String("command", command))
	return
}
