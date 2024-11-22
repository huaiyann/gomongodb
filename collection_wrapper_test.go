package gomongodb

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var dbColForTest string = "collection_wrapper_test"
var colWrapperForTest *collectionWrapper
var officialClient *Client

type testDataSt struct {
	Likes int64   `bson:"likes"`
	Score float64 `bson:"score"`
}

var testDataGroup1 = []interface{}{
	testDataSt{Likes: 3, Score: 0.8},
	testDataSt{Likes: 4, Score: 0.7},
	testDataSt{Likes: 1, Score: 1},
	testDataSt{Likes: 2, Score: 0.9},
	testDataSt{Likes: 5, Score: 0.6},
}

func genDefaultWrapper(t *testing.T) {
	if colWrapperForTest != nil {
		return
	}
	var err error
	officialClient, err = InitClient(Config{
		Hostport: "mongodb://127.0.0.1:27017",
		Poolsize: 1,
	})
	if err != nil {
		t.Error(err)
	}
	colWrapperForTest = &collectionWrapper{
		client:     officialClient,
		database:   dbColForTest,
		collection: dbColForTest,
	}
}

func resetTestData(t *testing.T, data []interface{}) {
	_, err := colWrapperForTest.client.Client().Database(dbColForTest).Collection(dbColForTest).DeleteMany(context.Background(), bson.M{})
	if err != nil {
		t.Error(err)
	}
	_, err = colWrapperForTest.client.Client().Database(dbColForTest).Collection(dbColForTest).InsertMany(context.Background(), data)
	if err != nil {
		t.Error(err)
	}
}

func Test_collectionWrapperOfficial_GenSortBson(t *testing.T) {

	type args struct {
		sort []string
	}
	tests := []struct {
		name       string
		args       args
		wantResult bson.D
	}{
		{
			name: "sorts",
			args: args{
				sort: []string{"_id", "-score", "+likes"},
			},
			wantResult: bson.D{
				{
					Key:   "_id",
					Value: 1,
				},
				{
					Key:   "score",
					Value: -1,
				},
				{
					Key:   "likes",
					Value: 1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &collectionWrapper{}
			if gotResult := c.GenSortBson(tt.args.sort); !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("collectionWrapperOfficial.GenSortBson() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func Test_collectionWrapperOfficial_FindCursor(t *testing.T) {
	type args struct {
		ctx    context.Context
		filter interface{}
		sort   []string
		skip   int64
		limit  int64
		opts   []*options.FindOptions
	}
	genDefaultWrapper(t)
	resetTestData(t, testDataGroup1)

	find := func(ctx context.Context, filter interface{}, sort []string, skip, limit int64,
		opts ...*options.FindOptions) (result []testDataSt, err error) {
		c := colWrapperForTest
		cursor, err := c.FindCursor(ctx, filter, sort, skip, limit, opts...)
		if err != nil {
			return nil, err
		}
		err = c.client.ScanCursor(ctx, cursor, &result)
		if err != nil {
			return nil, err
		}
		return
	}

	tests := []struct {
		name          string
		args          args
		wantErr       bool
		wantResult    []testDataSt
		wantResultLen int
	}{
		{
			name: "test_find_no_sort_no_skip_no_limit",
			args: args{
				ctx:    nil,
				filter: bson.M{},
			},
			wantErr:       false,
			wantResultLen: len(testDataGroup1),
		},
		{
			name: "test_find_nothing",
			args: args{
				ctx:    nil,
				filter: bson.M{"nothing": 1},
			},
			wantErr:       false,
			wantResultLen: 0,
		},
		{
			name: "test_find_duplicate_sort_no_skip_no_limit",
			args: args{
				ctx:    nil,
				filter: bson.M{},
				sort:   []string{"likes"},
				opts: []*options.FindOptions{
					options.Find().SetSort(colWrapperForTest.GenSortBson([]string{"-likes"})),
				},
			},
			wantErr:       false,
			wantResultLen: len(testDataGroup1),
			wantResult: []testDataSt{
				{Likes: 1, Score: 1},
				{Likes: 2, Score: 0.9},
				{Likes: 3, Score: 0.8},
				{Likes: 4, Score: 0.7},
				{Likes: 5, Score: 0.6},
			},
		},
		{
			name: "test_find_sort_skip_limit",
			args: args{
				ctx:    nil,
				filter: bson.M{},
				sort:   []string{"likes"},
				skip:   1,
				limit:  2,
				opts: []*options.FindOptions{
					options.Find().SetSkip(3),
				},
			},
			wantErr:       false,
			wantResultLen: 2,
			wantResult: []testDataSt{
				{Likes: 2, Score: 0.9},
				{Likes: 3, Score: 0.8},
			},
		},
		{
			name: "test_find_opt_projection",
			args: args{
				ctx:    nil,
				filter: bson.M{},
				sort:   []string{"likes"},
				skip:   1,
				opts: []*options.FindOptions{
					options.Find().SetProjection(bson.M{"likes": 1}),
				},
			},
			wantErr:       false,
			wantResultLen: 4,
			wantResult: []testDataSt{
				{Likes: 2},
				{Likes: 3},
				{Likes: 4},
				{Likes: 5},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := find(tt.args.ctx, tt.args.filter, tt.args.sort, tt.args.skip, tt.args.limit, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindCursor() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, result) {
				t.Errorf("collectionWrapperOfficial.FindCursor() wantResult %+v, but get %+v", tt.wantResult, result)
			}
			if len(result) != tt.wantResultLen {
				t.Errorf("collectionWrapperOfficial.FindCursor() wantResultLen %d, but get %d", tt.wantResultLen, len(result))
			}
		})
	}
}

func Test_collectionWrapperOfficial_Find(t *testing.T) {
	type args struct {
		ctx    context.Context
		filter interface{}
		result *[]testDataSt
		sort   []string
		skip   int64
		limit  int64
		opts   []*options.FindOptions
	}
	genDefaultWrapper(t)
	resetTestData(t, testDataGroup1)

	tests := []struct {
		name          string
		args          args
		wantErr       bool
		wantResult    *[]testDataSt
		wantResultLen int
	}{
		{
			name: "test_find_no_sort_no_skip_no_limit",
			args: args{
				ctx:    nil,
				filter: bson.M{},
				result: &[]testDataSt{},
			},
			wantErr:       false,
			wantResultLen: len(testDataGroup1),
		},
		{
			name: "test_find_nothing",
			args: args{
				ctx:    nil,
				filter: bson.M{"nothing": 1},
				result: &[]testDataSt{},
			},
			wantErr:       false,
			wantResult:    &[]testDataSt{},
			wantResultLen: 0,
		},
		{
			name: "test_find_duplicate_sort_no_skip_no_limit",
			args: args{
				ctx:    nil,
				filter: bson.M{},
				result: &[]testDataSt{},
				sort:   []string{"likes"},
				opts: []*options.FindOptions{
					options.Find().SetSort(colWrapperForTest.GenSortBson([]string{"-likes"})),
				},
			},
			wantErr:       false,
			wantResultLen: len(testDataGroup1),
			wantResult: &[]testDataSt{
				{Likes: 1, Score: 1},
				{Likes: 2, Score: 0.9},
				{Likes: 3, Score: 0.8},
				{Likes: 4, Score: 0.7},
				{Likes: 5, Score: 0.6},
			},
		},
		{
			name: "test_find_sort_skip_limit",
			args: args{
				ctx:    nil,
				filter: bson.M{},
				result: &[]testDataSt{},
				sort:   []string{"likes"},
				skip:   1,
				limit:  2,
				opts: []*options.FindOptions{
					options.Find().SetSkip(3),
				},
			},
			wantErr:       false,
			wantResultLen: 2,
			wantResult: &[]testDataSt{
				{Likes: 2, Score: 0.9},
				{Likes: 3, Score: 0.8},
			},
		},
		{
			name: "test_find_opt_projection",
			args: args{
				ctx:    nil,
				filter: bson.M{},
				result: &[]testDataSt{},
				sort:   []string{"likes"},
				skip:   1,
				opts: []*options.FindOptions{
					options.Find().SetProjection(bson.M{"likes": 1}),
				},
			},
			wantErr:       false,
			wantResultLen: 4,
			wantResult: &[]testDataSt{
				{Likes: 2},
				{Likes: 3},
				{Likes: 4},
				{Likes: 5},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			if err := c.Find(tt.args.ctx, tt.args.filter, tt.args.result, tt.args.sort, tt.args.skip, tt.args.limit, tt.args.opts...); (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.Find() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, tt.args.result) {
				t.Errorf("collectionWrapperOfficial.Find() wantResult %+v, but get %+v", tt.wantResult, tt.args.result)
			}
			if len(*tt.args.result) != tt.wantResultLen {
				t.Errorf("collectionWrapperOfficial.Find() wantResultLen %d, but get %d", tt.wantResultLen, len(*tt.args.result))
			}
		})
	}
}

func Test_collectionWrapperOfficial_FindOne(t *testing.T) {
	genDefaultWrapper(t)
	resetTestData(t, testDataGroup1)

	type args struct {
		ctx    context.Context
		filter interface{}
		result interface{}
		sort   []string
		skip   int64
		opts   []*options.FindOneOptions
	}
	tests := []struct {
		name       string
		args       args
		wantHas    bool
		wantErr    bool
		wantResult *testDataSt
	}{
		{
			name: "find_one_sort",
			args: args{
				filter: bson.M{"likes": bson.M{"$gt": 1}},
				result: new(testDataSt),
				sort:   []string{"likes"},
				skip:   1,
				opts: []*options.FindOneOptions{
					options.FindOne().SetProjection(bson.M{"likes": 1}),
				},
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataSt{Likes: 3},
		},
		{
			name: "find_one_sort_miss",
			args: args{
				filter: bson.M{"likes": 1},
				result: new(testDataSt),
				sort:   []string{"likes"},
				skip:   1,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: &testDataSt{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			gotHas, err := c.FindOne(tt.args.ctx, tt.args.filter, tt.args.result, tt.args.sort, tt.args.skip, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOne() = %v, want %v", gotHas, tt.wantHas)
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, tt.args.result) {
				t.Errorf("collectionWrapperOfficial.FindOne() wantResult %+v, but get %+v", tt.wantResult, tt.args.result)
			}
		})
	}
}

func Test_collectionWrapperOfficial_FindID(t *testing.T) {
	genDefaultWrapper(t)
	resetTestData(t, testDataGroup1)
	item := &struct {
		ID string `bson:"_id"`
	}{}
	has, err := colWrapperForTest.FindOne(context.Background(), bson.M{"likes": 2}, item, nil, 0)
	if err != nil {
		t.Error(err)
	}
	if !has {
		t.Errorf("get id fail")
	}
	ID, err := primitive.ObjectIDFromHex(item.ID)
	if err != nil {
		t.Error(err)
	}
	type args struct {
		ctx    context.Context
		ID     interface{}
		result interface{}
		opts   []*options.FindOneOptions
	}
	tests := []struct {
		name       string
		args       args
		wantHas    bool
		wantErr    bool
		wantResult *testDataSt
	}{
		{
			name: "find_id",
			args: args{
				result: new(testDataSt),
				ID:     ID,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataSt{Likes: 2, Score: 0.9},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			gotHas, err := c.FindID(tt.args.ctx, tt.args.ID, tt.args.result, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindID() = %v, want %v", gotHas, tt.wantHas)
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, tt.args.result) {
				t.Errorf("collectionWrapperOfficial.FindID() wantResult %+v, but get %+v", tt.wantResult, tt.args.result)
			}
		})
	}
}

func Test_collectionWrapperOfficial_FindOneAndUpdate(t *testing.T) {
	genDefaultWrapper(t)
	resetTestData(t, testDataGroup1)
	type args struct {
		ctx       context.Context
		filter    interface{}
		update    interface{}
		result    interface{}
		sort      []string
		upsert    bool
		returnNew bool
		opts      []*options.FindOneAndUpdateOptions
	}
	tests := []struct {
		name       string
		args       args
		wantHas    bool
		wantErr    bool
		wantResult *testDataSt
	}{
		{
			name: "find_ond_and_update_return_old",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 1}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				result:    new(testDataSt),
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataSt{Likes: 5, Score: 0.6},
		},
		{
			name: "find_ond_and_update_return_new",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 1}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				result:    new(testDataSt),
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataSt{Likes: 5, Score: 66.6},
		},
		{
			name: "find_ond_and_update_upsert_return_old",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 10}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				result:    new(testDataSt),
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: false,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: &testDataSt{},
		},
		{
			name: "find_ond_and_update_upsert_return_new",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 10}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				result:    new(testDataSt),
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataSt{Score: 66.6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			gotHas, err := c.FindOneAndUpdate(tt.args.ctx, tt.args.filter, tt.args.update, tt.args.result, tt.args.sort, tt.args.upsert, tt.args.returnNew, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOneAndUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOneAndUpdate() = %v, want %v", gotHas, tt.wantHas)
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, tt.args.result) {
				t.Errorf("collectionWrapperOfficial.FindOneAndUpdate() wantResult %+v, but get %+v", tt.wantResult, tt.args.result)
			}
		})
	}
}

func Test_collectionWrapperOfficial_FindOneAndReplace(t *testing.T) {
	genDefaultWrapper(t)
	type args struct {
		ctx         context.Context
		filter      interface{}
		replacement interface{}
		result      interface{}
		sort        []string
		upsert      bool
		returnNew   bool
		opts        []*options.FindOneAndReplaceOptions
	}
	tests := []struct {
		name       string
		args       args
		wantHas    bool
		wantErr    bool
		wantResult *testDataSt
	}{
		{
			name: "find_one_and_replace_upsert_false_return_new_has",
			args: args{
				filter:      bson.M{"likes": 1},
				replacement: &testDataSt{Likes: 8, Score: 2.1},
				result:      new(testDataSt),
				upsert:      false,
				returnNew:   true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataSt{Likes: 8, Score: 2.1},
		},
		{
			name: "find_one_and_replace_upsert_false_return_old_has",
			args: args{
				filter:      bson.M{"likes": 2},
				replacement: &testDataSt{Likes: 8, Score: 2.1},
				result:      new(testDataSt),
				upsert:      false,
				returnNew:   false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataSt{Likes: 2, Score: 0.9},
		},
		{
			name: "find_one_and_replace_upsert_false_return_new_miss",
			args: args{
				filter:      bson.M{"likes": 10},
				replacement: &testDataSt{Likes: 8, Score: 2.1},
				result:      new(testDataSt),
				upsert:      false,
				returnNew:   true,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: &testDataSt{},
		},
		{
			name: "find_one_and_replace_upsert_false_return_old_miss",
			args: args{
				filter:      bson.M{"likes": 10},
				replacement: &testDataSt{Likes: 8, Score: 2.1},
				result:      new(testDataSt),
				upsert:      false,
				returnNew:   false,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: &testDataSt{},
		},
		{
			name: "find_one_and_replace_upsert_true_return_old_miss",
			args: args{
				filter:      bson.M{"likes": 30},
				replacement: &testDataSt{Likes: 8, Score: 2.1},
				result:      new(testDataSt),
				upsert:      true,
				returnNew:   false,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: &testDataSt{},
		},
		{
			name: "find_one_and_replace_upsert_true_return_old_has",
			args: args{
				filter:      bson.M{"likes": 3},
				replacement: &testDataSt{Likes: 3, Score: 0.81},
				result:      new(testDataSt),
				upsert:      true,
				returnNew:   false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataSt{Likes: 3, Score: 0.8},
		},
		{
			name: "find_one_and_replace_upsert_true_return_new_miss",
			args: args{
				filter:      bson.M{"likes": 30},
				replacement: &testDataSt{Likes: 8, Score: 2.1},
				result:      new(testDataSt),
				upsert:      true,
				returnNew:   true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataSt{Likes: 8, Score: 2.1},
		},
		{
			name: "find_one_and_replace_upsert_true_return_new_has",
			args: args{
				filter:      bson.M{"likes": 3},
				replacement: &testDataSt{Likes: 8, Score: 2.2},
				result:      new(testDataSt),
				upsert:      true,
				returnNew:   true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataSt{Likes: 8, Score: 2.2},
		},
	}
	for _, tt := range tests {
		resetTestData(t, testDataGroup1)
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			gotHas, err := c.FindOneAndReplace(tt.args.ctx, tt.args.filter, tt.args.replacement, tt.args.result, tt.args.sort, tt.args.upsert, tt.args.returnNew, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOneAndReplace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOneAndReplace() = %v, want %v", gotHas, tt.wantHas)
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, tt.args.result) {
				t.Errorf("collectionWrapperOfficial.FindOneAndReplace() wantResult %+v, but get %+v", tt.wantResult, tt.args.result)
			}
		})
	}
}

func Test_collectionWrapperOfficial_FindOneAndDelete(t *testing.T) {
	genDefaultWrapper(t)
	type args struct {
		ctx    context.Context
		filter interface{}
		result interface{}
		sort   []string
		opts   []*options.FindOneAndDeleteOptions
	}
	tests := []struct {
		name       string
		args       args
		wantHas    bool
		wantErr    bool
		wantResult *testDataSt
	}{
		{
			name: "find_one_and_delete_miss",
			args: args{
				filter: bson.M{"likes": 10},
				result: new(testDataSt),
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: new(testDataSt),
		},
		{
			name: "find_one_and_delete_has",
			args: args{
				filter: bson.M{"likes": bson.M{"$gte": 1}},
				result: new(testDataSt),
				sort:   []string{"-likes"},
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataSt{Likes: 5, Score: 0.6},
		},
	}
	for _, tt := range tests {
		resetTestData(t, testDataGroup1)
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			gotHas, err := c.FindOneAndDelete(tt.args.ctx, tt.args.filter, tt.args.result, tt.args.sort, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOneAndDelete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOneAndDelete() = %v, want %v", gotHas, tt.wantHas)
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, tt.args.result) {
				t.Errorf("collectionWrapperOfficial.FindOneAndDelete() wantResult %+v, but get %+v", tt.wantResult, tt.args.result)
			}
		})
	}
}

func Test_collectionWrapperOfficial_InsertOne(t *testing.T) {
	genDefaultWrapper(t)
	type args struct {
		ctx      context.Context
		document interface{}
		opts     []*options.InsertOneOptions
	}
	ID := primitive.NewObjectIDFromTimestamp(time.Now())
	tests := []struct {
		name           string
		args           args
		wantInsertedID interface{}
		wantErr        bool
	}{
		{
			name: "insert_one",
			args: args{
				document: map[string]interface{}{
					"_id": ID,
				},
			},
			wantInsertedID: ID,
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			gotInsertedID, err := c.InsertOne(tt.args.ctx, tt.args.document, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.InsertOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotInsertedID, tt.wantInsertedID) {
				t.Errorf("collectionWrapperOfficial.InsertOne() = %v, want %v", gotInsertedID, tt.wantInsertedID)
			}
		})
	}
}

func Test_collectionWrapperOfficial_InsertMany(t *testing.T) {
	genDefaultWrapper(t)
	ID1 := primitive.NewObjectIDFromTimestamp(time.Now())
	ID2 := primitive.NewObjectIDFromTimestamp(time.Now())
	type args struct {
		ctx      context.Context
		document []interface{}
		opts     []*options.InsertManyOptions
	}
	tests := []struct {
		name            string
		args            args
		wantInsertedIDs []interface{}
		wantErr         bool
	}{
		{
			name: "insert_many",
			args: args{
				document: []interface{}{
					map[string]interface{}{
						"_id": ID1,
					},
					map[string]interface{}{
						"_id": ID2,
					},
				},
			},
			wantInsertedIDs: []interface{}{ID1, ID2},
			wantErr:         false,
		},
		{
			name: "insert_many_dup",
			args: args{
				document: []interface{}{
					map[string]interface{}{
						"_id": ID1,
					},
					map[string]interface{}{
						"_id": ID2,
					},
				},
			},
			wantInsertedIDs: nil,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			gotInsertedIDs, err := c.InsertMany(tt.args.ctx, tt.args.document, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.InsertMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotInsertedIDs, tt.wantInsertedIDs) {
				t.Errorf("collectionWrapperOfficial.InsertMany() = %v, want %v", gotInsertedIDs, tt.wantInsertedIDs)
			}
		})
	}
}

func Test_collectionWrapperOfficial_UpdateOne(t *testing.T) {
	genDefaultWrapper(t)
	resetTestData(t, testDataGroup1)
	type args struct {
		ctx    context.Context
		filter interface{}
		update interface{}
		upsert bool
		opts   []*options.UpdateOptions
	}
	ID := primitive.NewObjectID()
	tests := []struct {
		name            string
		args            args
		wantMatchedCnt  int64
		wantModifiedCnt int64
		wantUpsertedCnt int64
		wantUpsertedID  interface{}
		wantErr         bool
	}{
		{
			name: "update_one_upsert_false_hit",
			args: args{
				filter: bson.M{"likes": 1},
				update: bson.M{"$set": bson.M{"ext": 1}},
				upsert: false,
			},
			wantMatchedCnt:  1,
			wantModifiedCnt: 1,
			wantUpsertedCnt: 0,
			wantUpsertedID:  nil,
			wantErr:         false,
		},
		{
			name: "update_one_upsert_false_hit_no_modify",
			args: args{
				filter: bson.M{"likes": 1},
				update: bson.M{"$set": bson.M{"score": float64(1)}},
				upsert: false,
			},
			wantMatchedCnt:  1,
			wantModifiedCnt: 0,
			wantUpsertedCnt: 0,
			wantUpsertedID:  nil,
			wantErr:         false,
		},
		{
			name: "update_one_upsert_false_miss",
			args: args{
				filter: bson.M{"likes": 10},
				update: bson.M{"$set": bson.M{"ext": 1}},
				upsert: false,
			},
			wantMatchedCnt:  0,
			wantModifiedCnt: 0,
			wantUpsertedCnt: 0,
			wantUpsertedID:  nil,
			wantErr:         false,
		},
		{
			name: "update_one_upsert_true_hit",
			args: args{
				filter: bson.M{"likes": 1},
				update: bson.M{"$set": bson.M{"ext": 2}},
				upsert: true,
			},
			wantMatchedCnt:  1,
			wantModifiedCnt: 1,
			wantUpsertedCnt: 0,
			wantUpsertedID:  nil,
			wantErr:         false,
		},
		{
			name: "update_one_upsert_true_miss",
			args: args{
				filter: bson.M{"_id": ID, "likes": 10},
				update: bson.M{"$set": bson.M{"ext": 1}},
				upsert: true,
			},
			wantMatchedCnt:  0,
			wantModifiedCnt: 0,
			wantUpsertedCnt: 1,
			wantUpsertedID:  ID,
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			result, err := c.UpdateOne(tt.args.ctx, tt.args.filter, tt.args.update, tt.args.upsert, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.UpdateOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result.MatchedCount != tt.wantMatchedCnt {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotMatchedCnt = %v, want %v", result.MatchedCount, tt.wantMatchedCnt)
			}
			if result.ModifiedCount != tt.wantModifiedCnt {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotModifiedCnt = %v, want %v", result.ModifiedCount, tt.wantModifiedCnt)
			}
			if result.UpsertedCount != tt.wantUpsertedCnt {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotUpsertedCnt = %v, want %v", result.UpsertedCount, tt.wantUpsertedCnt)
			}
			if !reflect.DeepEqual(result.UpsertedID, tt.wantUpsertedID) {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotUpsertedID = %v, want %v", result.UpsertedID, tt.wantUpsertedID)
			}
		})
	}
}

func Test_collectionWrapperOfficial_UpdateID(t *testing.T) {
	genDefaultWrapper(t)
	resetTestData(t, testDataGroup1)
	type args struct {
		ctx    context.Context
		ID     interface{}
		update interface{}
		upsert bool
		opts   []*options.UpdateOptions
	}
	ID := primitive.NewObjectID()
	tests := []struct {
		name            string
		args            args
		wantMatchedCnt  int64
		wantModifiedCnt int64
		wantUpsertedCnt int64
		wantUpsertedID  interface{}
		wantErr         bool
	}{
		{
			name: "update_one_insert",
			args: args{
				ID: ID,
				update: bson.M{
					"$set": bson.M{"ext": 1},
				},
				upsert: true,
			},
			wantMatchedCnt:  0,
			wantModifiedCnt: 0,
			wantUpsertedCnt: 1,
			wantUpsertedID:  ID,
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			result, err := c.UpdateID(tt.args.ctx, tt.args.ID, tt.args.update, tt.args.upsert, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.UpdateID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result.MatchedCount != tt.wantMatchedCnt {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotMatchedCnt = %v, want %v", result.MatchedCount, tt.wantMatchedCnt)
			}
			if result.ModifiedCount != tt.wantModifiedCnt {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotModifiedCnt = %v, want %v", result.ModifiedCount, tt.wantModifiedCnt)
			}
			if result.UpsertedCount != tt.wantUpsertedCnt {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotUpsertedCnt = %v, want %v", result.UpsertedCount, tt.wantUpsertedCnt)
			}
			if !reflect.DeepEqual(result.UpsertedID, tt.wantUpsertedID) {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotUpsertedID = %v, want %v", result.UpsertedID, tt.wantUpsertedID)
			}
		})
	}
}

func Test_collectionWrapperOfficial_UpdateMany(t *testing.T) {
	genDefaultWrapper(t)
	type args struct {
		ctx    context.Context
		filter interface{}
		update interface{}
		upsert bool
		opts   []*options.UpdateOptions
	}
	ID := primitive.NewObjectID()
	tests := []struct {
		name            string
		args            args
		wantMatchedCnt  int64
		wantModifiedCnt int64
		wantUpsertedCnt int64
		wantUpsertedID  interface{}
		wantErr         bool
	}{
		{
			name: "update_many_upsert_false_miss",
			args: args{
				filter: bson.M{"likes": 10},
				update: bson.M{"$set": bson.M{"score": 1.1}},
				upsert: false,
			},
			wantMatchedCnt:  0,
			wantModifiedCnt: 0,
			wantUpsertedCnt: 0,
			wantUpsertedID:  nil,
			wantErr:         false,
		},
		{
			name: "update_many_upsert_false_hit",
			args: args{
				filter: bson.M{"likes": bson.M{"$gte": 4}},
				update: bson.M{"$set": bson.M{"score": 1.1}},
				upsert: false,
			},
			wantMatchedCnt:  2,
			wantModifiedCnt: 2,
			wantUpsertedCnt: 0,
			wantUpsertedID:  nil,
			wantErr:         false,
		},
		{
			name: "update_many_upsert_true_miss",
			args: args{
				filter: bson.M{"_id": ID, "likes": 10},
				update: bson.M{"$set": bson.M{"score": 1.1}},
				upsert: true,
			},
			wantMatchedCnt:  0,
			wantModifiedCnt: 0,
			wantUpsertedCnt: 1,
			wantUpsertedID:  ID,
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		resetTestData(t, testDataGroup1)
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			result, err := c.UpdateMany(tt.args.ctx, tt.args.filter, tt.args.update, tt.args.upsert, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.UpdateMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if result.MatchedCount != tt.wantMatchedCnt {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotMatchedCnt = %v, want %v", result.MatchedCount, tt.wantMatchedCnt)
			}
			if result.ModifiedCount != tt.wantModifiedCnt {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotModifiedCnt = %v, want %v", result.ModifiedCount, tt.wantModifiedCnt)
			}
			if result.UpsertedCount != tt.wantUpsertedCnt {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotUpsertedCnt = %v, want %v", result.UpsertedCount, tt.wantUpsertedCnt)
			}
			if !reflect.DeepEqual(result.UpsertedID, tt.wantUpsertedID) {
				t.Errorf("collectionWrapperOfficial.UpdateOne() gotUpsertedID = %v, want %v", result.UpsertedID, tt.wantUpsertedID)
			}
		})
	}
}

func Test_collectionWrapperOfficial_Count(t *testing.T) {
	genDefaultWrapper(t)
	resetTestData(t, testDataGroup1)
	type args struct {
		ctx    context.Context
		filter interface{}
		skip   int64
		limit  int64
		opts   []*options.CountOptions
	}
	tests := []struct {
		name      string
		args      args
		wantCount int64
		wantErr   bool
	}{
		{
			name: "count_normal",
			args: args{
				filter: bson.M{"likes": bson.M{"$gte": 2}},
				skip:   0,
				limit:  0,
			},
			wantCount: 4,
			wantErr:   false,
		},
		{
			name: "count_skip",
			args: args{
				filter: bson.M{"likes": bson.M{"$gte": 2}},
				skip:   1,
				limit:  0,
			},
			wantCount: 3,
			wantErr:   false,
		},
		{
			name: "count_limit",
			args: args{
				filter: bson.M{"likes": bson.M{"$gte": 2}},
				skip:   1,
				limit:  2,
			},
			wantCount: 2,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			gotCount, err := c.Count(tt.args.ctx, tt.args.filter, tt.args.skip, tt.args.limit, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.Count() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotCount != tt.wantCount {
				t.Errorf("collectionWrapperOfficial.Count() = %v, want %v", gotCount, tt.wantCount)
			}
		})
	}
}

func Test_collectionWrapperOfficial_DeleteOne(t *testing.T) {
	genDefaultWrapper(t)
	type args struct {
		ctx    context.Context
		filter interface{}
		opts   []*options.DeleteOptions
	}
	tests := []struct {
		name    string
		args    args
		wantHas bool
		wantErr bool
	}{
		{
			name: "delete_one_hit",
			args: args{
				filter: bson.M{"likes": 1},
			},
			wantHas: true,
			wantErr: false,
		},
		{
			name: "delete_one_miss",
			args: args{
				filter: bson.M{"likes": 10},
			},
			wantHas: false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		resetTestData(t, testDataGroup1)
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			gotHas, err := c.DeleteOne(tt.args.ctx, tt.args.filter, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.DeleteOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.DeleteOne() = %v, want %v", gotHas, tt.wantHas)
			}
		})
	}
}

func Test_collectionWrapperOfficial_DeleteID(t *testing.T) {
	genDefaultWrapper(t)
	resetTestData(t, testDataGroup1)
	type args struct {
		ctx  context.Context
		ID   interface{}
		opts []*options.DeleteOptions
	}
	tests := []struct {
		name    string
		args    args
		wantHas bool
		wantErr bool
	}{
		{
			name: "delete_id",
			args: args{
				ID: func() primitive.ObjectID {
					item := struct {
						ID primitive.ObjectID `bson:"_id"`
					}{}
					has, err := colWrapperForTest.FindOne(context.Background(), bson.M{"likes": 1}, &item, nil, 0)
					if err != nil {
						t.Error(err)
					}
					if !has {
						t.Errorf("get id fail")
					}
					return item.ID
				}(),
			},
			wantHas: true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			gotHas, err := c.DeleteID(tt.args.ctx, tt.args.ID, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.DeleteID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.DeleteID() = %v, want %v", gotHas, tt.wantHas)
			}
		})
	}
}

func Test_collectionWrapperOfficial_DeleteMany(t *testing.T) {
	genDefaultWrapper(t)
	type args struct {
		ctx    context.Context
		filter interface{}
		opts   []*options.DeleteOptions
	}
	tests := []struct {
		name           string
		args           args
		wantDeletedCnt int64
		wantErr        bool
	}{
		{
			name: "delete_many_miss",
			args: args{
				filter: bson.M{"likes": 0},
			},
			wantDeletedCnt: 0,
			wantErr:        false,
		},
		{
			name: "delete_many_hit",
			args: args{
				filter: bson.M{"likes": bson.M{"$gte": 2}},
			},
			wantDeletedCnt: 4,
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetTestData(t, testDataGroup1)
			c := colWrapperForTest
			gotDeletedCnt, err := c.DeleteMany(tt.args.ctx, tt.args.filter, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.DeleteMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotDeletedCnt != tt.wantDeletedCnt {
				t.Errorf("collectionWrapperOfficial.DeleteMany() = %v, want %v", gotDeletedCnt, tt.wantDeletedCnt)
			}
		})
	}
}

func Test_collectionWrapperOfficial_Distinct(t *testing.T) {
	genDefaultWrapper(t)
	resetTestData(t, testDataGroup1)
	type args struct {
		ctx       context.Context
		filedName string
		filter    interface{}
		opts      []*options.DistinctOptions
	}
	tests := []struct {
		name       string
		args       args
		wantResult []interface{}
		wantErr    bool
	}{
		{
			name: "distinct",
			args: args{
				filedName: "likes",
				filter:    bson.M{"likes": bson.M{"$gte": 2}},
			},
			wantResult: []interface{}{2, 3, 4, 5},
			wantErr:    false,
		},
	}
	sortMethod := func(data []interface{}) (result []string) {
		for _, v := range data {
			result = append(result, fmt.Sprintf("%v", v))
		}
		sort.Strings(result)
		return result
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			gotResult, err := c.Distinct(tt.args.ctx, tt.args.filedName, tt.args.filter, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.Distinct() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(sortMethod(gotResult), sortMethod(tt.wantResult)) {
				t.Errorf("collectionWrapperOfficial.Distinct() = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func Test_collectionWrapperOfficial_Aggregate(t *testing.T) {
	genDefaultWrapper(t)
	resetTestData(t, testDataGroup1)
	type TmpData struct {
		ID    string `bson:"_id"`
		Count int64  `bson:"count"`
		Min   int64  `bson:"min"`
	}
	type args struct {
		ctx      context.Context
		pipeline interface{}
		result   interface{}
		opts     []*options.AggregateOptions
	}
	tests := []struct {
		name       string
		args       args
		wantErr    bool
		wantResult *[]*TmpData
	}{
		{
			name: "aggregate",
			args: args{
				pipeline: []bson.M{
					{"$match": bson.M{"likes": bson.M{"$gte": 3}}},
					{"$group": bson.M{"_id": "666", "count": bson.M{"$sum": 1}, "min": bson.M{"$min": "$likes"}}},
				},
				result: &[]*TmpData{},
			},
			wantErr: false,
			wantResult: &[]*TmpData{
				{
					ID:    "666",
					Count: 3,
					Min:   3,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			if err := c.Aggregate(tt.args.ctx, tt.args.pipeline, tt.args.result, tt.args.opts...); (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.Aggregate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, tt.args.result) {
				t.Errorf("collectionWrapperOfficial.Aggregate() wantResult %+v, but get %+v", tt.wantResult, tt.args.result)
			}
		})
	}
}

func Test_collectionWrapperOfficial_BulkWrite(t *testing.T) {
	genDefaultWrapper(t)
	type args struct {
		ctx    context.Context
		models []mongo.WriteModel
		opts   []*options.BulkWriteOptions
	}
	tests := []struct {
		name       string
		args       args
		wantResult *mongo.BulkWriteResult
		wantErr    bool
	}{
		{
			name: "insert_upsert_delete",
			args: args{
				models: []mongo.WriteModel{
					mongo.NewInsertOneModel().SetDocument(bson.M{"likes": -1, "score": -2}),
					mongo.NewUpdateOneModel().SetFilter(bson.M{"_id": "tmp_id", "likes": -2}).SetUpdate(bson.M{"$set": bson.M{"score": -3}}).SetUpsert(true),
					mongo.NewUpdateManyModel().SetFilter(bson.M{"likes": bson.M{"$gte": 1}}).SetUpdate(bson.M{"$set": bson.M{"score": float64(1)}}),
					mongo.NewUpdateManyModel().SetFilter(bson.M{"likes": bson.M{"$gte": 1}}).SetUpdate(bson.M{"$set": bson.M{"score": float64(1)}}),
					mongo.NewDeleteManyModel().SetFilter(bson.M{"likes": bson.M{"$lt": 0}}),
				},
			},
			wantResult: &mongo.BulkWriteResult{
				InsertedCount: 1,
				MatchedCount:  10,
				ModifiedCount: 4,
				DeletedCount:  2,
				UpsertedCount: 1,
				UpsertedIDs: map[int64]interface{}{
					1: "tmp_id",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperForTest
			resetTestData(t, testDataGroup1)
			gotResult, err := c.BulkWrite(tt.args.ctx, tt.args.models, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.BulkWrite() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("collectionWrapperOfficial.BulkWrite() = %+v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}
