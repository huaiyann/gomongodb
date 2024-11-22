package gomongodb

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type testDataIDSt struct {
	ID    primitive.ObjectID `bson:"_id,omitempty"`
	Likes int64              `bson:"likes"`
	Score float64            `bson:"score"`
}

var colWrapperGenericForTest CollectionWrapperGeneric[testDataIDSt]
var colWrapperGenericForTestPtr CollectionWrapperGeneric[*testDataIDSt]

func genGenericWrapper(t *testing.T) {
	if colWrapperGenericForTest != nil {
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
	colWrapperGenericForTest = NewCollectionWrapper[testDataIDSt](officialClient, dbColForTest, dbColForTest)
	colWrapperGenericForTestPtr = NewCollectionWrapper[*testDataIDSt](officialClient, dbColForTest, dbColForTest)
}

func resetTestDataGeneric(t *testing.T, data []interface{}) {
	_, err := colWrapperGenericForTest.Collection().DeleteMany(context.Background(), bson.M{})
	if err != nil {
		t.Error(err)
	}
	_, err = colWrapperGenericForTest.Collection().InsertMany(context.Background(), data)
	if err != nil {
		t.Error(err)
	}
}

func Test_collectionWrapperGeneric_Find(t *testing.T) {
	type args struct {
		ctx    context.Context
		filter interface{}
		sort   []string
		skip   int64
		limit  int64
		opts   []*options.FindOptions
	}
	genGenericWrapper(t)
	resetTestDataGeneric(t, testDataGroup1)

	tests := []struct {
		name          string
		args          args
		wantErr       bool
		wantResult    []testDataIDSt
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
			wantResult:    nil,
			wantResultLen: 0,
		},
		{
			name: "test_find_duplicate_sort_no_skip_no_limit",
			args: args{
				ctx:    nil,
				filter: bson.M{},
				sort:   []string{"likes"},
				opts: []*options.FindOptions{
					options.Find().SetSort(colWrapperGenericForTest.GenSortBson([]string{"-likes"})),
				},
			},
			wantErr:       false,
			wantResultLen: len(testDataGroup1),
			wantResult: []testDataIDSt{
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
			wantResult: []testDataIDSt{
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
			wantResult: []testDataIDSt{
				{Likes: 2},
				{Likes: 3},
				{Likes: 4},
				{Likes: 5},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTest
			var err error
			gotResults, err := c.Find(tt.args.ctx, tt.args.filter, tt.args.sort, tt.args.skip, tt.args.limit, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.Find() error = %v, wantErr %v", err, tt.wantErr)
			}
			for i := range gotResults {
				gotResults[i].ID = primitive.NilObjectID
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, gotResults) {
				t.Errorf("collectionWrapperOfficial.Find() wantResult %+v, but get %+v", tt.wantResult, gotResults)
			}
			if len(gotResults) != tt.wantResultLen {
				t.Errorf("collectionWrapperOfficial.Find() wantResultLen %d, but get %d", tt.wantResultLen, len(gotResults))
			}
		})
	}
}

func Test_collectionWrapperGeneric_Ptr_Find(t *testing.T) {
	type args struct {
		ctx    context.Context
		filter interface{}
		sort   []string
		skip   int64
		limit  int64
		opts   []*options.FindOptions
	}
	genGenericWrapper(t)
	resetTestDataGeneric(t, testDataGroup1)

	tests := []struct {
		name          string
		args          args
		wantErr       bool
		wantResult    []*testDataIDSt
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
			wantResult:    nil,
			wantResultLen: 0,
		},
		{
			name: "test_find_duplicate_sort_no_skip_no_limit",
			args: args{
				ctx:    nil,
				filter: bson.M{},
				sort:   []string{"likes"},
				opts: []*options.FindOptions{
					options.Find().SetSort(colWrapperGenericForTest.GenSortBson([]string{"-likes"})),
				},
			},
			wantErr:       false,
			wantResultLen: len(testDataGroup1),
			wantResult: []*testDataIDSt{
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
			wantResult: []*testDataIDSt{
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
			wantResult: []*testDataIDSt{
				{Likes: 2},
				{Likes: 3},
				{Likes: 4},
				{Likes: 5},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTestPtr
			var err error
			gotResults, err := c.Find(tt.args.ctx, tt.args.filter, tt.args.sort, tt.args.skip, tt.args.limit, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.Find() error = %v, wantErr %v", err, tt.wantErr)
			}
			for i := range gotResults {
				gotResults[i].ID = primitive.NilObjectID
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, gotResults) {
				t.Errorf("collectionWrapperOfficial.Find() wantResult %+v, but get %+v", tt.wantResult, gotResults)
			}
			if len(gotResults) != tt.wantResultLen {
				t.Errorf("collectionWrapperOfficial.Find() wantResultLen %d, but get %d", tt.wantResultLen, len(gotResults))
			}
		})
	}
}

func Test_collectionWrapperGeneric_FindOne(t *testing.T) {
	genGenericWrapper(t)
	resetTestDataGeneric(t, testDataGroup1)

	type args struct {
		ctx    context.Context
		filter interface{}
		result testDataIDSt
		sort   []string
		skip   int64
		opts   []*options.FindOneOptions
	}
	tests := []struct {
		name       string
		args       args
		wantHas    bool
		wantErr    bool
		wantResult testDataIDSt
	}{
		{
			name: "find_one_sort",
			args: args{
				filter: bson.M{"likes": bson.M{"$gt": 1}},
				result: testDataIDSt{},
				sort:   []string{"likes"},
				skip:   1,
				opts: []*options.FindOneOptions{
					options.FindOne().SetProjection(bson.M{"likes": 1}),
				},
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: testDataIDSt{Likes: 3},
		},
		{
			name: "find_one_sort_miss",
			args: args{
				filter: bson.M{"likes": 1},
				result: testDataIDSt{},
				sort:   []string{"likes"},
				skip:   1,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: testDataIDSt{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTest
			var gotHas bool
			var err error
			tt.args.result, gotHas, err = c.FindOne(tt.args.ctx, tt.args.filter, tt.args.sort, tt.args.skip, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOne() = %v, want %v", gotHas, tt.wantHas)
			}
			tt.args.result.ID = primitive.NilObjectID
			if !reflect.DeepEqual(tt.wantResult, tt.args.result) {
				t.Errorf("collectionWrapperOfficial.FindOne() wantResult %+v, but get %+v", tt.wantResult, tt.args.result)
			}
		})
	}
}

func Test_collectionWrapperGeneric_Ptr_FindOne(t *testing.T) {
	genGenericWrapper(t)
	resetTestDataGeneric(t, testDataGroup1)

	type args struct {
		ctx    context.Context
		filter interface{}
		result *testDataIDSt
		sort   []string
		skip   int64
		opts   []*options.FindOneOptions
	}
	tests := []struct {
		name       string
		args       args
		wantHas    bool
		wantErr    bool
		wantResult *testDataIDSt
	}{
		{
			name: "find_one_sort",
			args: args{
				filter: bson.M{"likes": bson.M{"$gt": 1}},
				sort:   []string{"likes"},
				skip:   1,
				opts: []*options.FindOneOptions{
					options.FindOne().SetProjection(bson.M{"likes": 1}),
				},
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataIDSt{Likes: 3},
		},
		{
			name: "find_one_sort_miss",
			args: args{
				filter: bson.M{"likes": 1},
				sort:   []string{"likes"},
				skip:   1,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTestPtr
			var gotHas bool
			var err error
			tt.args.result, gotHas, err = c.FindOne(tt.args.ctx, tt.args.filter, tt.args.sort, tt.args.skip, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOne() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOne() = %v, want %v", gotHas, tt.wantHas)
			}
			if tt.args.result != nil {
				tt.args.result.ID = primitive.NilObjectID
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, tt.args.result) {
				t.Errorf("collectionWrapperOfficial.FindOne() wantResult %+v, but get %+v", tt.wantResult, tt.args.result)
			}
		})
	}
}

func Test_collectionWrapperGeneric_FindID(t *testing.T) {
	genGenericWrapper(t)
	resetTestDataGeneric(t, testDataGroup1)
	item, has, err := colWrapperGenericForTest.FindOne(context.Background(), bson.M{"likes": 2}, nil, 0)
	if err != nil {
		t.Error(err)
	}
	if !has {
		t.Errorf("get id fail")
	}
	ID := item.ID
	type args struct {
		ctx  context.Context
		ID   interface{}
		opts []*options.FindOneOptions
	}
	tests := []struct {
		name       string
		args       args
		wantHas    bool
		wantErr    bool
		wantResult testDataIDSt
	}{
		{
			name: "find_id",
			args: args{
				ID: ID,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: testDataIDSt{Likes: 2, Score: 0.9},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTest
			gotItem, gotHas, err := c.FindID(tt.args.ctx, tt.args.ID, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindID() = %v, want %v", gotHas, tt.wantHas)
			}
			gotItem.ID = primitive.NilObjectID
			if !reflect.DeepEqual(tt.wantResult, gotItem) {
				t.Errorf("collectionWrapperOfficial.FindID() wantResult %+v, but get %+v", tt.wantResult, gotItem)
			}
		})
	}
}

func Test_collectionWrapperGeneric_Ptr_FindID(t *testing.T) {
	genGenericWrapper(t)
	resetTestDataGeneric(t, testDataGroup1)
	item, has, err := colWrapperGenericForTest.FindOne(context.Background(), bson.M{"likes": 2}, nil, 0)
	if err != nil {
		t.Error(err)
	}
	if !has {
		t.Errorf("get id fail")
	}
	ID := item.ID
	type args struct {
		ctx  context.Context
		ID   interface{}
		opts []*options.FindOneOptions
	}
	tests := []struct {
		name       string
		args       args
		wantHas    bool
		wantErr    bool
		wantResult *testDataIDSt
	}{
		{
			name: "find_id",
			args: args{
				ID: ID,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataIDSt{Likes: 2, Score: 0.9},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTestPtr
			gotItem, gotHas, err := c.FindID(tt.args.ctx, tt.args.ID, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindID() = %v, want %v", gotHas, tt.wantHas)
			}
			gotItem.ID = primitive.NilObjectID
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, gotItem) {
				t.Errorf("collectionWrapperOfficial.FindID() wantResult %+v, but get %+v", tt.wantResult, gotItem)
			}
		})
	}
}

func Test_collectionWrapperGeneric_FindOneAndUpdate(t *testing.T) {
	genGenericWrapper(t)
	resetTestDataGeneric(t, testDataGroup1)
	type args struct {
		ctx       context.Context
		filter    interface{}
		update    interface{}
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
		wantResult testDataIDSt
	}{
		{
			name: "find_ond_and_update_return_old",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 1}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: testDataIDSt{Likes: 5, Score: 0.6},
		},
		{
			name: "find_ond_and_update_return_new",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 1}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: testDataIDSt{Likes: 5, Score: 66.6},
		},
		{
			name: "find_ond_and_update_upsert_return_old",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 10}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: false,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: testDataIDSt{},
		},
		{
			name: "find_ond_and_update_upsert_return_new",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 10}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: testDataIDSt{Score: 66.6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTest
			gotItem, gotHas, err := c.FindOneAndUpdate(tt.args.ctx, tt.args.filter, tt.args.update, tt.args.sort, tt.args.upsert, tt.args.returnNew, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOneAndUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOneAndUpdate() = %v, want %v", gotHas, tt.wantHas)
			}
			gotItem.ID = primitive.NilObjectID
			if !reflect.DeepEqual(tt.wantResult, gotItem) {
				t.Errorf("collectionWrapperOfficial.FindOneAndUpdate() wantResult %+v, but get %+v", tt.wantResult, gotItem)
			}
		})
	}
}

func Test_collectionWrapperGeneric_Ptr_FindOneAndUpdate(t *testing.T) {
	genGenericWrapper(t)
	resetTestDataGeneric(t, testDataGroup1)
	type args struct {
		ctx       context.Context
		filter    interface{}
		update    interface{}
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
		wantResult *testDataIDSt
	}{
		{
			name: "find_ond_and_update_return_old",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 1}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataIDSt{Likes: 5, Score: 0.6},
		},
		{
			name: "find_ond_and_update_return_new",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 1}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataIDSt{Likes: 5, Score: 66.6},
		},
		{
			name: "find_ond_and_update_upsert_return_old",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 10}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: false,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: nil,
		},
		{
			name: "find_ond_and_update_upsert_return_new",
			args: args{
				filter:    bson.M{"likes": bson.M{"$gt": 10}},
				update:    bson.M{"$set": bson.M{"score": 66.6}},
				sort:      []string{"-likes"},
				upsert:    true,
				returnNew: true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataIDSt{Score: 66.6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTestPtr
			gotItem, gotHas, err := c.FindOneAndUpdate(tt.args.ctx, tt.args.filter, tt.args.update, tt.args.sort, tt.args.upsert, tt.args.returnNew, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOneAndUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOneAndUpdate() = %v, want %v", gotHas, tt.wantHas)
			}
			if gotHas {
				gotItem.ID = primitive.NilObjectID
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, gotItem) {
				t.Errorf("collectionWrapperOfficial.FindOneAndUpdate() wantResult %+v, but get %+v", tt.wantResult, gotItem)
			}
		})
	}
}

func Test_collectionWrapperGeneric_FindOneAndReplace(t *testing.T) {
	genGenericWrapper(t)
	type args struct {
		ctx         context.Context
		filter      interface{}
		replacement interface{}
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
		wantResult testDataIDSt
	}{
		{
			name: "find_one_and_replace_upsert_false_return_new_has",
			args: args{
				filter:      bson.M{"likes": 1},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      false,
				returnNew:   true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: testDataIDSt{Likes: 8, Score: 2.1},
		},
		{
			name: "find_one_and_replace_upsert_false_return_old_has",
			args: args{
				filter:      bson.M{"likes": 2},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      false,
				returnNew:   false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: testDataIDSt{Likes: 2, Score: 0.9},
		},
		{
			name: "find_one_and_replace_upsert_false_return_new_miss",
			args: args{
				filter:      bson.M{"likes": 10},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      false,
				returnNew:   true,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: testDataIDSt{},
		},
		{
			name: "find_one_and_replace_upsert_false_return_old_miss",
			args: args{
				filter:      bson.M{"likes": 10},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      false,
				returnNew:   false,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: testDataIDSt{},
		},
		{
			name: "find_one_and_replace_upsert_true_return_old_miss",
			args: args{
				filter:      bson.M{"likes": 30},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      true,
				returnNew:   false,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: testDataIDSt{},
		},
		{
			name: "find_one_and_replace_upsert_true_return_old_has",
			args: args{
				filter:      bson.M{"likes": 3},
				replacement: &testDataIDSt{Likes: 3, Score: 0.81},
				upsert:      true,
				returnNew:   false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: testDataIDSt{Likes: 3, Score: 0.8},
		},
		{
			name: "find_one_and_replace_upsert_true_return_new_miss",
			args: args{
				filter:      bson.M{"likes": 30},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      true,
				returnNew:   true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: testDataIDSt{Likes: 8, Score: 2.1},
		},
		{
			name: "find_one_and_replace_upsert_true_return_new_has",
			args: args{
				filter:      bson.M{"likes": 3},
				replacement: &testDataIDSt{Likes: 8, Score: 2.2},
				upsert:      true,
				returnNew:   true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: testDataIDSt{Likes: 8, Score: 2.2},
		},
	}
	for _, tt := range tests {
		resetTestDataGeneric(t, testDataGroup1)
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTest
			gotItem, gotHas, err := c.FindOneAndReplace(tt.args.ctx, tt.args.filter, tt.args.replacement, tt.args.sort, tt.args.upsert, tt.args.returnNew, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOneAndReplace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOneAndReplace() = %v, want %v", gotHas, tt.wantHas)
			}
			gotItem.ID = primitive.NilObjectID
			if !reflect.DeepEqual(tt.wantResult, gotItem) {
				t.Errorf("collectionWrapperOfficial.FindOneAndReplace() wantResult %+v, but get %+v", tt.wantResult, gotItem)
			}
		})
	}
}

func Test_collectionWrapperGeneric_Ptr_FindOneAndReplace(t *testing.T) {
	genGenericWrapper(t)
	type args struct {
		ctx         context.Context
		filter      interface{}
		replacement interface{}
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
		wantResult *testDataIDSt
	}{
		{
			name: "find_one_and_replace_upsert_false_return_new_has",
			args: args{
				filter:      bson.M{"likes": 1},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      false,
				returnNew:   true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataIDSt{Likes: 8, Score: 2.1},
		},
		{
			name: "find_one_and_replace_upsert_false_return_old_has",
			args: args{
				filter:      bson.M{"likes": 2},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      false,
				returnNew:   false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataIDSt{Likes: 2, Score: 0.9},
		},
		{
			name: "find_one_and_replace_upsert_false_return_new_miss",
			args: args{
				filter:      bson.M{"likes": 10},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      false,
				returnNew:   true,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: nil,
		},
		{
			name: "find_one_and_replace_upsert_false_return_old_miss",
			args: args{
				filter:      bson.M{"likes": 10},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      false,
				returnNew:   false,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: nil,
		},
		{
			name: "find_one_and_replace_upsert_true_return_old_miss",
			args: args{
				filter:      bson.M{"likes": 30},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      true,
				returnNew:   false,
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: nil,
		},
		{
			name: "find_one_and_replace_upsert_true_return_old_has",
			args: args{
				filter:      bson.M{"likes": 3},
				replacement: &testDataIDSt{Likes: 3, Score: 0.81},
				upsert:      true,
				returnNew:   false,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataIDSt{Likes: 3, Score: 0.8},
		},
		{
			name: "find_one_and_replace_upsert_true_return_new_miss",
			args: args{
				filter:      bson.M{"likes": 30},
				replacement: &testDataIDSt{Likes: 8, Score: 2.1},
				upsert:      true,
				returnNew:   true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataIDSt{Likes: 8, Score: 2.1},
		},
		{
			name: "find_one_and_replace_upsert_true_return_new_has",
			args: args{
				filter:      bson.M{"likes": 3},
				replacement: &testDataIDSt{Likes: 8, Score: 2.2},
				upsert:      true,
				returnNew:   true,
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataIDSt{Likes: 8, Score: 2.2},
		},
	}
	for _, tt := range tests {
		resetTestDataGeneric(t, testDataGroup1)
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTestPtr
			gotItem, gotHas, err := c.FindOneAndReplace(tt.args.ctx, tt.args.filter, tt.args.replacement, tt.args.sort, tt.args.upsert, tt.args.returnNew, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOneAndReplace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOneAndReplace() = %v, want %v", gotHas, tt.wantHas)
			}
			if gotHas {
				gotItem.ID = primitive.NilObjectID
			}
			if tt.wantResult != nil && !reflect.DeepEqual(tt.wantResult, gotItem) {
				t.Errorf("collectionWrapperOfficial.FindOneAndReplace() wantResult %+v, but get %+v", tt.wantResult, gotItem)
			}
		})
	}
}

func Test_collectionWrapperGeneric_FindOneAndDelete(t *testing.T) {
	genGenericWrapper(t)
	type args struct {
		ctx    context.Context
		filter interface{}
		sort   []string
		opts   []*options.FindOneAndDeleteOptions
	}
	tests := []struct {
		name       string
		args       args
		wantHas    bool
		wantErr    bool
		wantResult testDataIDSt
	}{
		{
			name: "find_one_and_delete_miss",
			args: args{
				filter: bson.M{"likes": 10},
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: testDataIDSt{},
		},
		{
			name: "find_one_and_delete_has",
			args: args{
				filter: bson.M{"likes": bson.M{"$gte": 1}},
				sort:   []string{"-likes"},
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: testDataIDSt{Likes: 5, Score: 0.6},
		},
	}
	for _, tt := range tests {
		resetTestDataGeneric(t, testDataGroup1)
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTest
			gotItem, gotHas, err := c.FindOneAndDelete(tt.args.ctx, tt.args.filter, tt.args.sort, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOneAndDelete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOneAndDelete() = %v, want %v", gotHas, tt.wantHas)
			}
			gotItem.ID = primitive.NilObjectID
			if !reflect.DeepEqual(tt.wantResult, gotItem) {
				t.Errorf("collectionWrapperOfficial.FindOneAndDelete() wantResult %+v, but get %+v", tt.wantResult, gotItem)
			}
		})
	}
}

func Test_collectionWrapperGeneric_Ptr_FindOneAndDelete(t *testing.T) {
	genGenericWrapper(t)
	type args struct {
		ctx    context.Context
		filter interface{}
		sort   []string
		opts   []*options.FindOneAndDeleteOptions
	}
	tests := []struct {
		name       string
		args       args
		wantHas    bool
		wantErr    bool
		wantResult *testDataIDSt
	}{
		{
			name: "find_one_and_delete_miss",
			args: args{
				filter: bson.M{"likes": 10},
			},
			wantHas:    false,
			wantErr:    false,
			wantResult: nil,
		},
		{
			name: "find_one_and_delete_has",
			args: args{
				filter: bson.M{"likes": bson.M{"$gte": 1}},
				sort:   []string{"-likes"},
			},
			wantHas:    true,
			wantErr:    false,
			wantResult: &testDataIDSt{Likes: 5, Score: 0.6},
		},
	}
	for _, tt := range tests {
		resetTestDataGeneric(t, testDataGroup1)
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTestPtr
			gotItem, gotHas, err := c.FindOneAndDelete(tt.args.ctx, tt.args.filter, tt.args.sort, tt.args.opts...)
			if (err != nil) != tt.wantErr {
				t.Errorf("collectionWrapperOfficial.FindOneAndDelete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotHas != tt.wantHas {
				t.Errorf("collectionWrapperOfficial.FindOneAndDelete() = %v, want %v", gotHas, tt.wantHas)
			}
			if gotHas {
				gotItem.ID = primitive.NilObjectID
			}
			if gotItem != nil && !reflect.DeepEqual(tt.wantResult, gotItem) {
				t.Errorf("collectionWrapperOfficial.FindOneAndDelete() wantResult %+v, but get %+v", tt.wantResult, gotItem)
			}
		})
	}
}

func Test_collectionWrapperGeneric_InsertOne(t *testing.T) {
	genGenericWrapper(t)
	type args struct {
		ctx      context.Context
		document testDataIDSt
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
				document: testDataIDSt{
					ID: ID,
				},
			},
			wantInsertedID: ID,
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTest
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

func Test_collectionWrapperGeneric_Ptr_InsertOne(t *testing.T) {
	genGenericWrapper(t)
	type args struct {
		ctx      context.Context
		document *testDataIDSt
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
				document: &testDataIDSt{
					ID: ID,
				},
			},
			wantInsertedID: ID,
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTestPtr
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

func Test_collectionWrapperGeneric_InsertMany(t *testing.T) {
	genGenericWrapper(t)
	ID1 := primitive.NewObjectIDFromTimestamp(time.Now())
	ID2 := primitive.NewObjectIDFromTimestamp(time.Now())
	type args struct {
		ctx      context.Context
		document []testDataIDSt
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
				document: []testDataIDSt{
					{ID: ID1},
					{ID: ID2},
				},
			},
			wantInsertedIDs: []interface{}{ID1, ID2},
			wantErr:         false,
		},
		{
			name: "insert_many_dup",
			args: args{
				document: []testDataIDSt{
					{ID: ID1},
					{ID: ID2},
				},
			},
			wantInsertedIDs: nil,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTest
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

func Test_collectionWrapperGeneric_Ptr_InsertMany(t *testing.T) {
	genGenericWrapper(t)
	ID1 := primitive.NewObjectIDFromTimestamp(time.Now())
	ID2 := primitive.NewObjectIDFromTimestamp(time.Now())
	type args struct {
		ctx      context.Context
		document []*testDataIDSt
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
				document: []*testDataIDSt{
					{ID: ID1},
					{ID: ID2},
				},
			},
			wantInsertedIDs: []interface{}{ID1, ID2},
			wantErr:         false,
		},
		{
			name: "insert_many_dup",
			args: args{
				document: []*testDataIDSt{
					{ID: ID1},
					{ID: ID2},
				},
			},
			wantInsertedIDs: nil,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := colWrapperGenericForTestPtr
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
