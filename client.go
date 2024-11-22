package gomongodb

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Config struct {
	Hostport           string `json:"hostport" ini:"hostport" yaml:"hostport"`
	UserName           string `json:"username" ini:"username" yaml:"username"`
	Password           string `json:"password" ini:"password" yaml:"password"`
	Poolsize           int    `json:"poolsize" ini:"poolsize" yaml:"poolsize"`
	Timeout            int    `json:"timeout" ini:"timeout" yaml:"timeout"`
	SecondaryPreferred bool   `json:"enable_secondary_preferred" ini:"enable_secondary_preferred" yaml:"enable_secondary_preferred"`
}

type Client struct {
	conn                   *mongo.Client
	pool                   chan bool
	timeout                time.Duration
	metricTarget           string
	metricsLabelConverters []func(label string) (newLabel string, hit bool)
}

var initMetricOnce sync.Once

var (
	metricLatency *prometheus.HistogramVec
	metricError   *prometheus.CounterVec
)

func InitClient(cfg Config, opts ...*options.ClientOptions) (client *Client, err error) {
	// 矫正参数
	timeout := time.Duration(cfg.Timeout) * time.Second
	if timeout <= 0 {
		timeout = DEFAULT_SOCKET_TIMEOUT
	}
	poolsize := cfg.Poolsize
	if poolsize <= 0 {
		poolsize = DEFAULT_POOLSIZE
	}
	password := cfg.Password

	option := options.Client().ApplyURI(cfg.Hostport)
	option.SetConnectTimeout(timeout)
	option.SetSocketTimeout(timeout)
	option.SetMaxPoolSize(uint64(cfg.Poolsize) + 2)
	option.SetMaxConnIdleTime(time.Minute * 10)

	if cfg.UserName != "" {
		option.SetAuth(options.Credential{
			Username: cfg.UserName,
			Password: password,
		})
	}
	if cfg.SecondaryPreferred {
		option.SetReadPreference(readpref.SecondaryPreferred())
	}
	options := []*options.ClientOptions{option}
	options = append(options, opts...)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := mongo.Connect(ctx, options...)
	if err != nil {
		return
	}

	// 提前ping一下，才能马上识别到后面的节点身份
	err = conn.Ping(ctx, nil)
	if err != nil {
		return
	}

	pool := make(chan bool, poolsize)
	for i := 0; i < poolsize; i++ {
		pool <- true
	}
	_, file, line, _ := runtime.Caller(1)
	if subs := strings.Split(file, "/"); len(subs) > 2 {
		file = subs[len(subs)-2] + "/" + subs[len(subs)-1]
	}
	client = &Client{
		conn:    conn,
		pool:    pool,
		timeout: timeout,

		// 好多不规范的地方，把密码写到了明文的hostport中
		// 所以这里的metric标识没敢用hostport，而是使用client实例的初始化位置来代替
		metricTarget: fmt.Sprintf("%s:%d", file, line),
	}

	initMetricOnce.Do(func() {
		metricLatency, err = registMetrics(prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "gomongodb",
			Name:      "mongo_official_client_command_latency",
			Help:      "Histogram of mongo request",
			Buckets:   []float64{5, 15, 30, 50, 100, 300, 600, 1000, 2500, 5000, 10000},
		}, []string{"target", "command", "db", "collection"}))
		if err != nil {
			err = errors.Wrap(err, "registMetrics")
			return
		}
		metricError, err = registMetrics(prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "gomongodb",
			Name:      "mongo_official_client_command_error",
			Help:      "Counter of mongo request error",
		}, []string{"target", "command", "db", "collection"}))
		if err != nil {
			err = errors.Wrap(err, "registMetrics")
			return
		}
	})
	return
}

// Client get mongo driver client
func (f *Client) Client() *mongo.Client {
	return f.conn
}

// Timeout get configed timeout
func (f *Client) Timeout() time.Duration {
	return f.timeout
}

func (f *Client) ScanCursor(ctx context.Context, cursor *mongo.Cursor, result interface{}) (err error) {
	defer func() {
		err1 := cursor.Close(ctx)
		if err == nil && err1 != nil {
			err = err1
		}
	}()

	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		err = fmt.Errorf("result argument must be a slice address")
		return
	}
	slicev := resultv.Elem()
	slicev = slicev.Slice(0, slicev.Cap())
	elemt := slicev.Type().Elem()

	i := 0
	for ; cursor.Next(ctx); i++ {
		if slicev.Len() == i {
			// slice长度耗尽时，通过append触发slice_grow
			// 并将slice的len扩大到与cap相同，保证新增长的空间可用index索引
			elemp := reflect.New(elemt)
			err = cursor.Decode(elemp.Interface())
			if err != nil {
				return
			}
			slicev = reflect.Append(slicev, elemp.Elem())
			slicev = slicev.Slice(0, slicev.Cap())
		} else {
			// slice长度未耗尽时通过index索引使用剩余空间
			err = cursor.Decode(slicev.Index(i).Addr().Interface())
			if err != nil {
				return
			}
		}
	}
	// 将slice恢复为真正的长度
	resultv.Elem().Set(slicev.Slice(0, i))

	err = cursor.Err()
	if err != nil {
		return
	}
	return
}

// NewCollectionWrapper get collection operation wrapper, with a default waitPoolTimeout valued one second.
func (f *Client) NewCollectionWrapper(database, collection string) CollectionWrapper {
	return &collectionWrapper{
		client:     f,
		database:   database,
		collection: collection,
	}
}

/*
DoTransaction is helper to exec transaction.

Ref: https://www.mongodb.com/docs/drivers/go/current/fundamentals/transactions/

1. opts can be:

wc := writeconcern.New(writeconcern.WMajority())

opts := options.Transaction().SetWriteConcern(wc)

2. ATTENTION:

mongo commonds in fn must use sessCtx as its ctx param
*/
func (f *Client) DoTransaction(ctx context.Context, fn func(sessCtx mongo.SessionContext) (
	interface{}, error), opts ...*options.TransactionOptions) (res interface{}, err error) {
	session, err := f.Client().StartSession()
	if err != nil {
		err = errors.Wrap(err, "StartSession")
		return
	}
	defer session.EndSession(ctx)

	res, err = session.WithTransaction(ctx, fn, opts...)
	if err != nil {
		err = errors.Wrap(err, "WithTransaction")
		return
	}
	return
}

/*
AddMetricsLabelConverter

添加db、collection的转换逻辑，用于向Prometheus上报metrics时的lable处理，防止分库分表的存储导致label过多。

依次执行所有converter，直到返回hit=ture且newLabel不为空。
*/
func (f *Client) AddMetricsLabelConverter(c func(label string) (newLabel string, hit bool)) {
	f.metricsLabelConverters = append(f.metricsLabelConverters, c)
}

func (f *Client) convertMetricsLabel(label string) string {
	for _, converter := range f.metricsLabelConverters {
		newLabel, hit := converter(label)
		if hit && newLabel != "" {
			return newLabel
		}
	}
	return label
}

func registMetrics[T prometheus.Collector](collector T) (T, error) {
	var null T
	err := prometheus.DefaultRegisterer.Register(collector)
	if err == nil {
		return collector, nil
	}

	if arErr, ok := err.(prometheus.AlreadyRegisteredError); ok {
		already, ok := arErr.ExistingCollector.(T)
		if ok {
			return already, nil
		} else {
			return null, errors.Errorf("ExistingCollector, want %v, but %v",
				reflect.TypeOf(null), reflect.TypeOf(arErr.ExistingCollector))
		}
	}

	return null, err
}

func NewCollectionWrapper[T any](client *Client, database, collection string) CollectionWrapperGeneric[T] {
	wrapper := &collectionWrapper{
		client:     client,
		database:   database,
		collection: collection,
	}
	return &collectionWrapperGeneric[T]{
		collectionWrapper: wrapper,
	}
}
