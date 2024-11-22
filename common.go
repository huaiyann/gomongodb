package gomongodb

import (
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	DEFAULT_POOLSIZE       = 3
	DEFAULT_SOCKET_TIMEOUT = 10 * time.Second
)

// updateSafeCheck尽量约束update语句，防止意外的字段覆盖
func updateSafeCheck(update interface{}) error {
	if update == nil {
		return nil
	}
	m, ok := update.(bson.M)
	if !ok {
		return errors.New("update语句只接受bson.M类型")
	}
	// 如果set和unset语句使用了结构体类型，要检查
	checkSetAndUnsetStruct := func(data interface{}) error {
		tt := reflect.TypeOf(data)
		if tt.Kind() == reflect.Ptr {
			tt = tt.Elem()
		}
		if tt.Kind() != reflect.Struct {
			return nil
		}
		// 对于可导出的字段，必需为指针类型，且tag中有omitempty，且内容不是结构体或map
		for i := 0; i < tt.NumField(); i++ {
			field := tt.Field(i)
			if field.PkgPath != "" { // 忽略非导出字段
				continue
			}
			if tag := field.Tag.Get("bson"); tag == "-" {
				// 忽略不导出bson的字段
				continue
			}
			if k := field.Type.Kind(); k != reflect.Ptr {
				return errors.Errorf("字段必需为指针类型，但%s为%s", field.Name, k)
			}
			if k := field.Type.Elem().Kind(); k == reflect.Struct || k == reflect.Map {
				return errors.Errorf("字段类型不支持struct或map，但%s为%s", field.Name, k)
			}
			if tag := field.Tag.Get("bson"); !strings.Contains(tag, ",omitempty") {
				return errors.Errorf("字段tag必需包含omitempty，但%s `bson:\"%s\"`未包含", field.Name, tag)
			}
		}
		return nil
	}
	if set, ok := m["$set"]; ok {
		if err := checkSetAndUnsetStruct(set); err != nil {
			return errors.Wrap(err, "校验$set失败")
		}
	}
	if unset, ok := m["$unset"]; ok {
		if err := checkSetAndUnsetStruct(unset); err != nil {
			return errors.Wrap(err, "校验$unset失败")
		}
	}
	return nil
}
