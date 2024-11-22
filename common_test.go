package gomongodb

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func Test_updateSafeCheck(t *testing.T) {
	type args struct {
		update interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "nil update",
			args: args{
				update: nil,
			},
			wantErr: false,
		},
		{
			name: "not bson.M",
			args: args{
				update: map[string]interface{}{},
			},
			wantErr: true,
		},
		{
			name: "unexported field",
			args: args{
				update: bson.M{
					"$set": struct {
						a int
					}{},
				},
			},
			wantErr: false,
		},
		{
			name: "not pointer",
			args: args{
				update: bson.M{
					"$set": struct {
						a int
						A int `bson:"a,omitempty"`
					}{},
				},
			},
			wantErr: true,
		},
		{
			name: "no omitempty",
			args: args{
				update: bson.M{
					"$set": struct {
						A *int `bson:"a"`
					}{},
				},
			},
			wantErr: true,
		},
		{
			name: "point to struct",
			args: args{
				update: bson.M{
					"$unset": struct {
						A *struct{} `bson:"a,omitempty"`
					}{},
				},
			},
			wantErr: true,
		},
		{
			name: "point to map",
			args: args{
				update: bson.M{
					"$set": struct {
						A *map[string]string `bson:"a,omitempty"`
					}{},
				},
			},
			wantErr: true,
		},
		{
			name: "succ",
			args: args{
				update: bson.M{
					"$set": &struct {
						A *int `bson:"a,omitempty"`
					}{},
					"$unset": &struct {
						B *int `bson:"b,omitempty"`
					}{},
				},
			},
			wantErr: false,
		},
		{
			name: "ignore bson -",
			args: args{
				update: bson.M{
					"$set": struct {
						A map[string]string `bson:"-"`
					}{},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := updateSafeCheck(tt.args.update); (err != nil) != tt.wantErr {
				t.Errorf("updateSafeCheck() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
