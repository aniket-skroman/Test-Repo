package models

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type User struct {
	Id              primitive.ObjectID `json:"id,omitempty" bson:"_id"`
	Name            string             `json:"name" bson:"name" validate:"required"`
	Email           string             `json:"email,omitempty" bson:"email"`
	ProfilePic      string             `json:"profile_pic" bson:"avtar"`
	UserType        string             `json:"user_type" bson:"user_type"`
	Password        string             `json:"password" bson:"password"`
	IsPasswordReset bool               `json:",omitempty" bson:"is_pass_reset" default:"false"`
	OldPassword     string             `json:",omitempty"`
	Token           string             `json:"token,omitempty" bson:"token"`
	CreatedAt       primitive.DateTime `json:"createdAt" bson:"createdAt"`
	UpdatedAt       primitive.DateTime `bson:"updatedAt" json:"updatedAt"`
}

type TestModel struct {
	Id       primitive.ObjectID `json:"id,omitempty" bson:"_id"`
	TestName string             `json:"test_name" bson:"test"`
	Data     []int              `json:"data" bson:"data"`
}
