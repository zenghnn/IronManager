package IronManager

import (
	"fmt"
	"reflect"
)

//获取结构变量中所有包含xx标签的字段
func GetMapOfSign(body interface{}) map[string]interface{} {
	t := reflect.TypeOf(body)
	bodycopy := body
	dataValue := reflect.ValueOf(bodycopy)
	fmt.Println(dataValue)
	copy := map[string]interface{}{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		zsptag := field.Tag.Get("zsign")
		ii := dataValue.Field(i)
		if zsptag != "" {
			copy[field.Name] = ii.Interface()
		}
	}
	return copy
}
