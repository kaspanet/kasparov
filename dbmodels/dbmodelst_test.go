package dbmodels

import (
	"reflect"
	"strings"
	"testing"
)

func TestFieldNames(t *testing.T) {
	tests := []struct {
		fieldNames, model interface{}
	}{
		{
			fieldNames: &BlockFieldNames,
			model:      &Block{},
		},
		{
			fieldNames: &ParentBlockFieldNames,
			model:      &ParentBlock{},
		},
		{
			fieldNames: &RawBlockFieldNames,
			model:      &RawBlock{},
		},
		{
			fieldNames: &TransactionFieldNames,
			model:      &Transaction{},
		},
		{
			fieldNames: &TransactionBlockFieldNames,
			model:      &TransactionBlock{},
		},
		{
			fieldNames: &TransactionOutputFieldNames,
			model:      &TransactionOutput{},
		},
		{
			fieldNames: &TransactionInputFieldNames,
			model:      &TransactionInput{},
		},
		{
			fieldNames: &RawTransactionFieldNames,
			model:      &RawTransaction{},
		},
	}
	for _, test := range tests {
		values := structFieldNamesToStringsSlice(test.fieldNames)
		for _, value := range values {
			if !existsInModel(test.model, value) {
				t.Errorf("%s is not a field of %T", value, test.model)
			}
		}
	}
}

func existsInModel(model interface{}, fieldName string) bool {
	dotIndex := strings.Index(fieldName, ".")
	if dotIndex == -1 {
		fieldSet := modelFieldSet(model)
		_, ok := fieldSet[fieldName]
		return ok
	}
	emptyFieldModel, ok := getModelZeroValueByFieldName(model, fieldName[:dotIndex])
	if !ok {
		return false
	}
	return existsInModel(emptyFieldModel, fieldName[dotIndex+1:])
}

func getModelZeroValueByFieldName(model interface{}, fieldName string) (interface{}, bool) {
	elem := reflect.ValueOf(model).Elem()
	field := elem.FieldByName(fieldName)
	if field == (reflect.Value{}) {
		return nil, false
	}
	if field.Kind() == reflect.Slice {
		structField, ok := reflect.TypeOf(model).Elem().FieldByName(fieldName)
		if !ok {
			return nil, false
		}
		return reflect.New(structField.Type.Elem()).Interface(), true
	}
	return reflect.New(field.Type()).Interface(), true
}

func modelFieldSet(model interface{}) map[string]struct{} {
	elem := reflect.ValueOf(model).Elem()
	typeOfT := elem.Type()
	set := make(map[string]struct{})

	for i := 0; i < elem.NumField(); i++ {
		set[typeOfT.Field(i).Name] = struct{}{}
	}
	return set
}

func structFieldNamesToStringsSlice(s interface{}) []string {
	elem := reflect.ValueOf(s).Elem()
	values := make([]string, 0)

	for i := 0; i < elem.NumField(); i++ {
		f := elem.Field(i)
		values = append(values, string(f.Interface().(FieldName)))
	}
	return values
}
