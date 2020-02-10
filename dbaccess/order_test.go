package dbaccess

import "testing"

func TestStringToOrder(t *testing.T) {
	tests := []struct {
		orderString    string
		expectedResult Order
		expectedError  string
	}{
		{"ASC", OrderAscending, ""},
		{"DESC", OrderDescending, ""},
		{"asc", OrderAscending, ""},
		{"desc", OrderDescending, ""},
		{"AsC", OrderAscending, ""},
		{"DeSc", OrderDescending, ""},
		{"", OrderUnknown, "'' is not a valid order"},
		{"assc", OrderUnknown, "'assc' is not a valid order"},
		{"dfauhijadsfhlafhjlkdgflhjk18273981723", OrderUnknown, "'dfauhijadsfhlafhjlkdgflhjk18273981723' is not a valid order"},
	}

	for _, test := range tests {
		result, err := StringToOrder(test.orderString)

		if result != test.expectedResult {
			t.Errorf("%s: Expected result '%s' but got '%s'", test.orderString, test.expectedResult, result)
		}

		errString := ""
		if err != nil {
			errString = err.Error()
		}
		if test.expectedError != errString {
			t.Errorf("%s: Expected error '%s' but got '%s'", test.orderString, test.expectedError, errString)
		}
	}
}
