package dbaccess

import (
	"strings"

	"github.com/pkg/errors"
)

// Order signifies the order at which results to a query are going to be returned.
type Order string

// Order constants
const (
	OrderUnknown    Order = ""
	OrderAscending  Order = "ASC"
	OrderDescending Order = "DESC"
)

// StringToOrder converts an order string into an Order type.
// Returns OrderUnknown and an error if passed string is not ASC or DESC
func StringToOrder(orderString string) (Order, error) {
	order := Order(strings.ToUpper(orderString))
	if order != OrderAscending && order != OrderDescending {
		return OrderUnknown, errors.Errorf("'%s' is not a valid order", orderString)
	}
	return order, nil
}
