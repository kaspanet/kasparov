package dbaccess

import "github.com/jinzhu/gorm"

func preloadColumns(query *gorm.DB, preloadedColumns []string) *gorm.DB {
	for _, column := range preloadedColumns {
		query = query.Preload(column)
	}
	return query
}
