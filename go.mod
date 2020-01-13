module github.com/kaspanet/kasparov

go 1.13

require (
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/golang-migrate/migrate/v4 v4.7.1
	github.com/gorilla/handlers v1.4.2
	github.com/gorilla/mux v1.7.3
	github.com/jessevdk/go-flags v1.4.0
	github.com/jinzhu/gorm v1.9.11
	github.com/kaspanet/kaspad v0.1.1
	github.com/pkg/errors v0.8.1
)

replace github.com/kaspanet/kaspad => ../kaspad
