module github.com/zenghnn/IronManager

go 1.15

require (
	github.com/garyburd/redigo v1.6.2
	github.com/jinzhu/gorm v1.9.16
	github.com/tidwall/rhh v1.1.1 // indirect
	github.com/tidwall/shardmap v0.0.0-20190927132224-c190691bd211
	github.com/zenghnn/IronShard v0.0.0-00010101000000-000000000000
)

replace github.com/zenghnn/IronShard => ../IronShard
