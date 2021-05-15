package IronManager

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/tidwall/shardmap"
	"github.com/zenghnn/IronManager/cache"
	"github.com/zenghnn/IronShard"
	"reflect"
	"sync"
)

var (
	//MG_POOL map[string]*Javis
	IronMan *Javis
)

const CACHEO2U_LIMIT = 10000

type Javis struct {
	Main_IS         IronShard.Shard
	O2U             *shardmap.Map
	AllO2U          []string
	CacheMapByTable map[string]*IronShard.Shard
	tableStruct     map[string]reflect.Type
	jsonFields      []string
	updateRelation  map[string]map[int64][]string //  {user_m:[{100001:["mobile","last_login"]}]}
}

type simpleUm struct {
	Id      int64  `gorm:"column:id;type:bigint;" json:"id"`
	OpenId  string `gorm:"column:open_id;size:100;type:varchar(100);" json:"open_id"`
	Account string `gorm:"column:account;size:100;type:varchar(100);" json:"account"`
	AccType int    `gorm:"column:acc_type;size:100;type:int;" json:"acc_type"`
}

//初始化主表，即用户ID表 并把Openid-uid对应起来
func CreateMapOfOutId2UserId(db *gorm.DB, dbname string, redisCfg cache.RedisCfg, tbsql string, tbPrefix string) (o2u *shardmap.Map, ja *Javis, err error) {
	ironShard := IronShard.NewShard(db, dbname)
	cache.CreateRedisStore(redisCfg)                           // 初始化cache redis
	allUm, err := ironShard.Init(tbPrefix, tbsql, false, "id") //检查用户分表是否已经完成 如果已经创建好就返回所有的表名
	o2u = &shardmap.Map{}
	allO2UKey := []string{}
	if err != nil {
		return
	}
	//userChan := make(chan []simpleUm,10)
	//endsignor := make(chan bool)
	wgout := sync.WaitGroup{}
	umcount := len(allUm)
	wgout.Add(umcount)
	//首先把用户openid-userid 对应关系初始化，并存入redis
	for outcount, schema := range allUm {
		go func(sc IronShard.MysqlSchema, oocount int) {
			locCount := 0
			err = db.Table(sc.Name).Count(&locCount).Error
			if err != nil {
				fmt.Println("create mainkey to userid err(1):", err.Error())
				return
			}
			fortime := 0
			if locCount > 0 {
				fortime = locCount/CACHEO2U_LIMIT + 1
			} else {
				fortime = 0
			}
			selectFields := "id,open_id,account,acc_type"

			wgin := sync.WaitGroup{}
			wgin.Add(fortime)
			for i := 0; i < fortime; i++ {
				go func(i int, inoutCount int) {
					locUsers := []simpleUm{}
					err = db.Table(sc.Name).Select(selectFields).Offset(i * CACHEO2U_LIMIT).Limit(CACHEO2U_LIMIT).Find(&locUsers).Error
					if err != nil {
						fmt.Println("create mainkey to userid err(2):", err.Error())
						return
					}
					for _, usersimple := range locUsers {
						if usersimple.AccType == 2 { //如果是帐号密码就用帐号名来查找
							if usersimple.Account == "" {
								continue
							}
							//uOpen2Id[usersimple.Account] = usersimple.Id
							o2u.Set(usersimple.Account, usersimple.Id)
						} else {
							if usersimple.OpenId == "" {
								continue
							}
							//uOpen2Id[usersimple.OpenId] = usersimple.Id
							o2u.Set(usersimple.OpenId, usersimple.Id)
						}
					}
					//userChan <- locUsers
					wgin.Done()
				}(i, oocount)
				//fmt.Sprintf("- t ulen - %d", len(locUsers))
			}
			wgin.Wait()
			wgout.Done()
		}(schema, outcount)
		//fmt.Println("-tabel -"+schema.Name)
	}
	wgout.Wait()

	javis := Javis{Main_IS: ironShard, O2U: o2u, AllO2U: allO2UKey, CacheMapByTable: map[string]*IronShard.Shard{},
		tableStruct: map[string]reflect.Type{}, jsonFields: []string{}, updateRelation: map[string]map[int64][]string{}}
	ja = &javis
	IronMan = &javis
	return
}

func (jav *Javis) CreateTableCahe(tbPrefix string, priKey string, fieldSql string, tbStruct reflect.Type) {
	ironShard := IronShard.NewShard(jav.Main_IS.DB, jav.Main_IS.DBName)
	_, err := ironShard.Init(tbPrefix, fieldSql, false, priKey) //检查用户分表是否已经完成 如果已经创建好就返回所有的表名
	if err != nil {
		fmt.Println("CreateTableCaheByUserId err", tbPrefix)
		return
	}
	jav.CacheMapByTable[tbPrefix] = &ironShard
	jav.tableStruct[tbPrefix] = tbStruct

	jav.Refresh2Cache(tbPrefix, priKey)
}
