package IronManager

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/tidwall/shardmap"
	"github.com/zenghnn/IronManager/cache"
	"github.com/zenghnn/IronShard"
	"reflect"
	"strconv"
	"strings"
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
	UMPrefix        string //主表名
	AllO2U          []string
	CacheMapByTable map[string]*IronShard.Shard
	tableStruct     map[string]reflect.Type
	TbMainKey       map[string]string
	jsonFields      []string
	updateRelation  map[string]map[int64][]int //  {user_m:[{100001:[1,3]}]}  1,3对应到tableStruct 里面字段的index
	createRelation  map[string][]int64         //  {user_m:[100001,100005]}  要新建的用户ID记录下
	needNewTb       []string                   //  ["user_m"]  要新建的用户ID记录下
}

var maxIdLock *sync.RWMutex

//初始化主表，即用户ID表 并把Openid-uid对应起来
func CreateUserMain(db *gorm.DB, dbname string, redisCfg cache.RedisCfg, tbsql string, tbPrefix string, tbStruct reflect.Type) (o2u *shardmap.Map, ja *Javis, err error) {
	ironShard := IronShard.NewShard(db, dbname)
	cache.CreateRedisStore(redisCfg) // 初始化cache redis
	priKey := "id"
	allUm, err := ironShard.Init(tbPrefix, tbsql, false, priKey) //检查用户分表是否已经完成 如果已经创建好就返回所有的表名
	o2u = &shardmap.Map{}
	allO2UKey := []string{}
	if err != nil {
		return
	}

	jsonFields := []string{}
	fieldsArr := []string{}
	typeCount := tbStruct.NumField()
	for i := 0; i < typeCount; i++ {
		//如果标识为无视字段就跳过
		column := ""
		tag := tbStruct.Field(i).Tag.Get("gorm")
		tagInfos := strings.Split(tag, ";")
		column = ""
		push2jsonField := false
		for _, locStr := range tagInfos {
			locarr := strings.Split(locStr, ":")
			if locarr[0] == "column" {
				column = locarr[1]
			}
			if locarr[0] == "type" && locarr[1] == "json" {
				push2jsonField = true
			}
		}
		fieldsArr = append(fieldsArr, column)
		if push2jsonField {
			jsonFields = append(jsonFields, column)
		}
	}
	selectFields := strings.Join(fieldsArr, ",")

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
				fortime = locCount / CACHEO2U_LIMIT
				if (locCount % CACHEO2U_LIMIT) > 0 {
					fortime += 1
				}
			} else {
				fortime = 0
			}
			wgin := sync.WaitGroup{}
			wgin.Add(fortime)
			for i := 0; i < fortime; i++ {
				go func(ii int, inoutCount int) {
					locUsers := []UserM{}
					err = db.Table(sc.Name).Select(selectFields).Order("id").Offset(ii * CACHEO2U_LIMIT).Limit(CACHEO2U_LIMIT).Find(&locUsers).Error
					if err != nil {
						fmt.Println("create mainkey to userid err(2):", err.Error())
						return
					}
					pushUM2CacheBatch(sc.Name, locUsers, priKey, ii, o2u)
					wgin.Done()
				}(i, oocount)
			}
			wgin.Wait()
			wgout.Done()
		}(schema, outcount)
		//fmt.Println("-tabel -"+schema.Name)
	}
	wgout.Wait()

	javis := Javis{Main_IS: ironShard, O2U: o2u, UMPrefix: tbPrefix, AllO2U: allO2UKey, CacheMapByTable: map[string]*IronShard.Shard{},
		tableStruct: map[string]reflect.Type{}, jsonFields: []string{}, updateRelation: map[string]map[int64][]int{},
		createRelation: map[string][]int64{}, TbMainKey: map[string]string{}, needNewTb: []string{}}
	ja = &javis
	IronMan = &javis
	javis.tableStruct[tbPrefix] = tbStruct
	javis.TbMainKey[tbPrefix] = priKey
	javis.InitCron() //启动定时更新
	maxIdLock = new(sync.RWMutex)
	return
}

func (jav *Javis) CreateTableCahe(tbPrefix string, priKey string, fieldSql string, tbStruct reflect.Type) {
	ironShard := IronShard.NewShard(jav.Main_IS.DB, jav.Main_IS.DBName)
	_, err := ironShard.Init(tbPrefix, fieldSql, false, priKey) //检查用户分表是否已经完成 如果已经创建好就返回所有的表名
	if err != nil {
		fmt.Println("CreateTableCahe err", tbPrefix)
		return
	}
	jav.CacheMapByTable[tbPrefix] = &ironShard
	jav.tableStruct[tbPrefix] = tbStruct
	jav.TbMainKey[tbPrefix] = priKey
	//jav.Refresh2Cache(tbPrefix, priKey)
}

//在主表创建一个新的用户时候
func (jav *Javis) CreateUserMain(openid string, initData UserM) (err error, newData interface{}, regulerByTbName map[string]interface{}) {
	newData = map[string]interface{}{}
	_, exist := jav.O2U.Get(openid)
	if exist {
		return errors.New("openid has exist"), newData, nil
	}
	maxIdLock.Lock() //可能会并发  加个锁
	uid := jav.Main_IS.MaxId + 1
	calTbIdx := int(uid / IronShard.TableCountLimit)
	umTbIdx, sliceIdx := GetCacheRouter(uid)
	//tbStruct := jav.tableStruct[jav.Main_IS.TbPrefix]
	initData.Id = uid
	newData = initData
	cacheKey := jav.Main_IS.TbPrefix + "_" + strconv.Itoa(umTbIdx) + ":" + strconv.Itoa(sliceIdx)
	oldData := map[int64]interface{}{}
	oldBytes, err2 := cache.GetBytes(cacheKey)
	if err2 != nil && err2 == cache.ErrCacheMiss {
		oldData[uid] = newData
		err = cache.Add(cacheKey, oldData, 0)
		if err != nil {
			cache.Replace(cacheKey, oldData, 0)
		}
	} else {
		json.Unmarshal(oldBytes, &oldData)
		oldData[uid] = newData
		err = cache.Replace(cacheKey, oldData, 0)
	}
	err = nil
	if calTbIdx > jav.Main_IS.LastTableIdx { //如果已经最后一个表满了，那么就要新建一个表了
		jav.Main_IS.NewTable() //新增一个此类表
		//jav.needNewTb = append(jav.needNewTb, jav.Main_IS.TbPrefix)
	}
	creatRelation, ok := jav.createRelation[jav.Main_IS.TbPrefix]
	if !ok {
		creatRelation = []int64{}
	}
	creatRelation = append(creatRelation, uid)
	jav.createRelation[jav.Main_IS.TbPrefix] = creatRelation
	jav.Main_IS.MaxId = uid
	jav.O2U.Set(openid, uid)
	maxIdLock.Unlock()

	regulerByTbName = map[string]interface{}{}
	for locTbPrefix, _ := range jav.CacheMapByTable {
		reguler_use := RegularUse{Uid: uid, ShopGeomancy: "", RoomScheme: map[string]interface{}{}, ShopScheme: map[string]interface{}{}}
		regulerInfo, err1 := jav.GetOrCreateByTb(locTbPrefix, uid, reguler_use)
		if err1 == nil {
			regulerByTbName[locTbPrefix] = regulerInfo
		}
	}
	return nil, newData, regulerByTbName
}

func (jav *Javis) GetUM(uid int64) (umData UserM, err error) {
	umTbIdx, sliceIdx := GetCacheRouter(uid)
	cacheKey := jav.Main_IS.TbPrefix + "_" + strconv.Itoa(umTbIdx) + ":" + strconv.Itoa(sliceIdx)
	backBytes, err2 := cache.GetBytes(cacheKey)
	if err2 != nil && err2 == cache.ErrCacheMiss {
		return umData, errors.New("get um " + cacheKey + ")")
	}
	umData = UserM{}
	json.Unmarshal(backBytes, &umData)
	return umData, nil
}

func (jav *Javis) GetUMBytes(uid int64) (umDataBytes []byte, err error) {
	umTbIdx, sliceIdx := GetCacheRouter(uid)
	cacheKey := jav.Main_IS.TbPrefix + "_" + strconv.Itoa(umTbIdx) + ":" + strconv.Itoa(sliceIdx)
	backBytes, err2 := cache.GetBytes(cacheKey)
	if err2 != nil && err2 == cache.ErrCacheMiss {
		return nil, errors.New("get um " + cacheKey + ")")
	}
	umsData := map[int64]map[string]interface{}{}
	json.Unmarshal(backBytes, &umsData)
	um := umsData[uid]
	if um == nil {
		return nil, errors.New("GetUMBytes not find?")
	}
	umDataBytes, err = json.Marshal(um)
	return umDataBytes, nil
}

func pushUM2CacheBatch(tbname string, dataList []UserM, key string, batchIdx int, sm *shardmap.Map) {
	//先算出当前应该是这个表在内存(redis)中第几块
	//endCount := len(dataList)
	sliceMaps := map[string]map[int64]interface{}{}
	for idx, locdata := range dataList {
		tableInRedisSlice := batchIdx*(CACHEO2U_LIMIT/PerCacheSliceVolume) + (idx / PerCacheSliceVolume)
		//yushu := int(locdata.Id % PerCacheSliceVolume)
		if sm != nil {
			if locdata.AccType != 0 && locdata.AccType == 2 { //如果是帐号密码就用帐号名来查找
				if locdata.Account == "" {
					continue
				}
				sm.Set(locdata.Account, locdata.Id)
			} else {
				if locdata.OpenId == "" {
					continue
				}
				sm.Set(locdata.OpenId, locdata.Id)
			}
		}

		locKey := tbname + ":" + strconv.Itoa(tableInRedisSlice)
		theUmdata, exist := sliceMaps[locKey]
		if !exist {
			theUmdata = map[int64]interface{}{}
		}
		theUmdata[locdata.Id] = locdata
		sliceMaps[locKey] = theUmdata
	}

	for cacheKey, m := range sliceMaps {
		err, _ := cache.Get(cacheKey)
		if err == cache.ErrCacheMiss {
			err = cache.Add(cacheKey, m, 0)
			if err != nil {
				fmt.Println("create mainkey to userid err(3):", err.Error())
				return
			}
		} else {
			err = cache.Replace(cacheKey, m, 0)
			if err != nil {
				fmt.Println("create mainkey to userid err(4):", err.Error())
				return
			}
		}
	}

}

func createUMData(priKey string, uid int64, initData UserM) (UserM, error) {
	//newData := map[string]interface{}{}
	tbStruct := reflect.TypeOf(initData)
	tbValues := reflect.ValueOf(initData)
	typeCount := tbStruct.NumField()
	for i := 0; i < typeCount; i++ {
		column := ""
		structField := tbStruct.Field(i)
		tag := structField.Tag.Get("gorm")
		tagInfos := strings.Split(tag, ";")
		column = ""
		typestr := ""
		for _, locStr := range tagInfos {
			locarr := strings.Split(locStr, ":")
			if locarr[0] == "column" {
				column = locarr[1]
			}
			if locarr[0] == "type" {
				typestr = locarr[1]
			}
		}
		locField := tbValues.Field(i)
		if column == priKey {
			locField.Set(reflect.ValueOf(uid))
		} else if typestr == "json" {
			locField.Set(reflect.ValueOf("{}"))
		}
	}
	return initData, nil
}
