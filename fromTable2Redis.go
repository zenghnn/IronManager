package IronManager

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zenghnn/IronManager/cache"
	"github.com/zenghnn/IronShard"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

const PerCacheSliceVolume = 1000

func (jav *Javis) Refresh2Cache(tbprefix string, key string) {
	allUm := jav.CacheMapByTable[tbprefix].TableNames
	tbStruct := jav.tableStruct[tbprefix]
	fieldsArr := []string{}
	typeCount := tbStruct.NumField()
	for i := 0; i < typeCount; i++ {
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
			jav.jsonFields = append(jav.jsonFields, column)
		}
	}
	selectFields := strings.Join(fieldsArr, ",")
	//首先把用户openid-userid 对应关系初始化，并存入redis
	wgout := sync.WaitGroup{}
	wgout.Add(len(allUm))
	for outcount, tbname := range allUm {
		go func(loctbname string, oocount int) {
			locDatas := []RegularUse{}
			tableSumRows := 0
			err := jav.Main_IS.DB.Table(loctbname).Count(&tableSumRows).Error
			if err != nil {
				fmt.Println("create mainkey to userid err(1):", err.Error())
				return
			}
			fortime := 0
			if tableSumRows > 0 {
				fortime = tableSumRows/CACHEO2U_LIMIT + 1
			} else {
				fortime = 0
			}
			wgin := sync.WaitGroup{}
			wgin.Add(fortime)
			for i := 0; i < fortime; i++ {
				go func(batchIdx int) {
					err = jav.Main_IS.DB.Table(loctbname).Select(selectFields).Order(key).Offset(batchIdx * CACHEO2U_LIMIT).Limit(CACHEO2U_LIMIT).Find(&locDatas).Error
					if err != nil {
						fmt.Println("create mainkey to userid err(2):", err.Error())
						return
					}
					push2CacheBatch(loctbname, locDatas, key, batchIdx)
					wgin.Done()
				}(i)
			}
			wgin.Wait()
			wgout.Done()
		}(tbname, outcount)

	}
	//javis := Javis{Main_IS: ironShard,O2U: o2u,AllO2U: allO2UKey}
	//MG_POOL[mgName] = &javis
}

func (jav *Javis) GetCacheByUid(tbprefix string, uid int64) (info interface{}, err error) {
	uTbMainIdx, sliceIdx := GetCacheRouter(uid)
	//info = map[string]interface{}{}
	locKey := tbprefix + "_" + strconv.Itoa(uTbMainIdx) + ":" + strconv.Itoa(sliceIdx)
	cacheSliceBytes, err := cache.GetBytes(locKey)
	if err != nil && err == cache.ErrCacheMiss {
		return nil, nil
	}
	dataSlice := map[int64]interface{}{}
	err = json.Unmarshal(cacheSliceBytes, &dataSlice)
	if err != nil {
		return nil, err
	}
	return dataSlice[uid], nil
}

func (jav *Javis) GetOrCreateByTb(tbprefix string, uid int64, initData RegularUse) (info RegularUse, err error) {
	cacheMap, err := jav.GetCacheByUid(tbprefix, uid)
	if err != nil {
		return info, err
	} else if cacheMap == nil {
		//tbStruct := jav.tableStruct[tbprefix]
		newData := initData
		//判断是否大于当前的ID
		shard := jav.CacheMapByTable[tbprefix]
		uTbIdx, sliceIdx := GetCacheRouter(uid)
		cacheKey := ""
		if uid > shard.MaxId {
			//判断要不要建新的表
			if shard.LastTableIdx < uTbIdx {
				shard.NewTable()
				//jav.needNewTb = append(jav.needNewTb, jav.Main_IS.TbPrefix)
			}
		} else {
			return info, errors.New(fmt.Sprintf("no find uid cache & uid <= shard.MaxId:%s,uid:%d", tbprefix, uid))
		}
		cacheKey = shard.TbPrefix + "_" + strconv.Itoa(uTbIdx) + ":" + strconv.Itoa(sliceIdx)
		oldBytes, err := cache.GetBytes(cacheKey)
		oldData := map[int64]interface{}{}
		if err != nil && err == cache.ErrCacheMiss {
			oldData[uid] = newData
			cache.Add(cacheKey, oldData, 0)
		} else {
			json.Unmarshal(oldBytes, &oldData)
			oldData[uid] = newData
			//return nil, errors.New(fmt.Sprintf("find cache has exist cachekey[%s] create new table[%s_%d]", cacheKey, shard.TbPrefix, uTbIdx))
			err = cache.Replace(cacheKey, oldData, 0)
			if err != nil {
				return info, errors.New(fmt.Sprintf("cache replace err tb:%s,uid:%d", tbprefix, uid))
			}
		}
		//然后要把这里的更新标记出来，然后等待定时更新
		javNewRelation, ok := jav.createRelation[tbprefix]
		if !ok {
			javNewRelation = []int64{}
		}
		javNewRelation = append(javNewRelation, uid)
		jav.createRelation[tbprefix] = javNewRelation
		info = newData
	} else {
		info = RegularUse{}
		cacheBytes, _ := json.Marshal(cacheMap)
		json.Unmarshal(cacheBytes, &info)
	}
	return info, nil
}

func newUMData(tbStruct reflect.Type, priKey string, uid int64, initData map[string]interface{}) (map[string]interface{}, error) {
	newData := map[string]interface{}{}
	//tbStruct := jav.tableStruct[tbprefix]
	typeCount := tbStruct.NumField()
	for i := 0; i < typeCount; i++ {
		column := ""
		tag := tbStruct.Field(i).Tag.Get("gorm")
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
		if column == priKey {
			newData[column] = uid
		} else {
			//如果给出了初始化数据，那么就用已有的，否则使用默认

			if initVal, exist := initData[column]; exist {
				newData[column] = initVal
			} else {
				newData[column] = getDefaultValueByGormType(typestr)
			}

		}
	}
	return newData, nil
}

func GetCacheRouter(uid int64) (uTbMainIdx int, sliceIdx int) {
	uTbMainIdx = int(uid / IronShard.TableCountLimit)
	sliceIdx = int(uid%IronShard.TableCountLimit) / PerCacheSliceVolume
	return
}

func getDefaultValueByGormType(gtype string) (defaultV interface{}) {
	if strings.Contains(gtype, "varchar") {
		gtype = "varchar"
	}
	if strings.Contains(gtype, "int") { //这个可能会出现int(4) 这样的
		gtype = "int"
	}
	if strings.Contains(gtype, "timestamp") {
		gtype = "timestamp"
	}
	switch gtype {
	case "int":
		defaultV = 0
	case "bigint":
		defaultV = int64(0)
	case "json":
		defaultV = map[string]interface{}{}
	case "varchar":
	case "datetime":
	case "timestamp":
		defaultV = ""
	}
	return
}

func push2CacheBatch(tbname string, dataList []RegularUse, key string, batchIdx int) {
	//先算出当前应该是这个表在内存(redis)中第几块
	sliceMaps := map[string]map[int64]interface{}{}
	for idx, locdata := range dataList {
		tableInRedisSlice := batchIdx*(CACHEO2U_LIMIT/PerCacheSliceVolume) + (idx / PerCacheSliceVolume)
		locKey := tbname + ":" + strconv.Itoa(tableInRedisSlice)
		theUmdata, exist := sliceMaps[locKey]
		if !exist {
			theUmdata = map[int64]interface{}{}
		}
		theUmdata[locdata.Uid] = locdata
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
