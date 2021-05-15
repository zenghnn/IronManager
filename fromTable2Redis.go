package IronManager

import (
	"encoding/json"
	"fmt"
	"github.com/zenghnn/IronManager/cache"
	"github.com/zenghnn/IronShard"
	"strconv"
	"strings"
)

const PerCacheSliceVolume = 100

func (jav *Javis) Refresh2Cache(tbprefix string, key string) {
	allUm := jav.CacheMapByTable[tbprefix].TableNames
	tbStruct := jav.tableStruct[tbprefix]
	fieldsArr := []string{}
	typeCount := tbStruct.NumField()
	for i := 0; i < typeCount; i++ {
		//如果标识为无视字段就跳过
		tagiron := tbStruct.Field(i).Tag.Get("iron")
		if tagiron == "" || tagiron == "-" {
			continue
		}
		//如果标识为json字段，就在字段存取时候转换一下
		tagInfos := strings.Split(tagiron, ";")
		column := ""
		push2jsonField := false
		for _, locStr := range tagInfos {
			locarr := strings.Split(locStr, ":")
			if locarr[0] == "isjson" && locarr[1] == "true" {
				push2jsonField = true
			}
		}

		tag := tbStruct.Field(i).Tag.Get("gorm")
		tagInfos = strings.Split(tag, ";")
		column = ""
		for _, locStr := range tagInfos {
			locarr := strings.Split(locStr, ":")
			if locarr[0] == "column" {
				column = locarr[1]
			}
		}
		fieldsArr = append(fieldsArr, column)
		if push2jsonField {
			jav.jsonFields = append(jav.jsonFields, column)
		}
	}
	selectFields := strings.Join(fieldsArr, ",")
	//首先把用户openid-userid 对应关系初始化，并存入redis
	for _, tbname := range allUm {
		//uOpen2Id := map[string]int64{}
		locDatas := []map[string]interface{}{}
		tableSumRows := 0
		err := jav.Main_IS.DB.Table(tbname).Count(&tableSumRows).Error
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
		//selectFields :=  "uid,open_id,account,acc_type"
		for i := 0; i < fortime; i++ {
			err = jav.Main_IS.DB.Table(tbname).Select(selectFields).Order(key).Offset(i * CACHEO2U_LIMIT).Limit(CACHEO2U_LIMIT).Find(&locDatas).Error
			if err != nil {
				fmt.Println("create mainkey to userid err(2):", err.Error())
				return
			}
			pushLoop(tbname, locDatas, key, jav.jsonFields)
		}
	}
	//javis := Javis{Main_IS: ironShard,O2U: o2u,AllO2U: allO2UKey}
	//MG_POOL[mgName] = &javis
}

func (jav *Javis) GetCacheByUid(tbprefix string, uid int64) (info interface{}, err error) {
	uTbMainIdx, sliceIdx := GetCacheRouter(uid)
	//uTbMainIdx := int(uid/IronShard.TableCountLimit)
	//sliceIdx := int(uid % IronShard.TableCountLimit) /PerCacheSliceVolume
	locKey := tbprefix + "_" + strconv.Itoa(uTbMainIdx) + ":" + strconv.Itoa(sliceIdx)
	cacheSliceBytes, err := cache.GetBytes(locKey)
	if err != nil && err == cache.ErrCacheMiss {
		return nil, nil
	}
	dataSlice := map[int64]interface{}{}
	err = json.Unmarshal(cacheSliceBytes, &dataSlice)
	if err != nil {
		return
	}
	info = dataSlice[uid]
	return
}

func (jav *Javis) GetOrCreateByTb(tbprefix string, uid int64) (info interface{}, err error) {
	info, err = jav.GetCacheByUid(tbprefix, uid)
	if err != nil {
		return nil, err
	} else if info == nil {
		updateFields := []string{}
		newData := map[string]interface{}{}
		tbStruct := jav.tableStruct[tbprefix]
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
			updateFields = append(updateFields, column)
			newData[column] = getValueByGormType(typestr)
		}
		uTbMainIdx, sliceIdx := GetCacheRouter(uid)
		locKey := tbprefix + "_" + strconv.Itoa(uTbMainIdx) + ":" + strconv.Itoa(sliceIdx)
		cacheSliceBytes, err := cache.GetBytes(locKey)
		if err != nil && err == cache.ErrCacheMiss {
			return nil, nil
		}
		dataSlice := map[int64]interface{}{}
		err = json.Unmarshal(cacheSliceBytes, &dataSlice)
		dataSlice[uid] = newData
		err = cache.Replace(locKey, dataSlice, 0)
		if err != nil {
			return
		}
		//然后要把这里的更新标记出来，然后等待定时更新
		javUpRelation, ok := jav.updateRelation[tbprefix]
		if !ok {
			javUpRelation = map[int64][]string{}
		}
		javUpRelation[uid] = updateFields
	}
	return
}

func GetCacheRouter(uid int64) (uTbMainIdx int, sliceIdx int) {
	uTbMainIdx = int(uid/IronShard.TableCountLimit) + 1
	sliceIdx = int(uid%IronShard.TableCountLimit) / PerCacheSliceVolume
	return
}

func getValueByGormType(gtype string) (defaultV interface{}) {
	if strings.Contains(gtype, "varchar") {
		gtype = "varchar"
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
		defaultV = ""
	case "varchar":
	case "datetime":
	case "timestamp":
		defaultV = ""
	}
	return
}

func pushLoop(tbname string, dataList []map[string]interface{}, key string, needExchage []string) {
	//先算出当前应该是这个表在内存(redis)中第几块
	counter := 1
	allCount := len(dataList)
	var sliceMap map[int64]interface{}
	for _, locdata := range dataList {
		tableInRedisSlice := counter / PerCacheSliceVolume
		yushu := counter % PerCacheSliceVolume
		for _, tojsfield := range needExchage {
			temp := map[string]interface{}{}
			json.Unmarshal([]byte(locdata[tojsfield].(string)), &temp)
			locdata[tojsfield] = temp
		}
		sliceMap[locdata[key].(int64)] = locdata
		if yushu == 0 || allCount == counter { //整数时把整个一起放入
			locKey := tbname + ":" + strconv.Itoa(tableInRedisSlice)
			err, _ := cache.Get(locKey)
			if err == cache.ErrCacheMiss {
				err = cache.Add(locKey, sliceMap, 0)
				if err != nil {
					fmt.Println("create mainkey to userid err(3):", err.Error())
					return
				}
			} else {
				//data2map, _ := json.Marshal(sliceMap)
				err = cache.Replace(locKey, sliceMap, 0)
				if err != nil {
					fmt.Println("create mainkey to userid err(4):", err.Error())
					return
				}
			}
			sliceMap = map[int64]interface{}{}
		}
		counter++
	}
}
