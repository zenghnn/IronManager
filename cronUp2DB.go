package IronManager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/robfig/cron"
	"github.com/zenghnn/IronManager/cache"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

var rwLock *sync.RWMutex

func (jav *Javis) InitCron() {
	rwLock := new(sync.RWMutex)
	c := cron.New()
	c.AddFunc("0 */1 * * * ?", func() {
		go func() {
			rwLock.Lock()
			copyUpRelation := jav.updateRelation
			copyCreatRelation := jav.createRelation
			jav.updateRelation = map[string]map[int64][]int{}
			jav.createRelation = map[string][]int64{}
			rwLock.Unlock()

			//要更新的数据
			//updateSqls := []string{}
			for tbprefix, user2update := range copyUpRelation {
				tbStruct := jav.tableStruct[tbprefix]
				mainKey := jav.TbMainKey[tbprefix]
				for uid, fieldIdxs := range user2update {
					uTbMainIdx, sliceIdx := GetCacheRouter(uid)
					tbname := tbprefix + "_" + strconv.Itoa(uTbMainIdx)
					locKey := tbname + ":" + strconv.Itoa(sliceIdx)
					cacheSliceBytes, err := cache.GetBytes(locKey)
					if err != nil && err == cache.ErrCacheMiss {
						return
					}
					dataSlice := map[int64]interface{}{}
					err = json.Unmarshal(cacheSliceBytes, &dataSlice)
					userData := dataSlice[uid].(map[string]interface{})

					sql := "UPDATE " + tbname + " SET "
					upFieldCount := len(fieldIdxs)
					for locCount, fieldIdx := range fieldIdxs {
						tag := tbStruct.Field(fieldIdx).Tag.Get("gorm")
						tagInfos := strings.Split(tag, ";")
						column := ""
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
						if strings.Contains(typestr, "int") || typestr == "bigint" {
							sql += fmt.Sprintf(" `%s`=%d", column, userData[column])
						} else {
							columnBytes, err := json.Marshal(userData[column])
							if err != nil {
								continue
							}
							sql += fmt.Sprintf(" `%s`='%s'", column, string(columnBytes))
						}
						if (upFieldCount - 1) > locCount {
							sql += ","
						}
					}
					sql += fmt.Sprintf(" WHERE %s=%d;", mainKey, uid)
					//updateSqls = append(updateSqls,sql)
					err = jav.Main_IS.DB.Exec(sql).Error
					if err != nil {
						fmt.Println("update " + tbname + " :" + strconv.Itoa(int(uid)) + "err:" + err.Error())
					}
				}
			}

			//新的数据
			insertByTb := map[string]bytes.Buffer{}
			tbFields := map[string][]string{}
			for tbprefix, ids := range copyCreatRelation {
				tbStruct := jav.tableStruct[tbprefix]
				//mainKey := jav.TbMainKey[tbprefix]
				locBuffer, ok := insertByTb[tbprefix]
				locFields := tbFields[tbprefix]
				thisTBisFirs := false
				if !ok {
					locBuffer = bytes.Buffer{}
					//locBuffer.WriteString("insert into `")
					thisTBisFirs = true
				}
				if len(ids) == 0 {
					continue
				}
				for _, uid := range ids {
					uTbMainIdx, sliceIdx := GetCacheRouter(uid)
					tbname := tbprefix + "_" + strconv.Itoa(uTbMainIdx)
					//if thisTBisFirs{
					//	locBuffer.WriteString(tbname+"` (")
					//}
					locKey := tbname + ":" + strconv.Itoa(sliceIdx)
					cacheSliceBytes, err := cache.GetBytes(locKey)
					if err != nil && err == cache.ErrCacheMiss {
						return
					}
					dataSlice := map[int64]interface{}{}
					err = json.Unmarshal(cacheSliceBytes, &dataSlice)
					userData := dataSlice[uid].(map[string]interface{})
					locBuffer.WriteString("(")
					insertFieldCount := tbStruct.NumField()
					for i := 0; i < insertFieldCount; i++ {
						tag := tbStruct.Field(i).Tag.Get("gorm")
						tagInfos := strings.Split(tag, ";")
						column := ""
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

						if strings.Contains(typestr, "int") || typestr == "bigint" {
							locBuffer.WriteString(fmt.Sprintf("%d", userData[column]))
						} else if typestr == "json" {
							columnBytes, err := json.Marshal(userData[column])
							if err != nil {
								continue
							}
							locBuffer.WriteString(fmt.Sprintf("'%s'", string(columnBytes)))
						} else {
							locBuffer.WriteString(fmt.Sprintf("'%s'", userData[column]))
						}
						if (insertFieldCount - 1) > i {
							locBuffer.WriteString(",")
						} else {
							locBuffer.WriteString("),")
						}
						if thisTBisFirs {
							locFields = append(locFields, column)
						}
					}

				}
				insertByTb[tbprefix] = locBuffer
				tbFields[tbprefix] = locFields
			}
			for hereTbName, buffer := range insertByTb {
				tbfiels := tbFields[hereTbName]
				thissql := "insert into `" + hereTbName + "` (`" + strings.Join(tbfiels, "`,`") + ") values " + buffer.String()
				r, size := utf8.DecodeLastRuneInString(thissql)
				if r == utf8.RuneError && (size == 0 || size == 1) {
					size = 0
				}
				thissql = thissql[:len(thissql)-size]
				err := jav.Main_IS.DB.Exec(thissql).Error
				if err != nil {
					fmt.Println("insert(create) " + hereTbName + " err:" + err.Error())
				}
			}

		}()
	})
	c.Start()
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
		defaultV = map[string]interface{}{}
	case "varchar":
	case "datetime":
	case "timestamp":
		defaultV = ""
	}
	return
}
