package cache

import (
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"strconv"
	"time"
)

//redis的结构体
//type redis struct {
//	Network string `toml:"network"`
//	Address string `toml:"address"`
//	Auth    string `toml:"auth"`
//}

var (
	rstore       *RedisStore
	ErrCacheMiss = errors.New("cache misss")
	ErrNotStored = errors.New("cache:key already exist")
)

type RedisCfg struct {
	Host       string
	Port       string
	Pwd        string
	DefaultExp time.Duration
}

func CreateRedisStore(rdcfg RedisCfg) {
	if rstore != nil {
		return
	}
	createRedisStore(rdcfg)
}

//初始化Conf信息
func createRedisStore(cfg RedisCfg) {
	rstore = NewRedisCache("tcp", cfg.Host+":"+cfg.Port, cfg.Pwd, cfg.DefaultExp)
}

type CacheStore interface {
	Get(key string, value interface{}) error
	Set(key string, value interface{}, expires time.Duration) error
	Add(key string, value interface{}, expires time.Duration) error
	Delete(key string) error
	Replace(key string, value interface{}, expire time.Duration) error
}

type RedisStore struct {
	pool              *redis.Pool
	defaultExpiration time.Duration
}

//允许外部也使用这个
func GetRedisStore() *RedisStore {
	return rstore
}

//新建RedisStore对象
func NewRedisCache(network string, address string, password string, defaultExpiration time.Duration) *RedisStore {
	pool := &redis.Pool{
		MaxActive:   0,
		MaxIdle:     5,
		Wait:        false,
		IdleTimeout: 300 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial(network, address)
			if err != nil {
				return nil, err
			}
			if len(password) > 0 { // 有密码的情况
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			} else { // 没有密码的时候 ping 连接
				if _, err := c.Do("ping"); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}
	return &RedisStore{pool: pool, defaultExpiration: defaultExpiration}
}

func (c *RedisStore) Get(key string, value interface{}) error {
	conn := c.pool.Get()
	defer conn.Close()
	reply, err := conn.Do("get", key)
	if err != nil {
		return err
	}
	if reply == nil {
		return ErrCacheMiss
	}
	bytes, err := redis.Bytes(reply, err)
	return deserialize(bytes, value) //反序列化操作
}

//保存数据
func (c *RedisStore) Set(key string, value interface{}, expires time.Duration) error {
	conn := c.pool.Get()
	defer conn.Close()
	return c.invoke(conn.Do, key, value, expires)
}

//添加数据
func (c *RedisStore) Add(key string, value interface{}, expires time.Duration) error {
	conn := c.pool.Get()
	defer conn.Close()
	b, err := redis.Bool(conn.Do("exists", key))
	if err != nil {
		return err
	}
	if b { //如果key 已经存在
		return ErrNotStored
	} else {
		return c.invoke(conn.Do, key, value, expires)
	}

}

//删除数据
func (c *RedisStore) Delete(key string) error {
	conn := c.pool.Get()
	defer conn.Close()
	b, err := redis.Bool(conn.Do("exists", key))
	if err != nil {
		return err
	}
	if !b { //key值不存在
		return ErrCacheMiss
	}
	_, err2 := conn.Do("del", key) //删除key值
	return err2
}
func (c *RedisStore) Replace(key string, value interface{}, expires time.Duration) error {
	conn := c.pool.Get()
	defer conn.Close()
	b, err := redis.Bool(conn.Do("exists", key))
	if err != nil {
		return err
	}
	if !b { //key值不存在
		return ErrCacheMiss
	}
	err = c.invoke(conn.Do, key, value, expires)
	if value == nil { //空值不能保存
		return ErrNotStored
	} else {
		return err
	}
}

func (c *RedisStore) invoke(f func(string, ...interface{}) (interface{}, error),
	key string, value interface{}, expires time.Duration) error {
	b, err := serialize(value) //序列化操作，序列化可以保存对象
	if err != nil {
		return err
	}
	if expires > 0 {
		_, err := f("setex", key, int32(expires/time.Second), b)
		return err
	} else {
		_, err := f("set", key, b)
		return err
	}
}

//返回bytes
func GetBytes(key string) (bytes []byte, err error) {
	conn := rstore.pool.Get()
	defer conn.Close()
	reply, err := conn.Do("get", key)
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, ErrCacheMiss
	}
	bytes, err = redis.Bytes(reply, err)
	return
}

//获取实际值  比如存入为字符那么返回字符
func Get(key string) (error, interface{}) {
	var value interface{}
	conn := rstore.pool.Get()
	defer conn.Close()
	reply, err := conn.Do("get", key)
	if err != nil {
		return err, nil
	}
	if reply == nil {
		return ErrCacheMiss, nil
	}
	bytes, err := redis.Bytes(reply, err)
	err = deserialize(bytes, &value)
	return err, value //反序列化操作
}

//保存数据
func Set(key string, value interface{}, expires time.Duration) error {
	conn := rstore.pool.Get()
	defer conn.Close()
	return rstore.invoke(conn.Do, key, value, expires)
}

//添加数据
func Add(key string, value interface{}, expires time.Duration) error {
	conn := rstore.pool.Get()
	defer conn.Close()
	b, err := redis.Bool(conn.Do("exists", key))
	if err != nil {
		return err
	}
	if b { //如果key 已经存在
		return ErrNotStored
	} else {
		return rstore.invoke(conn.Do, key, value, expires)
	}

}

//删除数据
func Delete(key string) error {
	conn := rstore.pool.Get()
	defer conn.Close()
	b, err := redis.Bool(conn.Do("exists", key))
	if err != nil {
		return err
	}
	if !b { //key值不存在
		return ErrCacheMiss
	}
	_, err2 := conn.Do("del", key) //删除key值
	return err2
}
func Replace(key string, value interface{}, expires time.Duration) error {
	conn := rstore.pool.Get()
	defer conn.Close()
	b, err := redis.Bool(conn.Do("exists", key))
	if err != nil {
		return err
	}
	if !b { //key值不存在
		return ErrCacheMiss
	}
	err = rstore.invoke(conn.Do, key, value, expires)
	if value == nil { //空值不能保存
		return ErrNotStored
	} else {
		return err
	}
}

//序列化
func serialize(value interface{}) ([]byte, error) {
	if bytes, ok := value.([]byte); ok {
		return bytes, nil
	}

	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return []byte(strconv.FormatInt(v.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return []byte(strconv.FormatUint(v.Uint(), 10)), nil
	}

	var b bytes.Buffer
	encoder := gob.NewEncoder(&b)
	if err := encoder.Encode(value); err != nil { //编码
		return nil, err
	}
	return b.Bytes(), nil
}

//反序列化
func deserialize(byt []byte, ptr interface{}) (err error) {
	if bytes, ok := ptr.(*[]byte); ok {
		*bytes = byt

		return nil
	}

	if v := reflect.ValueOf(ptr); v.Kind() == reflect.Ptr { // 通过反射得到ptr类型，判断ptr是指针类型
		switch p := v.Elem(); p.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64: //符号整型
			var i int64
			i, err = strconv.ParseInt(string(byt), 10, 64)
			if err != nil {
				return err
			} else {
				p.SetInt(i)
			}
			return nil

		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64: //无符号整型
			var i uint64
			i, err = strconv.ParseUint(string(byt), 10, 64)
			if err != nil {
				return err
			} else {
				p.SetUint(i)
			}
			return nil
		}
	}

	b := bytes.NewBuffer(byt)
	decoder := gob.NewDecoder(b)
	if err = decoder.Decode(ptr); err != nil { //解码
		return err
	}
	return nil
}
