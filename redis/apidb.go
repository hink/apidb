package apidb

import (
	"github.com/go-redis/redis"
	"fmt"
)

// Database bolt wrapper
type Database struct {
	Redisdatabase *redis.Client
}

// Open new APIDB and open the database
func Open(address string, password string, buckets []string) (db *Database, err error) {
	db = new(Database)
	db.Redisdatabase = redis.NewClient(&redis.Options{
		Addr:     address + ":6379",
		Password: password, // no password set
		DB:       0,  // use default DB
	})

	if db.Redisdatabase == nil {
		return nil,fmt.Errorf("error connecting to redis db")
	}
	if buckets != nil {
		for _, bucket := range buckets {
			_, err = db.CreateBucket(bucket)
			if err != nil {
				return
			}
		}
	}
	return
}

// Close the bolt database
func (a *Database) Close() error {
	return a.Redisdatabase.Close()
}

// CreateBucket creates a new boltdb bucket
func (a *Database) CreateBucket(name string) (success bool, err error) {
	return true,nil
}

// DeleteBucket delets a database bucket
func (a *Database) DeleteBucket(name string) error {
	resp := a.Redisdatabase.Del(name)
	return resp.Err()
}

// EmptyBucket empties an existing bucket
func (a *Database) EmptyBucket(name string) error {
	err := a.DeleteBucket(name)
	if err != nil {
		return err
	}
	_, err = a.CreateBucket(name)
	if err != nil {
		return err
	}

	return nil
}

// Save a record
func (a *Database) Save(bucket string, key, value []byte) (err error) {
	err = a.Update(bucket, key, value)
	return
}

// Save a record
func (a *Database) SaveToList(listName string, value string, insertInFront bool) (err error) {
	//err = a.Update(bucket, key, value)
	if insertInFront {
		res := a.Redisdatabase.LPush(listName, value)
		return res.Err()

	}
	res := a.Redisdatabase.RPush(listName, value)
	return res.Err()
}

// Update a record (or save if doesn't exist)
func (a *Database) Update(bucket string, key, value []byte) (err error) {
	res := a.Redisdatabase.HSet(bucket,(string)(key),value)
	return res.Err()
}

// Get and return its value
func (a *Database) Get(bucket string, key []byte) (val []byte) {
	res := a.Redisdatabase.HGet(bucket,(string)(key))
	val = ([]byte)(res.Val())
	return
}

// GetAll values in a bucket
func (a *Database) GetAll(bucket string) (vals map[string]string, err error) {
	vals, err = a.GetAllWithKeys(bucket)
	return
	/*vals = [][]byte{}

	a.BoltDatabase.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		b.ForEach(func(k, v []byte) error {
			vals = append(vals, v)
			return nil
		})
		return nil
	})

	return*/
}

// GetAllWithKeys values in a bucket
func (a *Database) GetAllWithKeys(bucket string) (vals map[string]string, err error) {
	res := a.Redisdatabase.HGetAll(bucket)
	return res.Val(), res.Err()
}

func (a *Database) GetAllInList(listName string) (vals []string, err error) {
	res := a.Redisdatabase.LRange(listName,0,-1)
	return res.Val(), res.Err()
}

// Delete key
func (a *Database) Delete(bucket string, key []byte) (err error) {
	res := a.Redisdatabase.HDel(bucket,(string)(key))
	return res.Err()
}
