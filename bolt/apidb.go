package apidb

import (
	"github.com/boltdb/bolt"
)

// Database bolt wrapper
type Database struct {
	BoltDatabase *bolt.DB
}

// Open new APIDB and open the database
func Open(path string, buckets []string) (db *Database, err error) {
	db = new(Database)
	db.BoltDatabase, err = bolt.Open(path, 0600, nil)

	if err != nil {
		return
	}

	for _, bucket := range buckets {
		_, err = db.CreateBucket(bucket)
		if err != nil {
			return
		}
	}
	return
}

// Close the bolt database
func (a *Database) Close() error {
	return a.BoltDatabase.Close()
}

// CreateBucket creates a new boltdb bucket
func (a *Database) CreateBucket(name string) (bucket *bolt.Bucket, err error) {
	err = a.BoltDatabase.Update(func(tx *bolt.Tx) error {
		bucket, err = tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return err
		}
		return nil
	})
	return
}

// DeleteBucket delets a database bucket
func (a *Database) DeleteBucket(name string) error {
	err := a.BoltDatabase.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket([]byte(name))
		if err != nil {
			return err
		}
		return nil
	})
	return err
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

// Update a record (or save if doesn't exist)
func (a *Database) Update(bucket string, key, value []byte) (err error) {
	err = a.BoltDatabase.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		err = bucket.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

// Get and return its value
func (a *Database) Get(bucket string, key []byte) (val []byte) {
	a.BoltDatabase.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		val = b.Get(key)
		return nil
	})
	return
}

// GetAll values in a bucket
func (a *Database) GetAll(bucket string) (vals [][]byte, err error) {
	vals = [][]byte{}

	a.BoltDatabase.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		b.ForEach(func(k, v []byte) error {
			vals = append(vals, v)
			return nil
		})
		return nil
	})

	return
}

// GetAllWithKeys values in a bucket
func (a *Database) GetAllWithKeys(bucket string) (vals map[string][]byte, err error) {
	vals = make(map[string][]byte)

	a.BoltDatabase.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		b.ForEach(func(k, v []byte) error {
			vals[string(k)] = v
			return nil
		})
		return nil
	})

	return
}

// Delete key
func (a *Database) Delete(bucket string, key []byte) (err error) {
	if err = a.BoltDatabase.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bucket)).Delete(key)
	}); err != nil {
		return
	}
	return
}
