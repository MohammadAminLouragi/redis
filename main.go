package main

import (
	"encoding/gob"
	"os"
	"sync"
)

// DataBase represents a thread-safe in-memory key-value store.
type DataBase struct {
	data map[string]any // The map to store key-value pairs.
	lock sync.RWMutex   // A read-write mutex to ensure thread safety.
}

// NewDataBase initializes and returns a new instance of DataBase.
func NewDataBase() *DataBase {
	return &DataBase{
		data: make(map[string]any), // Initialize the map.
	}
}

// Set adds or updates a key-value pair in the database.
func (db *DataBase) Set(key string, value any) {
	db.lock.Lock()         // Acquire a write lock.
	defer db.lock.Unlock() // Release the lock when the function exits.
	db.data[key] = value   // Store the key-value pair.
}

// Get retrieves the value associated with a key from the database.
// Returns the value and a boolean indicating if the key exists.
func (db *DataBase) Get(key string) (any, bool) {
	db.lock.RLock()         // Acquire a read lock.
	defer db.lock.RUnlock() // Release the lock when the function exits.
	value, exists := db.data[key]
	return value, exists
}

// Persist saves the current state of the database to a file.
func (db *DataBase) Persist(fileName string) error {
	db.lock.RLock()         // Acquire a read lock to ensure data consistency.
	defer db.lock.RUnlock() // Release the lock when the function exits.

	file, err := os.Create(fileName) // Create or overwrite the file.
	if err != nil {
		return err // Return the error if file creation fails.
	}
	defer file.Close() // Ensure the file is closed after writing.

	encode := gob.NewEncoder(file) // Create a new encoder for the file.
	if err := encode.Encode(db.data); err != nil {
		return err // Return the error if encoding fails.
	}
	return nil // Return nil if the operation is successful.
}

// Load restores the database state from a file.
func (db *DataBase) Load(fileName string) error {
	db.lock.Lock()         // Acquire a write lock to modify the database.
	defer db.lock.Unlock() // Release the lock when the function exits.

	file, err := os.Open(fileName) // Open the file for reading.
	if err != nil {
		return err // Return the error if file opening fails.
	}
	defer file.Close() // Ensure the file is closed after reading.

	decode := gob.NewDecoder(file) // Create a new decoder for the file.
	if err := decode.Decode(&db.data); err != nil {
		return err // Return the error if decoding fails.
	}
	return nil // Return nil if the operation is successful.
}

func main() {
	// Create a new instance of the database.
	db := NewDataBase()

	// Add some key-value pairs to the database.
	db.Set("key1", "value1")
	db.Set("key2", "value2")

	// Persist the database to a file.
	err := db.Persist("database.gob")
	if err != nil {
		panic(err) // Terminate the program if an error occurs.
	}

	// Load the database from the file.
	err = db.Load("database.gob")
	if err != nil {
		panic(err) // Terminate the program if an error occurs.
	}

	// Retrieve a value from the database.
	value1, exists := db.Get("key2")
	if exists {
		println("key1:", value1.(string)) // Print the value if the key exists.
	} else {
		println("key1 does not exist") // Print a message if the key does not exist.
	}
}
