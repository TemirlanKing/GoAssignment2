package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

var (
	db          *sql.DB
	redisClient *redis.Client
)

type Product struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Price       float64 `json:"price"`
}

func getProduct(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id, _ := strconv.Atoi(params["id"])

	// Check if product is cached in Redis
	cachedData, err := redisClient.Get(r.Context(), "products:"+strconv.Itoa(id)).Result()
	if err == nil {
		var product Product
		if err := json.Unmarshal([]byte(cachedData), &product); err != nil {
			log.Println("Error unmarshaling cached product:", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(product)
		return
	} else if err != redis.Nil {
		log.Println("Error retrieving product from Redis:", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// If not cached, query from database
	row := db.QueryRow("SELECT id, name, description, price FROM products WHERE id = ?", id)
	var product Product
	err = row.Scan(&product.ID, &product.Name, &product.Description, &product.Price)
	if err != nil {
		log.Println("Error retrieving product from database:", err)
		http.Error(w, "Product not found", http.StatusNotFound)
		return
	}

	// Store data in Redis with TTL
	jsonData, err := json.Marshal(product)
	if err != nil {
		log.Println("Error marshaling product data:", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	if err := redisClient.Set(r.Context(), "products:"+strconv.Itoa(id), jsonData, 1*time.Hour).Err(); err != nil {
		log.Println("Error storing product in Redis:", err)
		// Don't return error to client, since data retrieval was successful
	}

	json.NewEncoder(w).Encode(product)
}

func main() {
	// Connect to Redis
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Connect to PostgreSQL
	var err error
	db, err = sql.Open("mysql", "root:@tcp(localhost:3306)/task_management")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Initialize router
	router := mux.NewRouter()

	// Define routes
	router.HandleFunc("/products/{id}", getProduct).Methods("GET")

	// Start server
	log.Fatal(http.ListenAndServe(":8000", router))
}
