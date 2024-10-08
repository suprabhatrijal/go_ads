package main

import (
	"fmt"
	"net/http"
)

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
}

func main() {
	connection_string := "host=localhost port=5432 dbname=postgres user=postgres password=password connect_timeout=10 sslmode=prefer"
	db := NewPostgresDatabase(connection_string)

	server := NewAPIServer(":8080", db)

	server.Run()
}
