package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type APIServer struct {
	addr             string
	postgresDatabase *PostgresDatabase
}

type AdsAuctionReqBody struct {
	Latitude         int    `json:"latitude"`
	Longitude        int    `json:"longitude"`
	PositiveKeywords string `json:"positive_keywords"`
	NegativeKeywords string `json:"negative_keywords"`
	Method           int    `json:"method"`
}

type AuctionResponse struct {
	AdId int64 `json:"ad_id"`
}

func NewAPIServer(addr string, postgresDatabase *PostgresDatabase) *APIServer {
	return &APIServer{
		addr:             addr,
		postgresDatabase: postgresDatabase,
	}
}

func (s *APIServer) Run() error {
	router := http.NewServeMux()

	router.HandleFunc("GET /ads_auction", func(w http.ResponseWriter, r *http.Request) {

		var auction AdsAuctionReqBody

		userId := "1534556"

		err := json.NewDecoder(r.Body).Decode(&auction)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		embeddings := s.postgresDatabase.GetPreferenceEmbeddings(userId)

		auctionWinner := s.postgresDatabase.GetAuctionResults(auction.Latitude, auction.Longitude, embeddings)
		response := AuctionResponse{AdId: auctionWinner}

		response_bytes, err := json.Marshal(response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Write(response_bytes)
	})

	server := http.Server{
		Addr:    s.addr,
		Handler: router,
	}
	log.Printf("Server has started %s", s.addr)
	return server.ListenAndServe()
}
