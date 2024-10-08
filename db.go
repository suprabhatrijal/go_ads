package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"
	pgxvector "github.com/pgvector/pgvector-go/pgx"
)

type PostgresDatabase struct {
	pool *pgxpool.Pool
}

func NewPostgresDatabase(connection_string string) *PostgresDatabase {
	config, err := pgxpool.ParseConfig(connection_string )
	if err != nil { fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	config.AfterConnect = func(context context.Context, conn *pgx.Conn) error {
		err = pgxvector.RegisterTypes(context, conn)
		return err
	}

  config.MaxConns = 50
  config.MinConns = 50

	pool, err := pgxpool.NewWithConfig(context.Background(), config)

	_, err = pool.Exec(context.Background(), "CREATE EXTENSION IF NOT EXISTS vector")
	_, err = pool.Exec(context.Background(), "CREATE EXTENSION IF NOT EXISTS postgis;")
	_, err = pool.Exec(context.Background(), "CREATE EXTENSION IF NOT EXISTS vector;")
	_, err = pool.Exec(context.Background(), "CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE;")

	_, err = pool.Exec(context.Background(), `
        CREATE TABLE IF NOT EXISTS ADS (
            adId BIGSERIAL PRIMARY KEY,
            campaignName TEXT,
            campaignDetails TEXT,
            startDateTime TIMESTAMP,
            endDateTime TIMESTAMP,
            amount DECIMAL,
            bidAmount DECIMAL,
            enableConversion BOOLEAN,
            conversionLink TEXT,
            positiveKeywords TEXT,
            negativeKeywords TEXT,
            embeddings vector(384),
            assetType TEXT,
            assetName TEXT
        );
    `)
	_, err = pool.Exec(context.Background(), `
        CREATE TABLE IF NOT EXISTS ADS_GEOM (
            adId INT,
            locationId BIGSERIAL PRIMARY KEY,
            placeName TEXT,
            latitude DECIMAL,
            longitude DECIMAL,
            radius DECIMAL,
            geometry GEOMETRY,
            CONSTRAINT fk_ad_id
                FOREIGN KEY (adId) REFERENCES ADS(adId)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        );
    `)
	_, err = pool.Exec(context.Background(), `
        CREATE TABLE IF NOT EXISTS EMBEDDING_CACHE (
            userId BIGSERIAL PRIMARY KEY,
            keywords TEXT,
            embeddings vector(384));
   `)

	return &PostgresDatabase{pool: pool}
}

func (p *PostgresDatabase) GetPreferenceEmbeddings(requestUserId string) pgvector.Vector {
	var embeddings pgvector.Vector
	err := p.pool.QueryRow(context.Background(), "SELECT embeddings FROM EMBEDDING_CACHE WHERE userId = $1;", requestUserId).Scan(&embeddings)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow faild: %v\n", err)
		os.Exit(1)
	}
	return embeddings
}

func (p *PostgresDatabase) GetAuctionResults(latitude int, longitude int, embedding pgvector.Vector) int64 {
	rows, err := p.pool.Query(context.Background(), `
                    WITH adsInArea AS 
                    (
                       SELECT
                          DISTINCT adId, bidAmount, embeddings 
                       FROM
                          ads 
                          JOIN
                             (SELECT * FROM ads_geom WHERE geometry ~ ST_MakePoint($1,$2)) USING(adId) 
                    )
                    ,
                    relevantAds AS 
                    (
                       SELECT
                          * 
                       FROM
                          adsInArea 
                       ORDER BY
                          adsInArea.embeddings <=> $3 LIMIT 10
                    )
                    SELECT
                       adId
                    FROM
                       relevantAds 
                    ORDER BY
                       bidAmount DESC LIMIT 2
    `, latitude, longitude, embedding)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query faild: %v\n", err)
		os.Exit(1)
	}

	defer rows.Close()
	var rowSlice []int64
	for rows.Next() {
		var id int64
		err := rows.Scan(&id)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Parsing rows failed: %v\n", err)
			os.Exit(1)
		}
		rowSlice = append(rowSlice, id)
	}

	if len(rowSlice) == 0 {
		return -1
	} else if len(rowSlice) == 1 {
		return rowSlice[0]
	}
	return rowSlice[0]
}
