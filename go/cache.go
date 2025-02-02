package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"time"

	"github.com/motoki317/sc"
)

var iconHashCache = sc.NewMust(fetchIconHash, 10*time.Minute, 10*time.Minute)

type txKeyType struct{}

var txKey = txKeyType{}

type DBconn interface {
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

func fetchIconHash(ctx context.Context, userID int64) (string, error) {
	db, ok := ctx.Value(txKey).(DBconn)
	if !ok {
		db = dbConn
	}

	var image []byte
	err := db.GetContext(ctx, &image, "SELECT image FROM icons WHERE user_id = ?", userID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
		image, err = os.ReadFile(fallbackImage)
		if err != nil {
			return "", err
		}
	}
	hash := sha256.Sum256(image)
	return hex.EncodeToString(hash[:]), nil
}
