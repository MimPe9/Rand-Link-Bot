package storage

import (
	"bot/storage"
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type PostgresStorage struct {
	db *sql.DB
}

func NewPostgresStorage(path string) (*PostgresStorage, error) {
	log.Printf("Connection string: %s", path) // Добавьте эту строку для отладки

	var db *sql.DB
	var err error

	// Попытки подключения с задержкой
	for i := 0; i < 10; i++ {
		db, err = sql.Open("postgres", path)
		if err != nil {
			log.Printf("Attempt %d: sql.Open error: %v", i+1, err)
			time.Sleep(2 * time.Second)
			continue
		}

		err = db.Ping()
		if err == nil {
			log.Println("Successfully connected to database")
			return &PostgresStorage{db}, nil
		}

		log.Printf("Attempt %d: Failed to ping database: %v", i+1, err)
		time.Sleep(2 * time.Second)
	}

	return nil, fmt.Errorf("failed to connect to database after retries: %w", err)
}

func (s *PostgresStorage) Save(ctx context.Context, page *storage.Page) error {
	q := `INSERT INTO pages (url, user_name) VALUES ($1, $2)`

	if _, err := s.db.ExecContext(ctx, q, page.URL, page.UserName); err != nil {
		return fmt.Errorf("can't save page: %w", err)
	}
	return nil
}

func (s *PostgresStorage) PickRandom(ctx context.Context, userName string) (*storage.Page, error) {
	q := `SELECT url FROM pages WHERE user_name = $1 ORDER BY RANDOM() LIMIT 1`

	var url string

	err := s.db.QueryRowContext(ctx, q, userName).Scan(&url)
	if err == sql.ErrNoRows {
		return nil, storage.ErrNoSavedPages
	}
	if err != nil {
		return nil, fmt.Errorf("can't pick random page: %w", err)
	}
	return &storage.Page{
		URL:      url,
		UserName: userName,
	}, nil
}

func (s *PostgresStorage) Remove(ctx context.Context, page *storage.Page) error {
	q := `DELETE FROM pages WHERE url = $1 AND user_name = $2`

	if _, err := s.db.ExecContext(ctx, q, page.URL, page.UserName); err != nil {
		return fmt.Errorf("can't remove page: %w", err)
	}
	return nil
}

func (s *PostgresStorage) IsExists(ctx context.Context, page *storage.Page) (bool, error) {
	q := `SELECT COUNT(*) FROM pages WHERE url = $1 AND user_name = $2`

	var count int

	if err := s.db.QueryRowContext(ctx, q, page.URL, page.UserName).Scan(&count); err != nil {
		return false, fmt.Errorf("can't check if page exists: %w", err)
	}

	return count > 0, nil
}

func (s *PostgresStorage) Init(ctx context.Context) error {
	q := `CREATE TABLE IF NOT EXISTS pages (url TEXT, user_name TEXT)`

	if _, err := s.db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("can't create table: %w", err)
	}
	return nil
}

func (s *PostgresStorage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
