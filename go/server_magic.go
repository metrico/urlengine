package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
	"github.com/metrico/pasticca/paste"
)

type PasteReference struct {
	Fingerprint string
	Hash        string
}

type Server struct {
	cache sync.Map
	db    *sql.DB
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	dbPath := getEnv("DB_PATH", "pasticca.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	server := &Server{db: db}
	if err := server.initDatabase(); err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	if err := server.resyncCache(); err != nil {
		log.Fatalf("Failed to resync cache: %v", err)
	}

	router := gin.Default()

	router.GET("/*path", server.handleRequest)
	router.HEAD("/*path", server.handleRequest)
	router.POST("/*path", server.handlePostRequest)

	port := getEnv("PORT", "3000")
	log.Printf("Server is running on http://0.0.0.0:%s\n", port)
	log.Fatal(router.Run(":" + port))
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func (s *Server) initDatabase() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS paste_references (
			path TEXT PRIMARY KEY,
			fingerprint TEXT NOT NULL,
			hash TEXT NOT NULL
		)
	`)
	return err
}

func (s *Server) resyncCache() error {
	rows, err := s.db.Query("SELECT path, fingerprint, hash FROM paste_references")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var path, fingerprint, hash string
		if err := rows.Scan(&path, &fingerprint, &hash); err != nil {
			return err
		}
		s.cache.Store(path, PasteReference{Fingerprint: fingerprint, Hash: hash})
	}

	return rows.Err()
}

func (s *Server) handleRequest(c *gin.Context) {
	requestPath := c.Param("path")
	if requestPath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Path is required"})
		return
	}

	if strings.Contains(requestPath, "*") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Wildcard paths are not supported"})
		return
	}

	// Check cache first
	if ref, ok := s.cache.Load(requestPath); ok {
		pasteRef := ref.(PasteReference)
		content, isEncrypted, err := paste.Load(pasteRef.Fingerprint, pasteRef.Hash)
		if err == nil {
			handleContent(c, content, isEncrypted)
			return
		}
		// If there's an error, we'll fall through to a 404
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "Not found"})
}

func handleContent(c *gin.Context, content string, isEncrypted bool) {
	contentType := "application/json"
	if !json.Valid([]byte(content)) {
		contentType = "text/plain"
	}

	c.Header("Content-Type", contentType)
	c.Header("Content-Length", fmt.Sprintf("%d", len(content)))
	c.Header("Accept-Ranges", "bytes")
	if isEncrypted {
		c.Header("X-Encrypted", "true")
	}

	if c.Request.Method == "HEAD" {
		c.Status(http.StatusOK)
		return
	}

	c.String(http.StatusOK, content)
}

func (s *Server) handlePostRequest(c *gin.Context) {
	requestPath := c.Param("path")
	if requestPath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Path is required"})
		return
	}

	body, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Error reading request body"})
		return
	}

	// For simplicity, we're not encrypting the content here
	fingerprint, hashWithAnchor, err := paste.Save(string(body), "", "", false)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Error saving paste"})
		return
	}

	hash := strings.Split(hashWithAnchor, "#")[0] // Remove anchor if present

	// Update cache
	pasteRef := PasteReference{Fingerprint: fingerprint, Hash: hash}
	s.cache.Store(requestPath, pasteRef)

	// Update database
	_, err = s.db.Exec("INSERT OR REPLACE INTO paste_references (path, fingerprint, hash) VALUES (?, ?, ?)", requestPath, fingerprint, hash)
	if err != nil {
		log.Printf("Error updating database: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Error updating database"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "path": requestPath})
}
