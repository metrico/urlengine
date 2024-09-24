package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const DBDir = ".local/tmp"

type HivePathInfo struct {
	Partitions  map[string]string
	FileName    string
	IsHiveStyle bool
}

func main() {
	router := gin.Default()

	if err := os.MkdirAll(DBDir, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	router.GET("/*path", handleRequest)
	router.HEAD("/*path", handleRequest)
	router.POST("/*path", handlePostRequest)

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

func parseHivePath(hivePath string) HivePathInfo {
	parts := strings.Split(hivePath, "/")
	fileName := parts[len(parts)-1]
	partitions := make(map[string]string)

	for _, part := range parts[:len(parts)-1] {
		if kv := strings.SplitN(part, "=", 2); len(kv) == 2 {
			partitions[kv[0]] = kv[1]
		}
	}

	return HivePathInfo{
		Partitions:  partitions,
		FileName:    fileName,
		IsHiveStyle: len(partitions) > 0,
	}
}

func getFilePath(requestPath string) string {
	info := parseHivePath(requestPath)

	if info.IsHiveStyle {
		path := DBDir
		for k, v := range info.Partitions {
			path = filepath.Join(path, fmt.Sprintf("%s=%s", k, v))
		}
		return filepath.Join(path, info.FileName)
	}

	return filepath.Join(DBDir, requestPath)
}

func handleRequest(c *gin.Context) {
	requestPath := c.Param("path")
	if requestPath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Path is required"})
		return
	}

	info := parseHivePath(requestPath)

	if strings.Contains(requestPath, "*") {
		handleWildcardRequest(c, info)
	} else {
		handleExactPathRequest(c, info)
	}
}

func handleWildcardRequest(c *gin.Context, info HivePathInfo) {
	pattern := buildSearchPattern(info)
	log.Printf("Searching for pattern: %s", pattern)

	matchingFiles, err := findMatchingFiles(pattern)
	if err != nil {
		log.Printf("Error finding matching files: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		return
	}

	log.Printf("Found %d matching files", len(matchingFiles))

	if len(matchingFiles) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "No matching files found"})
		return
	}

	if c.Request.Method == "HEAD" {
		handleWildcardHeadRequest(c, matchingFiles)
	} else if len(matchingFiles) == 1 {
		handleSingleFile(c, matchingFiles[0])
	} else {
		handleMultipleFiles(c, matchingFiles)
	}
}

func buildSearchPattern(info HivePathInfo) string {
	basePath := DBDir
	for k, v := range info.Partitions {
		if v == "*" {
			basePath = filepath.Join(basePath, fmt.Sprintf("%s=*", k))
		} else {
			basePath = filepath.Join(basePath, fmt.Sprintf("%s=%s", k, v))
		}
	}
	return filepath.Join(basePath, info.FileName)
}

func handleWildcardHeadRequest(c *gin.Context, matchingFiles []string) {
	var totalSize int64
	var lastModified time.Time

	for _, file := range matchingFiles {
		info, err := os.Stat(file)
		if err != nil {
			log.Printf("Error getting file info for %s: %v", file, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get file info"})
			return
		}
		totalSize += info.Size()
		if info.ModTime().After(lastModified) {
			lastModified = info.ModTime()
		}
	}

	log.Printf("HEAD request: Total size: %d, Last modified: %s, Matched files: %d",
		totalSize, lastModified.UTC().Format(http.TimeFormat), len(matchingFiles))

	c.Header("Content-Length", fmt.Sprintf("%d", totalSize))
	c.Header("Last-Modified", lastModified.UTC().Format(http.TimeFormat))
	c.Header("Accept-Ranges", "bytes")
	c.Header("Content-Type", "application/octet-stream")
	c.Header("X-Matched-Files", fmt.Sprintf("%d", len(matchingFiles)))
	c.Status(http.StatusOK)
}

func findMatchingFiles(pattern string) ([]string, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("error finding matching files: %v", err)
	}

	var result []string
	for _, match := range matches {
		info, err := os.Stat(match)
		if err != nil {
			log.Printf("Error accessing path %s: %v", match, err)
			continue
		}
		if !info.IsDir() {
			result = append(result, match)
		}
	}

	return result, nil
}

func handleExactPathRequest(c *gin.Context, info HivePathInfo) {
	filePath := getFilePath(c.Param("path"))

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
		}
		return
	}

	if fileInfo.IsDir() {
		// handleDirectory(c, filePath)
		// Return an empty response for directory requests
		c.String(http.StatusOK, "")
	} else {
		handleSingleFile(c, filePath)
	}
}

func handleSingleFile(c *gin.Context, filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to open file"})
		return
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	c.Header("Content-Length", fmt.Sprintf("%d", fileInfo.Size()))
	c.Header("Accept-Ranges", "bytes")
	c.Header("Content-Type", "application/octet-stream")

	if c.Request.Method == "HEAD" {
		c.Status(http.StatusOK)
		return
	}

	http.ServeContent(c.Writer, c.Request, fileInfo.Name(), fileInfo.ModTime(), file)
}

func handleMultipleFiles(c *gin.Context, matchingFiles []string) {
	relativeFiles := make([]string, len(matchingFiles))
	for i, file := range matchingFiles {
		relativeFiles[i], _ = filepath.Rel(DBDir, file)
	}

	if c.Request.Method == "HEAD" {
		c.Header("X-Matched-Files", fmt.Sprintf("%d", len(relativeFiles)))
		c.Status(http.StatusOK)
	} else {
		c.JSON(http.StatusOK, relativeFiles)
	}
}

func handleDirectory(c *gin.Context, dirPath string) {
	files, err := filepath.Glob(filepath.Join(dirPath, "*"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read directory"})
		return
	}

	relativeFiles := make([]string, len(files))
	for i, file := range files {
		relativeFiles[i], _ = filepath.Rel(DBDir, file)
	}

	c.JSON(http.StatusOK, relativeFiles)
}

func handlePostRequest(c *gin.Context) {
	requestPath := c.Param("path")
	if requestPath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Path is required"})
		return
	}

	filePath := getFilePath(requestPath)
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create directory"})
		return
	}

	file, err := os.Create(filePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create file"})
		return
	}
	defer file.Close()

	if _, err := io.Copy(file, c.Request.Body); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to write file"})
		return
	}

	relativePath, _ := filepath.Rel(DBDir, filePath)
	c.JSON(http.StatusOK, gin.H{"success": true, "path": relativePath})
}
