package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

const DBDir = ".local/tmp"

type StorageConfig struct {
	s3Client    *s3.Client
	bucketName  string
	useS3       bool
	uploadMutex sync.Mutex
}

type HivePathInfo struct {
	Partitions  map[string]string
	FileName    string
	IsHiveStyle bool
}

var storage StorageConfig

func initStorage() error {
    endpoint := getEnv("S3_ENDPOINT", "")
    bucketName := getEnv("S3_BUCKET", "")
    accessKey := getEnv("S3_ACCESS_KEY", "")
    secretKey := getEnv("S3_SECRET_KEY", "")
    useS3 := endpoint != "" && bucketName != "" && accessKey != "" && secretKey != ""

    if useS3 {
        log.Printf("Initializing MinIO storage with endpoint: %s, bucket: %s", endpoint, bucketName)
        
        cfg := aws.Config{
            Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
            EndpointResolver: aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
                return aws.Endpoint{
                    URL: endpoint,
                    Source: aws.EndpointSourceCustom,
                }, nil
            }),
        }

        client := s3.NewFromConfig(cfg, func(o *s3.Options) {
            o.UsePathStyle = true
        })

        // Verify MinIO connection
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        
        _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
            Bucket: aws.String(bucketName),
        })
        
        if err != nil {
            log.Printf("Failed to connect to MinIO: %v", err)
            return fmt.Errorf("MinIO connection test failed: %v", err)
        }

        storage = StorageConfig{
            s3Client:   client,
            bucketName: bucketName,
            useS3:      true,
        }
        
        log.Printf("Successfully connected to MinIO storage")
    } else {
        storage = StorageConfig{
            useS3: false,
        }
        log.Println("Running in local storage mode")
    }

    return nil
}

func main() {
	if err := initStorage(); err != nil {
		log.Fatal(err)
	}

	router := gin.Default()

	config := cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "HEAD"},
		AllowHeaders:     []string{"*"},
		ExposeHeaders:    []string{},
		MaxAge:           5000,
	}

	router.Use(cors.New(config))

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

func fetchFromS3(s3Path string, localPath string) error {
	if !storage.useS3 {
		return fmt.Errorf("S3 storage not configured")
	}

	ctx := context.Background()
	
	if err := os.MkdirAll(filepath.Dir(localPath), os.ModePerm); err != nil {
		return err
	}

	file, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	downloader := manager.NewDownloader(storage.s3Client)
	_, err = downloader.Download(ctx, file, &s3.GetObjectInput{
		Bucket: aws.String(storage.bucketName),
		Key:    aws.String(s3Path),
	})

	if err != nil {
		os.Remove(localPath)
		return fmt.Errorf("failed to download from S3: %v", err)
	}

	log.Printf("Successfully downloaded %s from S3", s3Path)
	return nil
}

func uploadToS3(localPath string, s3Path string) {
    if !storage.useS3 {
        log.Printf("MinIO not configured, skipping upload")
        return
    }

    // Clean the path
    s3Path = strings.TrimPrefix(s3Path, "/")
    log.Printf("Starting MinIO upload - local: %s, remote path: %s", localPath, s3Path)

    storage.uploadMutex.Lock()
    defer storage.uploadMutex.Unlock()

    file, err := os.Open(localPath)
    if err != nil {
        log.Printf("Failed to open file for MinIO upload: %v", err)
        return
    }
    defer file.Close()

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    uploader := manager.NewUploader(storage.s3Client)
    result, err := uploader.Upload(ctx, &s3.PutObjectInput{
        Bucket: aws.String(storage.bucketName),
        Key:    aws.String(s3Path),
        Body:   file,
    })

    if err != nil {
        log.Printf("Failed to upload to MinIO: %v", err)
        return
    }
    
    log.Printf("Successfully uploaded file to MinIO: %s, Location: %s", s3Path, result.Location)
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
		if os.IsNotExist(err) && storage.useS3 {
			// Try fetching from S3 before returning 404
			relPath, _ := filepath.Rel(DBDir, filePath)
			if err := fetchFromS3(relPath, filePath); err != nil {
				log.Printf("S3 fetch failed: %v", err)
				c.JSON(http.StatusNotFound, gin.H{"error": "Not found in local storage or S3"})
				return
			}
			fileInfo, err = os.Stat(filePath)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to stat file after S3 fetch"})
				return
			}
		} else if os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Not found"})
			return
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal server error"})
			return
		}
	}

	if fileInfo.IsDir() {
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
	
	// Asynchronously upload to S3 if configured
	if storage.useS3 {
		go uploadToS3(filePath, relativePath)
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "path": relativePath})
}
