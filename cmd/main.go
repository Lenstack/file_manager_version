package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	databaseFile  = "file_manager.db"
	storageDir    = "storage"
	compressedDir = "compressed"
)

// Initialize the database
func initDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", databaseFile)
	if err != nil {
		return nil, err
	}

	query := `
	CREATE TABLE IF NOT EXISTS actions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		action_type TEXT,
		filename TEXT,
		storage_id TEXT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE TABLE IF NOT EXISTS versions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		filename TEXT,
		version INTEGER,
		hash TEXT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	_, err = db.Exec(query)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Log actions into the database
func logAction(db *sql.DB, actionType, filename, storageID string) error {
	query := `INSERT INTO actions (action_type, filename, storage_id) VALUES (?, ?, ?);`
	_, err := db.Exec(query, actionType, filename, storageID)
	return err
}

// Log file versioning into the database
func logVersion(db *sql.DB, filename, hash string) error {
	var lastVersion int
	query := `
	SELECT version FROM versions
	WHERE filename = ?
	ORDER BY version DESC
	LIMIT 1;`
	err := db.QueryRow(query, filename).Scan(&lastVersion)

	if errors.Is(err, sql.ErrNoRows) {
		lastVersion = 0
	} else if err != nil {
		return err
	}

	query = `INSERT INTO versions (filename, version, hash) VALUES (?, ?, ?);`
	_, err = db.Exec(query, filename, lastVersion+1, hash)
	return err
}

// Store a file and manage its versioning
func storeFile(filePath string, db *sql.DB) (string, error) {
	if _, err := os.Stat(storageDir); os.IsNotExist(err) {
		if err := os.Mkdir(storageDir, os.ModePerm); err != nil {
			return "", fmt.Errorf("failed to create storage directory: %w", err)
		}
	}

	srcFile, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open source file: %w", err)
	}
	defer srcFile.Close()

	hash, err := hashFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to hash file: %w", err)
	}

	ext := filepath.Ext(filePath)
	filename := strings.TrimSuffix(filepath.Base(filePath), ext)

	hashedFilename := hash + ext
	storagePath := filepath.Join(storageDir, hashedFilename)

	if _, err := os.Stat(storagePath); err == nil {
		fmt.Printf("File %s already exists as %s. Skipping storage.\n", filePath, storagePath)
		if err := logAction(db, "store_duplicate", filename+ext, hashedFilename); err != nil {
			return "", err
		}
		return hashedFilename, nil
	}

	destFile, err := os.Create(storagePath)
	if err != nil {
		return "", fmt.Errorf("failed to create destination file: %w", err)
	}
	defer destFile.Close()

	if _, err := io.Copy(destFile, srcFile); err != nil {
		return "", fmt.Errorf("failed to copy file: %w", err)
	}

	if err := logAction(db, "store", filename+ext, hashedFilename); err != nil {
		return "", fmt.Errorf("failed to log action: %w", err)
	}

	if err := logVersion(db, filename+ext, hash); err != nil {
		return "", fmt.Errorf("failed to log version: %w", err)
	}

	fmt.Printf("File stored as %s\n", storagePath)
	return hashedFilename, nil
}

// Deduplicate files in a directory
func deduplicateFiles(directory string, db *sql.DB) error {
	hashes := make(map[string]string)
	hashesMutex := &sync.Mutex{}

	errCh := make(chan error, 1)
	done := make(chan bool)

	go func() {
		err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				fileHash, err := hashFile(path)
				if err != nil {
					return err
				}

				hashesMutex.Lock()
				if originalPath, exists := hashes[fileHash]; exists {
					fmt.Printf("Duplicate found: %s (original: %s). Deleting...\n", path, originalPath)
					if err := os.Remove(path); err != nil {
						hashesMutex.Unlock()
						return err
					}
					if err := logAction(db, "deduplicate", path, ""); err != nil {
						hashesMutex.Unlock()
						return err
					}
				} else {
					hashes[fileHash] = path
				}
				hashesMutex.Unlock()
			}
			return nil
		})
		if err != nil {
			errCh <- err
		}
		done <- true
	}()

	select {
	case err := <-errCh:
		return err
	case <-done:
		return nil
	}
}

// Hash a file using SHA-256
func hashFile(filepath string) (string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", fmt.Errorf("failed to hash file: %w", err)
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

// Compress a file using gzip
func compressFile(inputFile, outputDir string) error {
	// Ensure the output directory exists
	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Open the input file
	inFile, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer inFile.Close()

	// Construct the output file path
	outputFile := filepath.Join(outputDir, filepath.Base(inputFile)+".gz")

	// Create the output file
	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Create a new gzip writer with metadata
	gzipWriter := gzip.NewWriter(outFile)
	defer gzipWriter.Close()
	gzipWriter.Name = filepath.Base(inputFile) // Store the original file name in the header

	// Copy data from the input file to the gzip writer
	_, err = io.Copy(gzipWriter, inFile)
	if err != nil {
		return fmt.Errorf("failed to write compressed data: %w", err)
	}

	return nil
}

// Decompress a file using gzip
func decompressFile(inputFile, outputDir string) error {
	// Ensure the output directory exists
	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Open the compressed input file
	inFile, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer inFile.Close()

	// Create a new gzip reader
	gzipReader, err := gzip.NewReader(inFile)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	// Use the original file name from the gzip header
	outputFile := filepath.Join(outputDir, gzipReader.Name)
	if outputFile == "" {
		return fmt.Errorf("gzip header does not contain the original file name")
	}

	// Create the output file
	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Copy data from the gzip reader to the output file
	_, err = io.Copy(outFile, gzipReader)
	if err != nil {
		return fmt.Errorf("failed to write decompressed data: %w", err)
	}

	return nil
}

// Backup all files in a directory with compression
func backup(directory, output string) error {
	outFile, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	gzipWriter := gzip.NewWriter(outFile)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	err = filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error accessing file %s: %w", path, err)
		}
		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}
		defer file.Close()

		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return fmt.Errorf("failed to create tar header for file %s: %w", path, err)
		}

		relativePath, err := filepath.Rel(directory, path)
		if err != nil {
			return fmt.Errorf("failed to calculate relative path for file %s: %w", path, err)
		}
		header.Name = relativePath

		err = tarWriter.WriteHeader(header)
		if err != nil {
			return fmt.Errorf("failed to write tar header for file %s: %w", path, err)
		}

		_, err = io.Copy(tarWriter, file)
		if err != nil {
			return fmt.Errorf("failed to write file %s to tar archive: %w", path, err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to create backup: %w", err)
	}

	return nil
}

// Restore files from a compressed archive
func restore(archive, targetDir string) error {
	// Open the archive file
	inFile, err := os.Open(archive)
	if err != nil {
		return fmt.Errorf("failed to open archive file: %w", err)
	}
	defer inFile.Close()

	// Create a gzip reader
	gzipReader, err := gzip.NewReader(inFile)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	// Create a tar reader
	tarReader := tar.NewReader(gzipReader)

	// Extract files
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		// Construct the target path
		targetPath := filepath.Join(targetDir, header.Name)

		// Check the type of the header
		switch header.Typeflag {
		case tar.TypeDir: // Directory
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", targetPath, err)
			}
		case tar.TypeReg: // Regular file
			// Ensure the parent directory exists
			if err := os.MkdirAll(filepath.Dir(targetPath), os.ModePerm); err != nil {
				return fmt.Errorf("failed to create directory for file %s: %w", targetPath, err)
			}

			// Create the file
			outFile, err := os.Create(targetPath)
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", targetPath, err)
			}
			defer outFile.Close()

			// Copy file content
			if _, err := io.Copy(outFile, tarReader); err != nil {
				return fmt.Errorf("failed to extract file %s: %w", targetPath, err)
			}
		default:
			return fmt.Errorf("unsupported header type: %c in %s", header.Typeflag, header.Name)
		}
	}

	return nil
}

func main() {
	action := flag.String("action", "", "Action to perform: store, deduplicate, compress, backup, restore")
	input := flag.String("input", "", "Input file/directory")
	output := flag.String("output", "", "Output file/directory")
	flag.Parse()

	db, err := initDB()
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	switch *action {
	case "store":
		if *input == "" {
			log.Fatal("Please provide -input for storing a file")
		}
		if _, err := storeFile(*input, db); err != nil {
			log.Fatalf("Error storing file: %v", err)
		}
	case "deduplicate":
		if *input == "" {
			log.Fatal("Please provide a directory for deduplication using -input")
		}
		if err := deduplicateFiles(*input, db); err != nil {
			log.Fatalf("Error during deduplication: %v", err)
		}
	case "compress":
		if *input == "" {
			log.Fatal("Please provide -input for compression")
		}
		if err := compressFile(*input, compressedDir); err != nil {
			log.Fatalf("Error compressing file: %v", err)
		}
	case "decompress":
		if *input == "" || *output == "" {
			log.Fatal("Please provide -input and -output for decompression")
		}
		if err := decompressFile(*input, *output); err != nil {
			log.Fatalf("Error decompressing file: %v", err)
		}
	case "backup":
		if *input == "" || *output == "" {
			log.Fatal("Please provide -input directory and -output file for backup")
		}
		if err := backup(*input, *output); err != nil {
			log.Fatalf("Error creating backup: %v", err)
		}
	case "restore":
		if *input == "" || *output == "" {
			log.Fatal("Please provide -input backup file and -output directory for restoration")
		}
		if err := restore(*input, *output); err != nil {
			log.Fatalf("Error restoring backup: %v", err)
		}
	default:
		fmt.Println("Invalid action. Use -action with one of: store, deduplicate, compress, backup, restore")
	}
}
