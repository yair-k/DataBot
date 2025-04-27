package utils

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

// GenerateConfigKey creates a unique string key for a database configuration
func GenerateConfigKey(config map[string]interface{}) string {
	var username string
	if config["username"] != nil {
		if username, ok := config["username"].(string); ok {
			username = username
		} else if usernameString, ok := config["username"].(*string); ok {
			username = *usernameString
		}
	}

	port := ""
	if config["port"] != nil {
		if portStr, ok := config["port"].(string); ok {
			port = portStr
		} else if portString, ok := config["port"].(*string); ok {
			port = *portString
		}
	}
	// Create a unique key based on connection details
	key := fmt.Sprintf("%s:%s:%s:%s:%s",
		config["type"],
		config["host"],
		port,
		username,
		config["database"])

	return key
}

// FetchCertificateFromURL downloads a certificate from a URL and stores it temporarily
func FetchCertificateFromURL(url string) (string, error) {
	// Create a temporary file
	tmpFile, err := ioutil.TempFile("", "cert-*.pem")
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer tmpFile.Close()

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Fetch the certificate
	resp, err := client.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to fetch certificate from URL: %v", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to fetch certificate, status: %s", resp.Status)
	}

	// Copy the certificate to the temporary file
	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to save certificate: %v", err)
	}

	// Return the path to the temporary file
	return tmpFile.Name(), nil
}

// PrepareCertificatesFromURLs fetches certificates from URLs and returns their local paths
func PrepareCertificatesFromURLs(sslCertURL, sslKeyURL, sslRootCertURL string) (certPath, keyPath, rootCertPath string, tempFiles []string, err error) {
	// Fetch client certificate if URL provided
	if sslCertURL != "" {
		certPath, err = FetchCertificateFromURL(sslCertURL)
		if err != nil {
			// Clean up any files already created
			for _, file := range tempFiles {
				os.Remove(file)
			}
			return "", "", "", nil, fmt.Errorf("failed to fetch client certificate: %v", err)
		}
		tempFiles = append(tempFiles, certPath)
	}

	// Fetch client key if URL provided
	if sslKeyURL != "" {
		keyPath, err = FetchCertificateFromURL(sslKeyURL)
		if err != nil {
			// Clean up any files already created
			for _, file := range tempFiles {
				os.Remove(file)
			}
			return "", "", "", nil, fmt.Errorf("failed to fetch client key: %v", err)
		}
		tempFiles = append(tempFiles, keyPath)
	}

	// Fetch CA certificate if URL provided
	if sslRootCertURL != "" {
		rootCertPath, err = FetchCertificateFromURL(sslRootCertURL)
		if err != nil {
			// Clean up any files already created
			for _, file := range tempFiles {
				os.Remove(file)
			}
			return "", "", "", nil, fmt.Errorf("failed to fetch CA certificate: %v", err)
		}
		tempFiles = append(tempFiles, rootCertPath)
	}

	return certPath, keyPath, rootCertPath, tempFiles, nil
}
