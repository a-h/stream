package main

import (
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"time"
)

func main() {
	start := time.Now()
	fmt.Println("Finding Go Lambda function code directories...")
	dirs, err := getDirectoriesContainingMainGoFiles("./api")
	if err != nil {
		fmt.Printf("Failed to get directories containing main.go files: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("%d Lambda entrypoints found...\n", len(dirs))
	for i := 0; i < len(dirs); i++ {
		fmt.Printf("Building Lambda %d of %d...\n", i+1, len(dirs))
		buildMainGoFile(dirs[i])
	}
	fmt.Printf("Built %d Lambda functions in %v\n", len(dirs), time.Now().Sub(start))
}

func buildMainGoFile(path string) error {
	// GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o lambdaHandler .
	cmd := exec.Command("go", "build", "-ldflags=-s -w", "-o", "lambdaHandler")
	cmd.Dir = path
	cmd.Env = append(os.Environ(),
		"GOOS=linux",
		"GOARCH=amd64")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error running command: %w", err)
	}
	if exitCode := cmd.ProcessState.ExitCode(); exitCode != 0 {
		return fmt.Errorf("non-zero exit code: %v", exitCode)
	}
	return nil
}

func getDirectoriesContainingMainGoFiles(srcPath string) (paths []string, err error) {
	filepath.Walk(srcPath, func(currentPath string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			// Continue.
			return nil
		}
		d, f := path.Split(currentPath)
		if f == "main.go" {
			paths = append(paths, d)
		}
		return nil
	})
	if err != nil {
		err = fmt.Errorf("failed to walk directory: %w", err)
		return
	}
	return
}
