package main

import (
	"context"
	_ "embed"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/google/go-licenses/licenses"
	"github.com/google/licenseclassifier"
)

const (
	outputFile = "LICENSE-3rdparty.csv"
)

var (
	ignoredPath = []string{"github.com/DataDog/temporal-large-payload-codec"}
)

func main() {
	if err := execute(); err != nil {
		log.Fatal(err)
	}
}

type Component struct {
	Origin    string `json:"origin"`
	License   string `json:"license"`
	Copyright string `json:"copyright"`
}

//go:embed overrides.json
var componentOverrides []byte

func execute() error {
	classifier, err := licenses.NewClassifier(0.9)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(outputFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx := context.Background()

	libs, err := licenses.Libraries(ctx, classifier, false, ignoredPath, "./...")
	if err != nil {
		return err
	}

	components := map[string]Component{}
	var overrides map[string]Component
	if err := json.Unmarshal(componentOverrides, &overrides); err != nil {
		return err
	}

	for _, lib := range libs {
		var component Component
		var licenseUrl string
		if lib.LicensePath != "" {
			// Parse copyright
			if b, err := os.ReadFile(lib.LicensePath); err == nil {
				component.Copyright = licenseclassifier.CopyrightHolder(string(b))
			}
			// Parse license type
			if ln, _, err := classifier.Identify(lib.LicensePath); err == nil {
				component.License = ln
			}
			// Parse license URL
			if lURL, err := lib.FileURL(ctx, lib.LicensePath); err == nil {
				licenseUrl = lURL
			}
		}
		if licenseUrl != "" {
			component.Origin = strings.Split(licenseUrl, "/blob/")[0]
		}

		components[lib.Name()] = component
	}

	for k, v := range overrides {
		components[k] = v
	}

	return writeComponents(f, components)
}

func writeComponents(w io.Writer, components map[string]Component) error {
	c := csv.NewWriter(w)
	defer c.Flush()

	if err := c.Write([]string{"Component", "Origin", "License", "Copyright"}); err != nil {
		return err
	}

	// Sort component names
	var keys []string
	for k, _ := range components {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		component := components[k]
		if err := c.Write([]string{k, component.Origin, component.License, component.Copyright}); err != nil {
			return err
		}
	}

	return nil
}
