// recording-export is an offline administrator tool. Output contains decrypted
// conversation data; it is intentionally not exposed through an HTTP endpoint.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/Wei-Shaw/sub2api/internal/pkg/upstreamrecord"
)

func main() {
	keyFile := flag.String("key-file", "", "path to the private 32-byte recording key")
	flag.Parse()
	if *keyFile == "" || flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: recording-export -key-file PATH ARCHIVE [ARCHIVE ...]\nOutput is sensitive decrypted JSONL. Process archives in filename order. Check export_integrity on capture_end and the command exit status; a source recording_complete flag alone is insufficient.")
		os.Exit(2)
	}
	key, err := os.ReadFile(*keyFile)
	if err != nil || len(key) != 32 {
		fmt.Fprintln(os.Stderr, "cannot read a valid recording key")
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	integrity := upstreamrecord.NewIntegrityChecker()
	for _, path := range flag.Args() {
		file, err := os.Open(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, "cannot open recording archive")
			os.Exit(1)
		}
		err = upstreamrecord.ReadArchive(file, key, func(event upstreamrecord.Event) error {
			state, err := integrity.Observe(event)
			if err != nil {
				return err
			}
			return encoder.Encode(upstreamrecord.ExportEvent{Event: event, ExportIntegrity: state})
		})
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			fmt.Fprintln(os.Stderr, "recording export failed; any partial output must be treated as incomplete")
			os.Exit(1)
		}
	}
	total, incomplete := integrity.Counts()
	fmt.Fprintf(os.Stderr, "captures=%d incomplete=%d\n", total, incomplete)
	if incomplete != 0 {
		os.Exit(1)
	}
}
