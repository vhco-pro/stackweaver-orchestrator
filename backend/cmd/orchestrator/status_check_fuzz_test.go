// Copyright (c) 2025 VH & Co BV. Licensed under the Business Source License 1.1. See LICENSE for details.

package main

import (
	"testing"
)

// FuzzExtractPRNumber asserts extractPRNumber never panics on arbitrary
// committer strings (which come from upstream VCS webhook payloads — untrusted).
func FuzzExtractPRNumber(f *testing.F) {
	f.Add("")
	f.Add("user:42")
	f.Add("user")
	f.Add("user:abc")
	f.Add("user:-1")
	f.Add("user:99999999999999999999")
	f.Add("a:b:c:d:42")
	f.Fuzz(func(t *testing.T, committer string) {
		n := extractPRNumber(committer)
		// Contract: returns 0 when not extractable; positive int otherwise.
		// Never returns negative.
		if n < 0 {
			t.Fatalf("extractPRNumber(%q) returned negative: %d", committer, n)
		}
	})
}
