package httplink

// levenshteinDistance computes the edit distance between two strings.
func levenshteinDistance(a, b string) int {
	if a == "" {
		return len(b)
	}
	if b == "" {
		return len(a)
	}

	// Use two rows instead of full matrix for space efficiency
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)

	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min(curr[j-1]+1, min(prev[j]+1, prev[j-1]+cost))
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

// normalizedLevenshtein returns 1.0 - (distance / maxLen), so 1.0 = identical.
func normalizedLevenshtein(a, b string) float64 {
	if a == b {
		return 1.0
	}
	maxLen := len(a)
	if len(b) > maxLen {
		maxLen = len(b)
	}
	if maxLen == 0 {
		return 1.0
	}
	dist := levenshteinDistance(a, b)
	return 1.0 - float64(dist)/float64(maxLen)
}

// ngramOverlap computes the character n-gram overlap coefficient between two strings.
// Returns |intersection(ngrams(a), ngrams(b))| / min(|ngrams(a)|, |ngrams(b)|).
func ngramOverlap(a, b string, n int) float64 {
	if len(a) < n || len(b) < n {
		return 0
	}

	ngramsA := buildNgrams(a, n)
	ngramsB := buildNgrams(b, n)

	intersection := 0
	for ng := range ngramsA {
		if ngramsB[ng] {
			intersection++
		}
	}

	minSize := len(ngramsA)
	if len(ngramsB) < minSize {
		minSize = len(ngramsB)
	}
	if minSize == 0 {
		return 0
	}
	return float64(intersection) / float64(minSize)
}

func buildNgrams(s string, n int) map[string]bool {
	ngrams := make(map[string]bool)
	for i := 0; i <= len(s)-n; i++ {
		ngrams[s[i:i+n]] = true
	}
	return ngrams
}

// confidenceBand returns the confidence band label for a given score.
func confidenceBand(score float64) string {
	switch {
	case score >= 0.7:
		return "high"
	case score >= 0.45:
		return "medium"
	case score >= 0.25:
		return "speculative"
	default:
		return ""
	}
}
