package store

// RiskLevel classifies impact based on BFS hop depth.
type RiskLevel string

const (
	RiskCritical RiskLevel = "CRITICAL"
	RiskHigh     RiskLevel = "HIGH"
	RiskMedium   RiskLevel = "MEDIUM"
	RiskLow      RiskLevel = "LOW"
)

// HopToRisk maps a BFS hop depth to a risk level.
func HopToRisk(hop int) RiskLevel {
	switch hop {
	case 1:
		return RiskCritical
	case 2:
		return RiskHigh
	case 3:
		return RiskMedium
	default:
		return RiskLow
	}
}

// ImpactSummary aggregates risk counts from a BFS traversal.
type ImpactSummary struct {
	Critical        int  `json:"critical"`
	High            int  `json:"high"`
	Medium          int  `json:"medium"`
	Low             int  `json:"low"`
	Total           int  `json:"total"`
	HasCrossService bool `json:"has_cross_service"`
}

// BuildImpactSummary computes risk distribution from deduplicated node hops.
func BuildImpactSummary(hops []*NodeHop, edges []EdgeInfo) ImpactSummary {
	var s ImpactSummary
	for _, nh := range hops {
		switch HopToRisk(nh.Hop) {
		case RiskCritical:
			s.Critical++
		case RiskHigh:
			s.High++
		case RiskMedium:
			s.Medium++
		case RiskLow:
			s.Low++
		}
		s.Total++
	}
	for _, e := range edges {
		if e.Type == "HTTP_CALLS" || e.Type == "ASYNC_CALLS" {
			s.HasCrossService = true
			break
		}
	}
	return s
}

// DeduplicateHops removes duplicate nodes from BFS results, keeping the
// minimum hop (highest risk) for each node.
func DeduplicateHops(hops []*NodeHop) []*NodeHop {
	best := make(map[int64]*NodeHop)
	for _, nh := range hops {
		if existing, ok := best[nh.Node.ID]; !ok || nh.Hop < existing.Hop {
			best[nh.Node.ID] = nh
		}
	}
	result := make([]*NodeHop, 0, len(best))
	for _, nh := range best {
		result = append(result, nh)
	}
	return result
}
