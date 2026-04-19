package enricher

import (
	"regexp"
	"strings"
)

// NestJSMetadata holds extracted metadata from a NestJS TypeScript source file.
type NestJSMetadata struct {
	ClassName      string
	ControllerPath string
	IsInjectable   bool
	Routes         []RouteInfo
	Dependencies   []DIDepInfo
	FilePath       string
}

// RouteInfo describes a single HTTP route decorator.
type RouteInfo struct {
	Method string // "Get", "Post", "Put", "Delete", "Patch"
	Path   string
	Guards []string
}

// DIDepInfo describes a constructor-injected dependency.
type DIDepInfo struct {
	ParamName string
	TypeName  string
}

// InternalRequestCall describes a call to InternalRequest.<method>({...}).
type InternalRequestCall struct {
	Method      string // "get", "post", "put", "delete"
	ServiceName string // e.g., "CONTACTS_API"
	Route       string // e.g., "upsert"
}

// EventPatternCall describes a detected event publisher or subscriber.
type EventPatternCall struct {
	Topic    string // e.g., "contact.created"
	Role     string // "producer" or "consumer"
	Symbol   string // function/class name
	FilePath string
}

var (
	reController     = regexp.MustCompile(`@Controller\(\s*['"]([^'"]*)['"]\s*\)`)
	reClassName      = regexp.MustCompile(`export\s+class\s+(\w+)`)
	reInjectable     = regexp.MustCompile(`@Injectable\(\)`)
	reRoute          = regexp.MustCompile(`@(Get|Post|Put|Delete|Patch)\(\s*['"]?([^'")]*?)['"]?\s*\)`)
	reUseGuards      = regexp.MustCompile(`@UseGuards\(([^)]+)\)`)
	reConstructor    = regexp.MustCompile(`constructor\s*\(([\s\S]*?)\)`)
	reDIParam        = regexp.MustCompile(`(?:private|protected|public)\s+(?:readonly\s+)?(\w+)\s*:\s*(\w+)`)
	reInternalReq    = regexp.MustCompile(`InternalRequest\.(get|post|put|delete|patch)\(\s*\{([\s\S]*?)\}\s*\)`)
	reServiceName    = regexp.MustCompile(`serviceName\s*:\s*SERVICE_NAME\.(\w+)`)
	reRouteField     = regexp.MustCompile(`route\s*:\s*['"]([^'"]+)['"]`)
	reEventPattern   = regexp.MustCompile(`@EventPattern\(\s*['"]([^'"]+)['"]`)
	reMessagePattern = regexp.MustCompile(`@MessagePattern\(\s*['"]([^'"]+)['"]`)
	rePubSubPublish  = regexp.MustCompile(`pubSub\.publish\(\s*['"]([^'"]+)['"]`)
	rePubSubEmit     = regexp.MustCompile(`\.emit\(\s*['"]([^'"]+)['"]`)
)

// ExtractNestJSMetadata extracts NestJS decorator metadata from TypeScript source.
func ExtractNestJSMetadata(source, filePath string) (NestJSMetadata, error) {
	meta := NestJSMetadata{FilePath: filePath}

	// Controller path
	if m := reController.FindStringSubmatch(source); m != nil {
		meta.ControllerPath = m[1]
	}

	// Class name
	if m := reClassName.FindStringSubmatch(source); m != nil {
		meta.ClassName = m[1]
	}

	// Injectable
	meta.IsInjectable = reInjectable.MatchString(source)

	// Routes with guards lookup
	lines := strings.Split(source, "\n")
	routeMatches := reRoute.FindAllStringSubmatchIndex(source, -1)
	for _, idx := range routeMatches {
		method := source[idx[2]:idx[3]]
		path := source[idx[4]:idx[5]]

		// Find the line number of this route decorator
		routePos := idx[0]
		routeLine := strings.Count(source[:routePos], "\n")

		// Look back up to 3 lines for @UseGuards
		var guards []string
		startLine := routeLine - 3
		if startLine < 0 {
			startLine = 0
		}
		for i := startLine; i < routeLine; i++ {
			if gm := reUseGuards.FindStringSubmatch(lines[i]); gm != nil {
				// Split guards by comma and trim
				for _, g := range strings.Split(gm[1], ",") {
					g = strings.TrimSpace(g)
					if g != "" {
						guards = append(guards, g)
					}
				}
			}
		}

		meta.Routes = append(meta.Routes, RouteInfo{
			Method: method,
			Path:   path,
			Guards: guards,
		})
	}

	// Constructor DI dependencies
	if m := reConstructor.FindStringSubmatch(source); m != nil {
		body := m[1]
		deps := reDIParam.FindAllStringSubmatch(body, -1)
		for _, d := range deps {
			meta.Dependencies = append(meta.Dependencies, DIDepInfo{
				ParamName: d[1],
				TypeName:  d[2],
			})
		}
	}

	return meta, nil
}

// ExtractInternalRequests extracts InternalRequest.<method>({...}) calls from source.
func ExtractInternalRequests(source string) ([]InternalRequestCall, error) {
	matches := reInternalReq.FindAllStringSubmatch(source, -1)
	var calls []InternalRequestCall
	for _, m := range matches {
		method := m[1]
		body := m[2]

		var serviceName, route string
		if sm := reServiceName.FindStringSubmatch(body); sm != nil {
			serviceName = sm[1]
		}
		if rm := reRouteField.FindStringSubmatch(body); rm != nil {
			route = rm[1]
		}

		calls = append(calls, InternalRequestCall{
			Method:      method,
			ServiceName: serviceName,
			Route:       route,
		})
	}
	return calls, nil
}

// ExtractEventPatterns finds @EventPattern, @MessagePattern, pubSub.publish,
// and .emit() calls in source code, returning producer/consumer event pattern calls.
func ExtractEventPatterns(source, filePath string) []EventPatternCall {
	var patterns []EventPatternCall

	// Find class name for symbol attribution
	className := ""
	if m := reClassName.FindStringSubmatch(source); m != nil {
		className = m[1]
	}

	// Consumers: @EventPattern('topic') and @MessagePattern('topic')
	for _, m := range reEventPattern.FindAllStringSubmatch(source, -1) {
		patterns = append(patterns, EventPatternCall{
			Topic:    m[1],
			Role:     "consumer",
			Symbol:   className,
			FilePath: filePath,
		})
	}
	for _, m := range reMessagePattern.FindAllStringSubmatch(source, -1) {
		patterns = append(patterns, EventPatternCall{
			Topic:    m[1],
			Role:     "consumer",
			Symbol:   className,
			FilePath: filePath,
		})
	}

	// Producers: pubSub.publish('topic', ...) and .emit('topic', ...)
	for _, m := range rePubSubPublish.FindAllStringSubmatch(source, -1) {
		patterns = append(patterns, EventPatternCall{
			Topic:    m[1],
			Role:     "producer",
			Symbol:   className,
			FilePath: filePath,
		})
	}
	for _, m := range rePubSubEmit.FindAllStringSubmatch(source, -1) {
		patterns = append(patterns, EventPatternCall{
			Topic:    m[1],
			Role:     "producer",
			Symbol:   className,
			FilePath: filePath,
		})
	}

	return patterns
}
