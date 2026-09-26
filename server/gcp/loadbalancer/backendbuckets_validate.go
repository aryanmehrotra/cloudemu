package loadbalancer

import (
	"math"
	"regexp"
	"strconv"
	"strings"

	cerrors "github.com/stackshy/cloudemu/v2/errors"
)

// Cloud CDN limits, from the BackendBucketCdnPolicy field docs in
// cloud.google.com/go/compute/apiv1/computepb (compute v1.60.0).
const (
	// maxCDNTTLSeconds is the largest defaultTtl / maxTtl / clientTtl (1 year).
	maxCDNTTLSeconds = 31622400
	// maxServeWhileStaleSeconds is the largest serveWhileStale (1 week).
	maxServeWhileStaleSeconds = 604800
	// maxBypassCacheHeaders is how many bypassCacheOnRequestHeaders are allowed.
	maxBypassCacheHeaders = 5

	cacheModeCacheAllStatic = "CACHE_ALL_STATIC"

	fieldCacheMode  = "cacheMode"
	fieldDefaultTTL = "defaultTtl"
	fieldMaxTTL     = "maxTtl"

	fieldService        = "service"
	fieldDefaultService = "defaultService"
)

// rfc1035Name is the compute resource-name grammar: 1-63 characters, a
// lowercase letter first, then lowercase letters, digits or dashes, not ending
// in a dash.
var rfc1035Name = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)

// validCacheModes are the cdnPolicy.cacheMode values the API accepts.
//
//nolint:gochecknoglobals // immutable lookup table, not mutable state
var validCacheModes = map[string]bool{
	cacheModeCacheAllStatic: true,
	"USE_ORIGIN_HEADERS":    true,
	"FORCE_CACHE_ALL":       true,
}

// validCompressionModes are the compressionMode values the API accepts.
//
//nolint:gochecknoglobals // immutable lookup table, not mutable state
var validCompressionModes = map[string]bool{
	"AUTOMATIC": true,
	"DISABLED":  true,
}

// urlMapServiceFields are the url-map members that name a backend service or
// backend bucket.
//
//nolint:gochecknoglobals // immutable lookup table, not mutable state
var urlMapServiceFields = map[string]bool{fieldService: true, fieldDefaultService: true}

// isBackendBucketRef reports whether ref is a backendBuckets self-link or
// relative path (full URL or "projects/{p}/global/backendBuckets/{name}").
func isBackendBucketRef(ref string) bool {
	return strings.Contains(ref, "/"+resourceBackendBuckets+"/")
}

// validateRFC1035Name rejects a missing or malformed resource name.
func validateRFC1035Name(name string) error {
	if !rfc1035Name.MatchString(name) {
		return cerrors.Newf(cerrors.InvalidArgument,
			"Invalid value for field 'resource.name': '%s'. Must be a match of regex '(?:[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?)'", name)
	}

	return nil
}

// applyBackendBucketDefaults fills cdnPolicy.cacheMode with the documented
// default (CACHE_ALL_STATIC) when Cloud CDN is enabled or a cdnPolicy is given
// without one.
func applyBackendBucketDefaults(body map[string]any) {
	enabled, _ := body["enableCdn"].(bool)
	policy, hasPolicy := body["cdnPolicy"].(map[string]any)

	if !enabled && !hasPolicy {
		return
	}

	if !hasPolicy {
		policy = map[string]any{}
		body["cdnPolicy"] = policy
	}

	if _, ok := policy[fieldCacheMode]; !ok {
		policy[fieldCacheMode] = cacheModeCacheAllStatic
	}
}

// validateCompressionMode rejects an unrecognized compressionMode.
func validateCompressionMode(v any) error {
	if v == nil {
		return nil
	}

	mode, _ := v.(string)
	if !validCompressionModes[mode] {
		return cerrors.Newf(cerrors.InvalidArgument, "Invalid value for field 'resource.compressionMode': '%v'.", v)
	}

	return nil
}

// validateCDNPolicy checks the cdnPolicy members with documented constraints;
// every other member passes through untouched.
func validateCDNPolicy(v any) error {
	if v == nil {
		return nil
	}

	policy, ok := v.(map[string]any)
	if !ok {
		return cerrors.New(cerrors.InvalidArgument, "Invalid value for field 'resource.cdnPolicy': must be an object.")
	}

	if mode, present := policy[fieldCacheMode]; present {
		if s, _ := mode.(string); !validCacheModes[s] {
			return cerrors.Newf(cerrors.InvalidArgument, "Invalid value for field 'resource.cdnPolicy.cacheMode': '%v'.", mode)
		}
	}

	if err := validateCDNRanges(policy); err != nil {
		return err
	}

	return validateCDNLists(policy)
}

// validateCDNRanges checks the TTL bounds and that defaultTtl <= maxTtl.
func validateCDNRanges(policy map[string]any) error {
	limits := []struct {
		field string
		max   int64
	}{
		{fieldDefaultTTL, maxCDNTTLSeconds},
		{fieldMaxTTL, maxCDNTTLSeconds},
		{"clientTtl", maxCDNTTLSeconds},
		{"serveWhileStale", maxServeWhileStaleSeconds},
		{"signedUrlCacheMaxAgeSec", math.MaxInt64},
	}

	for _, l := range limits {
		raw, present := policy[l.field]
		if !present {
			continue
		}

		n, ok := jsonInt(raw)
		if !ok || n < 0 || n > l.max {
			return cerrors.Newf(cerrors.InvalidArgument,
				"Invalid value for field 'resource.cdnPolicy.%s': '%v'. Must be between 0 and %d.", l.field, raw, l.max)
		}
	}

	defTTL, hasDef := jsonInt(policy[fieldDefaultTTL])
	maxTTL, hasMax := jsonInt(policy[fieldMaxTTL])

	if hasDef && hasMax && defTTL > maxTTL {
		return cerrors.Newf(cerrors.InvalidArgument,
			"Invalid value for field 'resource.cdnPolicy.defaultTtl': '%d'. defaultTtl cannot be greater than maxTtl (%d).",
			defTTL, maxTTL)
	}

	return nil
}

// validateCDNLists checks the list-valued cdnPolicy members.
func validateCDNLists(policy map[string]any) error {
	if headers, _ := policy["bypassCacheOnRequestHeaders"].([]any); len(headers) > maxBypassCacheHeaders {
		return cerrors.Newf(cerrors.InvalidArgument,
			"Invalid value for field 'resource.cdnPolicy.bypassCacheOnRequestHeaders': at most %d headers are allowed.",
			maxBypassCacheHeaders)
	}

	negPolicy, _ := policy["negativeCachingPolicy"].([]any)
	negEnabled, _ := policy["negativeCaching"].(bool)

	if len(negPolicy) > 0 && !negEnabled {
		return cerrors.New(cerrors.InvalidArgument,
			"Invalid value for field 'resource.cdnPolicy.negativeCachingPolicy': negativeCaching must be enabled.")
	}

	return nil
}

// jsonInt reads an integral JSON value: a number, or a decimal string (proto
// JSON encodes int64 fields such as signedUrlCacheMaxAgeSec as strings).
func jsonInt(v any) (int64, bool) {
	switch t := v.(type) {
	case float64:
		if t != math.Trunc(t) || t < math.MinInt64 || t > math.MaxInt64 {
			return 0, false
		}

		return int64(t), true
	case string:
		n, err := strconv.ParseInt(t, 10, 64)

		return n, err == nil
	default:
		return 0, false
	}
}
