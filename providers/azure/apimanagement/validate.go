package apimanagement

import (
	"encoding/json"
	"regexp"
	"strings"

	cerrors "github.com/stackshy/cloudemu/v2/errors"
)

const (
	// maxServiceNameLen is the longest service name Azure accepts.
	maxServiceNameLen = 50

	// skuConsumption is the serverless tier, the only one whose capacity is 0.
	skuConsumption = "Consumption"
)

// serviceNamePattern is Azure's service-name rule: starts with a letter, then
// letters, digits and hyphens, and does not end with a hyphen.
var serviceNamePattern = regexp.MustCompile(`^[A-Za-z]([A-Za-z0-9-]*[A-Za-z0-9])?$`)

// validSKUs is the armapimanagement v3 SKUType enum, keyed lowercase so the
// lookup is case-insensitive like ARM, mapped to the canonical casing.
//
//nolint:gochecknoglobals // static lookup table
var validSKUs = map[string]string{
	"developer":   "Developer",
	"basic":       "Basic",
	"standard":    "Standard",
	"premium":     "Premium",
	"consumption": skuConsumption,
	"isolated":    "Isolated",
	"basicv2":     "BasicV2",
	"standardv2":  "StandardV2",
}

// canonicalSKU returns the canonical casing of a known SKU name, or the input
// unchanged when it is unknown (validation rejects unknown names first).
func canonicalSKU(name string) string {
	if c, ok := validSKUs[strings.ToLower(name)]; ok {
		return c
	}

	return name
}

// validateCreate rejects a create/replace request with missing or malformed
// required fields: the path identity, location, the SKU block and the two
// publisher properties.
func validateCreate(sub, rg, name, location string, in *ServiceInput) error {
	switch {
	case sub == "":
		return invalid("subscription is required")
	case rg == "":
		return invalid("resource group is required")
	case location == "":
		return invalid("location is required")
	}

	if err := validateName(name); err != nil {
		return err
	}

	if in.SkuName == nil || *in.SkuName == "" {
		return invalid("sku.name is required")
	}

	if in.SkuCapacity == nil {
		return invalid("sku.capacity is required")
	}

	if err := validateSKU(*in.SkuName, *in.SkuCapacity); err != nil {
		return err
	}

	return validatePublisher(in.Properties)
}

// validateName enforces Azure's service-name rule (1-50 characters, starts with
// a letter, letters/digits/hyphens, no trailing hyphen).
func validateName(name string) error {
	if name == "" || len(name) > maxServiceNameLen || !serviceNamePattern.MatchString(name) {
		return cerrors.Newf(cerrors.InvalidArgument,
			"invalid API Management service name %q: it must be 1-%d characters, start with a letter, "+
				"contain only letters, digits and hyphens, and not end with a hyphen", name, maxServiceNameLen)
	}

	return nil
}

// validateSKU checks the SKU name is a known tier and the capacity fits it: the
// Consumption tier must be 0 units, every other tier at least 1.
func validateSKU(name string, capacity int32) error {
	canon, ok := validSKUs[strings.ToLower(name)]
	if !ok {
		return cerrors.Newf(cerrors.InvalidArgument, "invalid sku.name %q", name)
	}

	if canon == skuConsumption {
		if capacity != 0 {
			return cerrors.Newf(cerrors.InvalidArgument,
				"sku.capacity must be 0 for the Consumption tier, got %d", capacity)
		}

		return nil
	}

	if capacity < 1 {
		return cerrors.Newf(cerrors.InvalidArgument,
			"sku.capacity must be at least 1 for the %s tier, got %d", canon, capacity)
	}

	return nil
}

// validatePublisher requires non-empty properties.publisherEmail and
// properties.publisherName.
func validatePublisher(props json.RawMessage) error {
	var p struct {
		PublisherEmail string `json:"publisherEmail"`
		PublisherName  string `json:"publisherName"`
	}

	if len(props) > 0 {
		if err := json.Unmarshal(props, &p); err != nil {
			return cerrors.Newf(cerrors.InvalidArgument, "malformed properties: %v", err)
		}
	}

	switch {
	case strings.TrimSpace(p.PublisherEmail) == "":
		return invalid("properties.publisherEmail is required")
	case strings.TrimSpace(p.PublisherName) == "":
		return invalid("properties.publisherName is required")
	default:
		return nil
	}
}

// invalid is an InvalidArgument error (ARM 400 InvalidParameter).
func invalid(msg string) error {
	return cerrors.New(cerrors.InvalidArgument, msg)
}
