package loadbalancer

import (
	"strconv"
	"strings"

	cerrors "github.com/stackshy/cloudemu/v2/errors"
	"github.com/stackshy/cloudemu/v2/server/wire/gcprest"
	lbdriver "github.com/stackshy/cloudemu/v2/services/loadbalancer/driver"
)

// Private Service Connect (PSC) consumer forwarding rules.
//
// A PSC consumer rule targets either a producer's service attachment (a
// regional rule whose target is a .../serviceAttachments/{name} self-link) or
// a Google APIs bundle (a global rule whose target is "all-apis" or "vpc-sc").
// Such a rule carries no loadBalancingScheme, so the EXTERNAL default applied
// to ordinary rules must not be synthesized for it, and GCP reports the
// connection it established through pscConnectionStatus / pscConnectionId.
const (
	pscTargetAllAPIs = "all-apis"
	pscTargetVPCSC   = "vpc-sc"

	pscServiceAttachmentsSegment = "/serviceAttachments/"

	// pscStatusAccepted is the connection status of a PSC rule the emulator
	// created: there is no producer-side acceptance list to reject it.
	pscStatusAccepted = "ACCEPTED"
)

// isGoogleAPIsBundle reports whether target names a PSC Google APIs bundle.
func isGoogleAPIsBundle(target string) bool {
	return target == pscTargetAllAPIs || target == pscTargetVPCSC
}

// isPSCTarget reports whether a forwarding rule with this target is a PSC
// consumer rule.
func isPSCTarget(target string) bool {
	return isGoogleAPIsBundle(target) || strings.Contains(target, pscServiceAttachmentsSegment)
}

// validatePSCTarget rejects a Google APIs bundle target on a regional rule:
// all-apis / vpc-sc are only valid on global forwarding rules.
//
//nolint:gocritic // rp is a request-scoped value
func validatePSCTarget(rp gcprest.ResourcePath, target string) error {
	if isGoogleAPIsBundle(target) && rp.Scope != gcprest.ScopeGlobal {
		return cerrors.Newf(cerrors.InvalidArgument,
			"Invalid value for field 'resource.target': '%s'. A Google APIs bundle target is only valid on a global forwarding rule.",
			target)
	}

	return nil
}

// applyPSCFields sets pscConnectionStatus and a stable pscConnectionId on a
// PSC consumer rule's response; other rules are left untouched.
func applyPSCFields(out *forwardingRuleResponse, lb *lbdriver.LBInfo) {
	if !isPSCTarget(lb.Tags[frTargetTag]) {
		return
	}

	id := fnvHash("psc:" + lb.ID)
	if id == 0 {
		id = 1
	}

	out.PscConnectionStatus = pscStatusAccepted
	out.PscConnectionID = strconv.FormatUint(id, 10)
}
