package identity

import "fmt"

// HandleReuseError is returned when a stored reference resolves to an
// object whose identity extensible attribute holds a UID different from
// the caller's. Per ADR-IN-0006 this is refused, not adopted: the object
// the reference points at belongs to a different managed resource (or
// was never one of ours), and mutating or deleting it would silently
// destroy someone else's record. Match with errors.As, e.g.:
//
//	var reuse *identity.HandleReuseError
//	if errors.As(err, &reuse) { ... }
type HandleReuseError struct {
	// ObjectType is the WAPI object type (e.g. "record:a").
	ObjectType string
	// Ref is the reference that resolved to a foreign object.
	Ref string
	// WantUID is the caller's own managed resource UID.
	WantUID string
	// GotUID is the UID actually stamped on the resolved object.
	GotUID string
}

func (e *HandleReuseError) Error() string {
	return fmt.Sprintf(
		"refusing to manage %s %s: its %q extensible attribute holds identity %s, not this resource's %s — "+
			"this reference now points at a different object, most likely because it was reused after the original "+
			"was deleted and a new object was created at the same computed address; the operator's options are to "+
			"delete this managed resource (it no longer owns anything) or, if the Grid object is actually the right "+
			"one, clear the stamped attribute manually and let this resource adopt it",
		e.ObjectType, e.Ref, EAKey, e.GotUID, e.WantUID,
	)
}

// AmbiguousMatchError is returned when an identity-EA search matches more
// than one Grid object for the same UID. Per ADR-IN-0006 this is refused,
// never resolved by taking the first result — silently picking one is
// exactly the defect this package exists to close. Match with errors.As,
// e.g.:
//
//	var ambiguous *identity.AmbiguousMatchError
//	if errors.As(err, &ambiguous) { ... }
type AmbiguousMatchError struct {
	// ObjectType is the WAPI object type (e.g. "record:a").
	ObjectType string
	// UID is the managed resource UID that was searched for.
	UID string
	// Count is the number of objects the search matched (always > 1).
	Count int
}

func (e *AmbiguousMatchError) Error() string {
	return fmt.Sprintf(
		"refusing to manage %s: %d Grid objects carry the %q extensible attribute set to %s — identity must be "+
			"unique to verify ownership, and taking an arbitrary match risks acting on an object this resource does "+
			"not own; an operator should search the Grid for %s=%s and remove the stamped attribute from every "+
			"object except the one this managed resource should own",
		e.ObjectType, e.Count, EAKey, e.UID, EAKey, e.UID,
	)
}
