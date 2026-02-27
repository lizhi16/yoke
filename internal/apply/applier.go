package apply

import (
	"context"
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Applier defines the interface for applying Kubernetes resources using server-side apply.
type Applier interface {
	// Apply performs a server-side apply of the given unstructured resource.
	Apply(ctx context.Context, obj *unstructured.Unstructured, fieldManager string, opts ...ApplyOption) error
}

// ApplyOption configures the apply behavior.
type ApplyOption func(*applyOptions)

type applyOptions struct {
	force bool
}

// WithForce sets the force flag on the server-side apply, which forces the field manager
// to acquire ownership of conflicting fields.
func WithForce(force bool) ApplyOption {
	return func(o *applyOptions) {
		o.force = force
	}
}

// ServerSideApplier implements Applier using controller-runtime's client.Client
// with server-side apply (Patch with ApplyPatchType).
type ServerSideApplier struct {
	Client client.Client
}

// NewApplier creates a new ServerSideApplier with the given client.
func NewApplier(c client.Client) Applier {
	return &ServerSideApplier{
		Client: c,
	}
}

// Apply performs a server-side apply of the given unstructured resource.
func (a *ServerSideApplier) Apply(ctx context.Context, obj *unstructured.Unstructured, fieldManager string, opts ...ApplyOption) error {
	options := &applyOptions{
		force: true, // default to force to avoid conflicts
	}
	for _, opt := range opts {
		opt(options)
	}

	if obj == nil {
		return fmt.Errorf("cannot apply nil object")
	}

	// Ensure the object has a valid GVK set
	gvk := obj.GroupVersionKind()
	if gvk.Kind == "" || gvk.Version == "" {
		return fmt.Errorf("object must have GroupVersionKind set, got: %v", gvk)
	}

	// Marshal the object to JSON for the patch data
	data, err := json.Marshal(obj.Object)
	if err != nil {
		return fmt.Errorf("failed to marshal object for server-side apply: %w", err)
	}

	// Build the patch object
	patch := client.RawPatch(types.ApplyPatchType, data)

	// Build patch options
	patchOpts := []client.PatchOption{
		client.FieldOwner(fieldManager),
	}
	if options.force {
		patchOpts = append(patchOpts, client.ForceOwnership)
	}

	// Perform the server-side apply via Patch
	if err := a.Client.Patch(ctx, obj, patch, patchOpts...); err != nil {
		return fmt.Errorf("server-side apply failed for %s %s/%s: %w",
			gvk.Kind,
			obj.GetNamespace(),
			obj.GetName(),
			err,
		)
	}

	return nil
}

// ApplyAll applies a list of unstructured resources using server-side apply.
// It returns the first error encountered, if any.
func ApplyAll(ctx context.Context, applier Applier, objects []*unstructured.Unstructured, fieldManager string, opts ...ApplyOption) error {
	for i, obj := range objects {
		if err := applier.Apply(ctx, obj, fieldManager, opts...); err != nil {
			return fmt.Errorf("failed to apply resource %d (%s %s/%s): %w",
				i,
				obj.GetKind(),
				obj.GetNamespace(),
				obj.GetName(),
				err,
			)
		}
	}
	return nil
}