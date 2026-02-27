package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	yokeflightv1alpha1 "github.com/yokecd/yoke-operator/api/v1alpha1"
	"github.com/yokecd/yoke-operator/internal/apply"
	"github.com/yokecd/yoke-operator/internal/flight"
	"github.com/yokecd/yoke-operator/internal/wasm"
)

const (
	finalizerName = "yokeflight.yoke.cd/finalizer"
	fieldManager  = "yoke-atc-controller"
)

// ATCReconciler reconciles a YokeFlight object.
type ATCReconciler struct {
	client.Client
	Scheme  *runtime.Scheme
	Fetcher flight.Fetcher
	Runner  wasm.Runner
	Applier apply.Applier
}

// +kubebuilder:rbac:groups=yoke.cd,resources=yokeflights,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=yoke.cd,resources=yokeflights/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=yoke.cd,resources=yokeflights/finalizers,verbs=update
// +kubebuilder:rbac:groups="*",resources="*",verbs="*"

// Reconcile handles the reconciliation loop for YokeFlight resources.
func (r *ATCReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the YokeFlight instance.
	var yokeFlight yokeflightv1alpha1.YokeFlight
	if err := r.Get(ctx, req.NamespacedName, &yokeFlight); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("YokeFlight resource not found; ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "unable to fetch YokeFlight")
		return ctrl.Result{}, err
	}

	// Handle deletion.
	if !yokeFlight.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&yokeFlight, finalizerName) {
			logger.Info("Running finalizer for YokeFlight", "name", yokeFlight.Name)
			if err := r.cleanupResources(ctx, &yokeFlight); err != nil {
				logger.Error(err, "failed to cleanup resources")
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(&yokeFlight, finalizerName)
			if err := r.Update(ctx, &yokeFlight); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present.
	if !controllerutil.ContainsFinalizer(&yokeFlight, finalizerName) {
		controllerutil.AddFinalizer(&yokeFlight, finalizerName)
		if err := r.Update(ctx, &yokeFlight); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Update status to Processing.
	r.setCondition(&yokeFlight, "Processing", metav1.ConditionFalse, "Reconciling", "Processing YokeFlight resource")
	if err := r.Status().Update(ctx, &yokeFlight); err != nil {
		logger.Error(err, "unable to update YokeFlight status")
		// Don't return error here, continue processing.
	}

	// Step 1: Fetch the WASM module.
	logger.Info("Fetching WASM module", "source", yokeFlight.Spec.WasmSource)
	wasmBytes, err := r.Fetcher.Fetch(ctx, yokeFlight.Spec.WasmSource)
	if err != nil {
		logger.Error(err, "failed to fetch WASM module")
		r.setCondition(&yokeFlight, "Ready", metav1.ConditionFalse, "FetchFailed", fmt.Sprintf("Failed to fetch WASM module: %v", err))
		if statusErr := r.Status().Update(ctx, &yokeFlight); statusErr != nil {
			logger.Error(statusErr, "unable to update YokeFlight status")
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Step 2: Prepare input for the WASM module.
	input, err := r.buildInput(&yokeFlight)
	if err != nil {
		logger.Error(err, "failed to build input for WASM module")
		r.setCondition(&yokeFlight, "Ready", metav1.ConditionFalse, "InputError", fmt.Sprintf("Failed to build input: %v", err))
		if statusErr := r.Status().Update(ctx, &yokeFlight); statusErr != nil {
			logger.Error(statusErr, "unable to update YokeFlight status")
		}
		return ctrl.Result{}, err
	}

	// Step 3: Run the WASM module.
	logger.Info("Running WASM module")
	output, err := r.Runner.Run(ctx, wasmBytes, input)
	if err != nil {
		logger.Error(err, "failed to run WASM module")
		r.setCondition(&yokeFlight, "Ready", metav1.ConditionFalse, "WasmError", fmt.Sprintf("WASM execution failed: %v", err))
		if statusErr := r.Status().Update(ctx, &yokeFlight); statusErr != nil {
			logger.Error(statusErr, "unable to update YokeFlight status")
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Step 4: Parse the output into Kubernetes resources.
	resources, err := parseResources(output)
	if err != nil {
		logger.Error(err, "failed to parse WASM output")
		r.setCondition(&yokeFlight, "Ready", metav1.ConditionFalse, "ParseError", fmt.Sprintf("Failed to parse WASM output: %v", err))
		if statusErr := r.Status().Update(ctx, &yokeFlight); statusErr != nil {
			logger.Error(statusErr, "unable to update YokeFlight status")
		}
		return ctrl.Result{}, err
	}

	// Step 5: Apply resources via server-side apply.
	logger.Info("Applying resources", "count", len(resources))
	var appliedResources []yokeflightv1alpha1.ResourceRef
	for _, res := range resources {
		// Set owner reference so resources get cleaned up with the YokeFlight.
		if err := r.setOwnerReference(&yokeFlight, res); err != nil {
			logger.Error(err, "failed to set owner reference", "resource", res.GetName())
		}

		if err := r.Applier.Apply(ctx, res, fieldManager); err != nil {
			logger.Error(err, "failed to apply resource",
				"kind", res.GetKind(),
				"name", res.GetName(),
				"namespace", res.GetNamespace(),
			)
			r.setCondition(&yokeFlight, "Ready", metav1.ConditionFalse, "ApplyFailed",
				fmt.Sprintf("Failed to apply %s/%s: %v", res.GetKind(), res.GetName(), err))
			if statusErr := r.Status().Update(ctx, &yokeFlight); statusErr != nil {
				logger.Error(statusErr, "unable to update YokeFlight status")
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

		appliedResources = append(appliedResources, yokeflightv1alpha1.ResourceRef{
			APIVersion: res.GetAPIVersion(),
			Kind:       res.GetKind(),
			Name:       res.GetName(),
			Namespace:  res.GetNamespace(),
		})

		logger.Info("Applied resource",
			"kind", res.GetKind(),
			"name", res.GetName(),
			"namespace", res.GetNamespace(),
		)
	}

	// Step 6: Update status.
	yokeFlight.Status.AppliedResources = appliedResources
	yokeFlight.Status.ObservedGeneration = yokeFlight.Generation
	r.setCondition(&yokeFlight, "Ready", metav1.ConditionTrue, "Reconciled", "All resources applied successfully")
	if err := r.Status().Update(ctx, &yokeFlight); err != nil {
		logger.Error(err, "unable to update YokeFlight status")
		return ctrl.Result{}, err
	}

	logger.Info("Successfully reconciled YokeFlight", "name", yokeFlight.Name)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ATCReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&yokeflightv1alpha1.YokeFlight{}).
		Complete(r)
}

// buildInput constructs the JSON input to pass to the WASM module.
func (r *ATCReconciler) buildInput(yokeFlight *yokeflightv1alpha1.YokeFlight) ([]byte, error) {
	input := map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      yokeFlight.Name,
			"namespace": yokeFlight.Namespace,
		},
		"spec": yokeFlight.Spec,
	}

	// If the spec has raw input/values, include them directly.
	if yokeFlight.Spec.Input != nil {
		var rawInput interface{}
		if err := json.Unmarshal(yokeFlight.Spec.Input.Raw, &rawInput); err == nil {
			input["input"] = rawInput
		}
	}

	return json.Marshal(input)
}

// parseResources parses WASM output bytes into a list of unstructured Kubernetes resources.
func parseResources(output []byte) ([]*unstructured.Unstructured, error) {
	if len(output) == 0 {
		return nil, fmt.Errorf("WASM module produced empty output")
	}

	var resources []*unstructured.Unstructured

	// Try to parse as a JSON array first.
	var rawList []json.RawMessage
	if err := json.Unmarshal(output, &rawList); err == nil {
		for i, raw := range rawList {
			obj := &unstructured.Unstructured{}
			if err := obj.UnmarshalJSON(raw); err != nil {
				return nil, fmt.Errorf("failed to unmarshal resource at index %d: %w", i, err)
			}
			resources = append(resources, obj)
		}
		return resources, nil
	}

	// Try to parse as a single object.
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(output); err != nil {
		return nil, fmt.Errorf("failed to unmarshal WASM output as Kubernetes resource: %w", err)
	}

	// Check if it's a List kind.
	if obj.IsList() {
		items, err := obj.ToList()
		if err != nil {
			return nil, fmt.Errorf("failed to convert list: %w", err)
		}
		for i := range items.Items {
			resources = append(resources, &items.Items[i])
		}
		return resources, nil
	}

	return []*unstructured.Unstructured{obj}, nil
}

// setOwnerReference sets the owner reference on the resource so it gets garbage collected
// when the YokeFlight is deleted. Only works for namespace-scoped resources in the same namespace.
func (r *ATCReconciler) setOwnerReference(owner *yokeflightv1alpha1.YokeFlight, obj *unstructured.Unstructured) error {
	// Only set owner reference if the resource is in the same namespace.
	if obj.GetNamespace() == "" || obj.GetNamespace() != owner.Namespace {
		return nil
	}

	ownerRef := metav1.OwnerReference{
		APIVersion: owner.APIVersion,
		Kind:       owner.Kind,
		Name:       owner.Name,
		UID:        owner.UID,
	}

	// Check if owner reference already exists.
	existingRefs := obj.GetOwnerReferences()
	for _, ref := range existingRefs {
		if ref.UID == owner.UID {
			return nil
		}
	}

	existingRefs = append(existingRefs, ownerRef)
	obj.SetOwnerReferences(existingRefs)
	return nil
}

// cleanupResources handles cleanup when a YokeFlight is being deleted.
func (r *ATCReconciler) cleanupResources(ctx context.Context, yokeFlight *yokeflightv1alpha1.YokeFlight) error {
	logger := log.FromContext(ctx)

	if yokeFlight.Status.AppliedResources == nil {
		return nil
	}

	for _, ref := range yokeFlight.Status.AppliedResources {
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion(ref.APIVersion)
		obj.SetKind(ref.Kind)
		obj.SetName(ref.Name)
		obj.SetNamespace(ref.Namespace)

		key := types.NamespacedName{Name: ref.Name, Namespace: ref.Namespace}
		if err := r.Get(ctx, key, obj); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			logger.Error(err, "failed to get resource for cleanup",
				"kind", ref.Kind,
				"name", ref.Name,
			)
			continue
		}

		// Check if we own this resource before deleting.
		owned := false
		for _, ownerRef := range obj.GetOwnerReferences() {
			if ownerRef.UID == yokeFlight.UID {
				owned = true
				break
			}
		}

		if !owned {
			// For cluster-scoped resources or resources without owner refs,
			// delete them explicitly.
			if err := r.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
				logger.Error(err, "failed to delete resource during cleanup",
					"kind", ref.Kind,
					"name", ref.Name,
				)
			} else {
				logger.Info("Deleted resource during cleanup",
					"kind", ref.Kind,
					"name", ref.Name,
				)
			}
		}
		// Owned resources will be garbage collected automatically.
	}

	return nil
}

// setCondition sets a condition on the YokeFlight status.
func (r *ATCReconciler) setCondition(yokeFlight *yokeflightv1alpha1.YokeFlight, condType string, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
		ObservedGeneration: yokeFlight.Generation,
	}

	if yokeFlight.Status.Conditions == nil {
		yokeFlight.Status.Conditions = []metav1.Condition{}
	}

	meta.SetStatusCondition(&yokeFlight.Status.Conditions, condition)
}

// Ensure ATCReconciler implements reconcile.Reconciler.
var _ = (&ATCReconciler{}).Reconcile