package op

import (
	ctrl "sigs.k8s.io/controller-runtime"
)

type WidgetReconciler struct{}

func (r *WidgetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&Widget{}).
		Owns(&Gadget{}).
		Complete(r)
}
