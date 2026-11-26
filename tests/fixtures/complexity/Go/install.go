package op

import "helm.sh/helm/v3/pkg/chart/loader"

func deployOperator() error {
	_, err := loader.Load("charts/operator")
	return err
}
