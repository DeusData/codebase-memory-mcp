resource "helm_release" "this" {
  name  = "app"
  chart = "${path.module}/charts/app"
}

resource "helm_release" "ingress" {
  name       = "ingress"
  repository = "https://kubernetes.github.io/ingress-nginx"
  chart      = "ingress-nginx"
}
