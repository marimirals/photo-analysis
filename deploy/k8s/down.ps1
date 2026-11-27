# Stops the system.
#   .\deploy\k8s\down.ps1          - remove the app from the cluster, keep the cluster
#   .\deploy\k8s\down.ps1 -Cluster - delete the whole kind cluster (all data is lost)
param([switch]$Cluster)

if ($Cluster) {
    kind delete cluster --name photo
} else {
    kubectl delete -k deploy --ignore-not-found
}
