# scheduler-controller

`scheduler-controller` is a Kubernetes operator that applies scheduling policy to selected workloads through a cluster-scoped `SchedulingPolicy` custom resource.

The controller is intended for environments where workload scheduling settings should be managed centrally instead of being repeated in every workload manifest. A policy selects workloads by API version, kind, namespace, and labels, then enforces scheduling-related fields such as `schedulerName`, `priorityClassName`, and labels.


## What It Does

`SchedulingPolicy` provides a declarative way to describe three things:

- Which workloads should be managed.
- Which policy should win when multiple policies match the same workload.
- Which scheduling values should be applied to those workloads.

For native Kubernetes workloads, the controller updates the workload itself or its pod template. For KServe `InferenceService`, it updates supported component pod specs and labels directly in the custom resource spec.

The custom resource API group is `ai-paas.org`.

```yaml
apiVersion: ai-paas.org/v1alpha1
kind: SchedulingPolicy
```


## Supported Workloads

| API version | Kind | Applied to |
| --- | --- | --- |
| `v1` | `ReplicationController` | Pod template spec and template labels |
| `apps/v1` | `Deployment` | Pod template spec and template labels |
| `apps/v1` | `StatefulSet` | Pod template spec and template labels |
| `apps/v1` | `DaemonSet` | Pod template spec and template labels |
| `batch/v1` | `CronJob` | Job template pod spec and template labels |
| `serving.kserve.io/v1beta1` | `InferenceService` | `predictor`, `transformer`, and `explainer` component pod specs and labels |

For `InferenceService` in KServe serverless mode, Knative Serving must allow `kubernetes.podspec-schedulername` and `kubernetes.podspec-priorityclassname` in the `knative-serving/config-features` ConfigMap.


## Example

```yaml
apiVersion: ai-paas.org/v1alpha1
kind: SchedulingPolicy
metadata:
  name: ml-workflow-batch
spec:
  priority: 100
  sources:
    - apiVersion: apps/v1
      kind: Deployment
      namespaces:
        - kubeflow
      selector:
        matchLabels:
          workflow-id: example-workflow
    - apiVersion: batch/v1
      kind: CronJob
      namespaces:
        - kubeflow
      selector:
        matchLabels:
          workflow-id: example-workflow
  target:
    schedulerName: custom-scheduler
    priorityClassName: high-priority
    labels:
      queue: ml-batch
      team: ml-platform
```

When more than one policy matches the same workload, the controller chooses the policy with the highest `spec.priority`. If priorities are equal, the earliest `metadata.creationTimestamp` wins. If both values are equal, the policy name is used as a deterministic final tie-breaker. Managed workloads are annotated with `ai-paas.org/scheduling-policy` to show which policy last applied scheduling settings.

The sample manifest in this repository is available at `config/samples/v1alpha1_schedulingpolicy.yaml`.


## Installation

The commands below use only a locally built image from a cloned repository.

Prerequisites:

- Go 1.25 or later
- Docker or a compatible container runtime
- kubectl
- A Kubernetes cluster reachable from the current kubeconfig
- A local development cluster that can load locally built images, such as kind, minikube, or Docker Desktop Kubernetes

Clone the repository:

```sh
git clone https://github.com/ai-paas/scheduler-controller.git
cd scheduler-controller
```

Build the controller image locally:

```sh
make docker-build IMG=scheduler-controller:dev
```

Load the image into your local cluster.

For kind:

```sh
kind load docker-image scheduler-controller:dev
```

For minikube:

```sh
minikube image load scheduler-controller:dev
```

For Docker Desktop Kubernetes, the locally built Docker image is typically available to the cluster without an extra load step.

Install the CRD and deploy the controller:

```sh
make install
make deploy IMG=scheduler-controller:dev
```

Verify the deployment:

```sh
kubectl get crd schedulingpolicies.ai-paas.org
kubectl get pods -n scheduler-controller-system
```

Apply the sample policy:

```sh
kubectl apply -f config/samples/v1alpha1_schedulingpolicy.yaml
kubectl get schedulingpolicies.ai-paas.org
kubectl describe schedulingpolicy schedulingpolicy-sample
```


## Development

Common development commands:

```sh
make manifests
make generate
make fmt
make vet
make test
make build
```

Run the controller locally against the current kubeconfig:

```sh
make install
make run
```

Generate a single install manifest for local inspection:

```sh
make build-installer IMG=scheduler-controller:dev
```

The generated manifest is written to `dist/install.yaml`.


## License

See [LICENSE](LICENSE).
