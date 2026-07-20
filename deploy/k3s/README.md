# SCENE horse k3s deployment

This Kustomize layout targets namespace `scene` and
`https://scene.135.181.140.68.sslip.io`. It uses the cluster's Traefik ingress
controller and cert-manager `letsencrypt-prod` ClusterIssuer.

The app references `scene-app-aws` and `scene-app-auth`; the dispatcher references
only `scene-app-aws`. The Traefik middleware references
`scene-ingress-basic-auth`. The manifests never create or render those Secrets.
The middleware protects every external path and removes its BasicAuth
`Authorization` header before proxying. External API/MCP clients therefore send
the SCENE application token in `X-SCENE-API-Token`; direct clients can continue
to use Bearer auth.
The `scene-runner` ServiceAccount inherits only `scene-registry` for kubelet image
pulls. It has no RBAC, does not mount an API token, and runner Jobs receive no AWS
environment or application Secret references. Only `scene-dispatcher` is bound
to the exact namespaced Job, immutable execution-Secret, pod status, and pod-log
permissions used by `KubernetesRunnerClient`.

## Prerequisite gate

Do not deploy until all of these conditions hold:

- SCENE-19, SCENE-20, SCENE-7, and the core of SCENE-15 are merged and green;
- `python -m app.services.dispatcher` is the delivered dispatcher entry point;
- `/healthz`, `/readyz`, and `/version` have their SCENE-15 semantics;
- the CloudFormation stack is healthy and its non-secret outputs are recorded;
- app and runner images are built for `linux/amd64`, pushed, and resolved to
  immutable registry digests;
- the horse runner node pool remains `amd64`; both static workloads and generated
  runner Jobs select that architecture;
- the temporary basic-auth ingress is explicitly accepted as operator-only, not
  customer-ready authentication. SCENE-21 remains the customer-access gate.

## Local validation and image pinning

The committed `registry.invalid` image values and every `REPLACE_WITH_*` value are
deliberately non-runnable. Replace the bucket placeholder with the stack's
`ArtifactBucketName` output; set `SCENE_VERSION`, the full 40-character
`SCENE_GIT_SHA`, and the UTC `SCENE_BUILD_TIME`; and replace both images with
references in the form
`registry/repository@sha256:<64 lowercase hex characters>`. Kustomize propagates
the image values into the app, dispatcher, and readiness Job.

Both Dockerfiles pin their base manifest lists. The app image installs the
hash-locked Linux dependency set in `requirements.staging.lock`; regenerate it
with the command recorded in that file whenever `pyproject.toml` changes. The
runner installs the exact Playwright Python version matching its Microsoft base
image from `requirements.runner.lock`; regenerate that file from
`requirements.runner.in` using its recorded command.

Inspect each remote manifest and confirm it contains `linux/amd64` before using
its digest. Then run:

```sh
scripts/scene_aws_validate.rb
scripts/scene_k3s_validate.rb --strict
kubectl kustomize deploy/k3s >/dev/null
```

Without `--strict`, the k3s validator accepts the source template but warns for
each non-runnable deployment placeholder. It still checks Kustomize rendering,
resource identity, probes, resources, security contexts, RBAC, Secret isolation,
storage configuration, ingress/TLS, network policy, and digest syntax.

The namespace enforces, audits, and warns at Pod Security `restricted`. Static
workloads and generated runner Jobs run as fixed non-root users, use the runtime
default seccomp profile, drop capabilities, and mount writable ephemeral paths
for temporary files and artifacts. `scene-runtime` also supplies default
ephemeral storage requests/limits to generated runner containers through a
`LimitRange`. The default-deny policy has one cert-manager exception: only
kube-system Traefik pods can reach pods labeled as HTTP-01 solvers, and only on
the solver's TCP port 8089.

Horse evaluates the dispatcher's in-cluster `kubernetes.default:443` connection
after service DNAT. The dispatcher policy therefore allows the exact horse API
endpoint `135.181.140.68/32` on translated TCP port `6443`, in addition to
public HTTPS for AWS. Update that `/32` together with this host-specific layout
if the horse control-plane address changes.

Only Traefik and SCENE runner pods can reach the app under these policies. The
app therefore sets `FORWARDED_ALLOW_IPS=*` so Uvicorn preserves Traefik's HTTPS
scheme in generated viewer and artifact links; do not broaden app ingress
without narrowing that trust setting at the same time.

## Cluster preflight

All cluster commands use the operator-local kubeconfig, which must never enter
the repository:

```sh
export KUBECONFIG=/Users/michael/.kube/config-horse

kubectl get nodes -o wide
kubectl top nodes
kubectl -n kube-system get deployment,pod -l app.kubernetes.io/name=traefik
kubectl get deployment,pod -A -l app.kubernetes.io/instance=cert-manager
kubectl get namespace scene
kubectl diff -k deploy/k3s
```

Stop on an unexpected namespace owner, unavailable Traefik/cert-manager,
non-amd64 scheduling, insufficient capacity, broad RBAC, a rendered Secret, or
an image placeholder.

## Secret creation

Create the namespace before any external Secret. Do not apply the complete
Kustomize bundle yet, because the Deployments reference those Secrets:

```sh
kubectl apply -f deploy/k3s/namespace.yaml
```

Create the AWS Secret using the direct-pipe procedure in `deploy/aws/README.md`.
Generate the app API token without printing it:

```sh
set +x
ruby -rsecurerandom -e 'puts "SCENE_API_TOKEN=#{SecureRandom.hex(32)}"' |
  kubectl -n scene create secret generic scene-app-auth \
    --from-env-file=/dev/stdin --dry-run=client -o yaml |
  kubectl apply -f -
```

Create the temporary operator-only Traefik basic-auth Secret through a pipe;
`htpasswd` prompts for the password without placing it in shell history:

```sh
set +x
htpasswd -nB scene-reviewer |
  kubectl -n scene create secret generic scene-ingress-basic-auth \
    --from-file=users=/dev/stdin --dry-run=client -o yaml |
  kubectl apply -f -
```

Create `scene-registry` as a `kubernetes.io/dockerconfigjson` Secret before either
Deployment starts. Dynamic runner Jobs inherit it through `scene-runner`, because
the current Job builder does not emit `imagePullSecrets`. Never copy any Secret
object into evidence.

## Install and upgrade

Inspect the rendered resources and server-side diff before applying. A completed
readiness Job is immutable; delete only that named, ticket-owned Job before an
upgrade that changes its pod template. Its TTL normally removes it after one
hour.

```sh
kubectl -n scene delete job scene-runner-readiness --ignore-not-found
kubectl apply -k deploy/k3s
kubectl -n scene rollout status deployment/scene-app --timeout=5m
kubectl -n scene rollout status deployment/scene-dispatcher --timeout=5m
kubectl -n scene wait --for=condition=complete job/scene-runner-readiness --timeout=10m
kubectl -n scene logs job/scene-runner-readiness
```

The dispatcher Deployment intentionally has one replica while SCENE-7's durable
claim loop serializes work. The app has two replicas and a PodDisruptionBudget.
The readiness Job uses the same runner ServiceAccount/image-pull path as generated
Jobs, waits up to three minutes for aggregate `/readyz`, reaches
`/api/orchestrator/ping` through cluster DNS, verifies a writable ephemeral
workspace and baked `/opt/scene/runner.py`, and launches Chromium and Firefox
headlessly. It receives neither AWS credentials nor an execution Secret.

## Verification

Record command results without Secret values or presigned URLs:

```sh
kubectl -n scene get deployment,pod,job,service,ingress
kubectl -n scene get certificate
kubectl auth can-i --as=system:serviceaccount:scene:scene-dispatcher create jobs -n scene
kubectl auth can-i --as=system:serviceaccount:scene:scene-dispatcher get jobs -n scene
kubectl auth can-i --as=system:serviceaccount:scene:scene-dispatcher delete jobs -n scene
kubectl auth can-i --as=system:serviceaccount:scene:scene-dispatcher create secrets -n scene
kubectl auth can-i --as=system:serviceaccount:scene:scene-dispatcher get secrets -n scene
kubectl auth can-i --as=system:serviceaccount:scene:scene-dispatcher delete secrets -n scene
kubectl auth can-i --as=system:serviceaccount:scene:scene-dispatcher list pods -n scene
kubectl auth can-i --as=system:serviceaccount:scene:scene-dispatcher get pods/log -n scene
kubectl auth can-i --as=system:serviceaccount:scene:scene-dispatcher create selfsubjectaccessreviews.authorization.k8s.io
kubectl auth can-i --as=system:serviceaccount:scene:scene-runner create jobs -n scene
kubectl auth can-i --as=system:serviceaccount:scene:scene-runner get secrets -n scene
kubectl -n scene exec deployment/scene-app -- python -c \
  'import urllib.request; print(urllib.request.urlopen("http://scene.scene.svc.cluster.local/healthz").status)'
curl -fsS https://scene.135.181.140.68.sslip.io/healthz
curl -fsS https://scene.135.181.140.68.sslip.io/readyz
curl -fsS https://scene.135.181.140.68.sslip.io/version
```

The external requests must return `401` without basic auth. Use `curl -u
scene-reviewer` to enter the password interactively for authorized checks.
For a protected application mutation, use the same interactive BasicAuth prompt
and add `X-SCENE-API-Token: $SCENE_API_TOKEN`; do not attempt to send Bearer and
Basic credentials in the single `Authorization` header. Configure MCP with the
paired `SCENE_INGRESS_BASIC_AUTH_USERNAME` and
`SCENE_INGRESS_BASIC_AUTH_PASSWORD` environment variables documented in
`docs/agent-api.md`. Inject them from a protected source with shell tracing
disabled so neither credential appears in command arguments or logs.
The final two runner authorization checks must print `no`; the dispatcher checks
must print `yes`. Dispatcher readiness consumes its fresh durable lease and the
complete Kubernetes capability result published by the reconciliation loop. App
readiness is the aggregate DynamoDB, S3, runner configuration, and dispatcher
gate. Confirm generated
runner Job specs have no `AWS_*` variables, API token, Docker socket, or
`host.docker.internal`, and that callbacks use
`http://scene.scene.svc.cluster.local`.

## Configuration handoff

Create a mode-0600 config-only export outside the checkout before import. Stream
it to one app pod's bounded `/tmp` volume, run the dry run, record only aggregate
counts, then apply and delete the temporary copy:

```sh
APP_POD="$(kubectl -n scene get pod -l app.kubernetes.io/component=app -o jsonpath='{.items[0].metadata.name}')"

kubectl -n scene exec -i "$APP_POD" -- sh -ec \
  'umask 077; cat > /tmp/scene-config.json' < "$SCENE_CONFIG_EXPORT"
kubectl -n scene exec "$APP_POD" -- \
  python scripts/scene_config.py import --input /tmp/scene-config.json
kubectl -n scene exec "$APP_POD" -- \
  python scripts/scene_config.py import --input /tmp/scene-config.json --apply
kubectl -n scene exec "$APP_POD" -- rm -f /tmp/scene-config.json
```

Verify project/page/task/batch IDs, credentials, actions, browsers, viewports,
thresholds, and global settings. Confirm no runs, executions, baselines, webhook
delivery history, or artifact references were imported. Replay the dry run to
prove idempotency before acceptance runs.

## Backup and export

Before configuration changes, export the active configuration to a private file
outside the repository. Confirm DynamoDB PITR and S3 versioning are enabled with
the commands in `deploy/aws/README.md`. Keep the export mode `0600`, record its
checksum and secure location, and never attach it to the ticket because it may
contain page credentials.

## Rollback

Retain DynamoDB and S3. Reapply `deploy/k3s` from the previously accepted commit,
whose ConfigMap must contain the prior app and runner digests, then wait for both
rollouts. Do not use a mutable tag. If only one Deployment changed, Kubernetes
`rollout undo` is acceptable after confirming that the referenced ConfigMap still
contains the matching prior runtime values.

Stop and investigate if rollback readiness fails; do not bypass `/readyz` or
expose the service anonymously.

## Teardown

Teardown requires explicit approval. Export configuration first, then remove
only SCENE-owned Kubernetes resources with `kubectl delete -k deploy/k3s`.
Delete the externally managed SCENE Secrets separately after workloads are gone,
including `scene-app-aws`, `scene-app-auth`, `scene-ingress-basic-auth`, and
`scene-registry`. The dispatcher should already have removed every execution
Secret. The AWS stack is independent: its table, bucket, and transport policy
remain retained, and storage deletion is never implied by Kubernetes teardown.
