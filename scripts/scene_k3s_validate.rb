#!/usr/bin/env ruby
# frozen_string_literal: true

require "open3"
require "optparse"
require "pathname"
require "time"
require "yaml"

options = {
  directory: Pathname.new(__dir__).join("..", "deploy", "k3s").cleanpath,
  strict: false
}

OptionParser.new do |parser|
  parser.banner = "Usage: scene_k3s_validate.rb [--directory PATH] [--strict]"
  parser.on("--directory PATH", String, "Kustomize directory to validate") do |path|
    options[:directory] = Pathname.new(path)
  end
  parser.on("--strict", "Reject every committed non-runnable deployment placeholder") do
    options[:strict] = true
  end
end.parse!

errors = []
warnings = []
check = lambda { |condition, message| errors << message unless condition }

unless system("command", "-v", "kubectl", out: File::NULL, err: File::NULL)
  warn "ERROR: kubectl with built-in Kustomize support is required"
  exit 1
end

rendered, render_error, render_status = Open3.capture3("kubectl", "kustomize", options[:directory].to_s)
unless render_status.success?
  warn render_error
  warn "ERROR: kubectl kustomize failed"
  exit 1
end

begin
  # Psych 3.1 on macOS has safe_load but not safe_load_stream. The input here is
  # local output produced by kubectl Kustomize from this checkout.
  documents = YAML.load_stream(rendered).compact
rescue Psych::SyntaxError => e
  warn "ERROR: rendered YAML is invalid: #{e.message}"
  exit 1
end

resource = lambda do |kind, name|
  match = documents.find do |document|
    document["kind"] == kind && document.dig("metadata", "name") == name
  end
  errors << "missing #{kind}/#{name}" unless match
  match || {}
end

identities = documents.map { |document| [document["kind"], document.dig("metadata", "namespace"), document.dig("metadata", "name")] }
check.call(identities.uniq.length == identities.length, "rendered resources must have unique kind/namespace/name identities")
check.call(documents.none? { |document| document["kind"] == "Secret" }, "committed manifests must not contain Secret resources")
check.call(documents.none? { |document| %w[ClusterRole ClusterRoleBinding].include?(document["kind"]) }, "SCENE must not create cluster-scoped RBAC")

namespace = resource.call("Namespace", "scene")
namespace_labels = namespace.dig("metadata", "labels") || {}
check.call(namespace_labels["pod-security.kubernetes.io/enforce"] == "restricted", "scene namespace must enforce restricted pod security")
check.call(namespace_labels["pod-security.kubernetes.io/audit"] == "restricted", "scene namespace must audit restricted pod security")
check.call(namespace_labels["pod-security.kubernetes.io/warn"] == "restricted", "scene namespace must warn on restricted pod security violations")
documents.reject { |document| document["kind"] == "Namespace" }.each do |document|
  check.call(document.dig("metadata", "namespace") == "scene", "#{document['kind']}/#{document.dig('metadata', 'name')} must be in namespace scene")
end

app_account = resource.call("ServiceAccount", "scene-app")
dispatcher_account = resource.call("ServiceAccount", "scene-dispatcher")
runner_account = resource.call("ServiceAccount", "scene-runner")
check.call(app_account["automountServiceAccountToken"] == false, "scene-app must not mount a Kubernetes API token")
check.call(dispatcher_account["automountServiceAccountToken"] == true, "scene-dispatcher must mount its Kubernetes API token")
check.call(runner_account["automountServiceAccountToken"] == false, "scene-runner must not mount a Kubernetes API token")
pull_secret_names = lambda do |subject|
  Array(subject["imagePullSecrets"]).map { |entry| entry["name"] }.compact.sort
end
check.call(pull_secret_names.call(app_account).empty?, "scene-app ServiceAccount must not carry runtime credentials")
check.call(pull_secret_names.call(dispatcher_account).empty?, "scene-dispatcher ServiceAccount must not carry runtime credentials")
check.call(pull_secret_names.call(runner_account) == ["scene-registry"], "scene-runner must inherit only the registry pull Secret for generated Jobs")

role = resource.call("Role", "scene-job-dispatcher")
role_rules = Array(role["rules"])
normalize_rule = lambda do |rule|
  {
    "apiGroups" => Array(rule["apiGroups"]).sort,
    "resources" => Array(rule["resources"]).sort,
    "verbs" => Array(rule["verbs"]).sort
  }
end
expected_role_rules = [
  { "apiGroups" => ["batch"], "resources" => ["jobs"], "verbs" => %w[create delete get] },
  { "apiGroups" => [""], "resources" => ["secrets"], "verbs" => %w[create delete get] },
  { "apiGroups" => [""], "resources" => ["pods"], "verbs" => %w[get list] },
  { "apiGroups" => [""], "resources" => ["pods/log"], "verbs" => ["get"] }
].map { |rule| normalize_rule.call(rule) }.sort_by(&:to_s)
actual_role_rules = role_rules.map { |rule| normalize_rule.call(rule) }.sort_by(&:to_s)
check.call(actual_role_rules == expected_role_rules, "dispatcher Role must match the Job, immutable Secret lifecycle, pod status, and pod log contract exactly")
check.call(role_rules.flat_map { |rule| Array(rule["resources"]) }.none? { |name| name == "configmaps" }, "dispatcher Role must not grant ConfigMap access")
check.call(role_rules.flat_map { |rule| Array(rule["verbs"]) }.none? { |verb| verb == "*" }, "dispatcher Role must not contain wildcard verbs")
binding = resource.call("RoleBinding", "scene-job-dispatcher")
subjects = Array(binding["subjects"])
check.call(subjects == [{ "kind" => "ServiceAccount", "name" => "scene-dispatcher", "namespace" => "scene" }], "Job Role must bind only scene-dispatcher")
check.call(binding["roleRef"] == { "apiGroup" => "rbac.authorization.k8s.io", "kind" => "Role", "name" => "scene-job-dispatcher" }, "dispatcher RoleBinding must reference only scene-job-dispatcher")

runtime = resource.call("ConfigMap", "scene-runtime")
runtime_data = runtime.fetch("data", {})
expected_runtime = {
  "AWS_REGION" => "eu-central-1",
  "SCENE_ARTIFACT_STORAGE" => "s3",
  "SCENE_DYNAMODB_TABLE" => "scene-staging",
  "SCENE_ENV" => "staging",
  "SCENE_K3S_NAMESPACE" => "scene",
  "SCENE_K3S_RUNNER_SERVICE_ACCOUNT" => "scene-runner",
  "SCENE_K3S_SERVICE_URL" => "http://scene.scene.svc.cluster.local",
  "SCENE_RUNNER_BACKEND" => "k3s",
  "SCENE_RUNNER_CALLBACK_BASE_URL" => "http://scene.scene.svc.cluster.local",
  "SCENE_RUNNER_IMAGE_AUTOBUILD" => "false",
  "SCENE_S3_PREFIX" => "scene",
  "SCENE_STATE_BACKEND" => "dynamodb"
}
expected_runtime.each do |name, value|
  check.call(runtime_data[name] == value, "scene-runtime #{name} must be #{value}")
end
check.call(runtime_data.values.none? { |value| value.to_s.include?("host.docker.internal") }, "k3s runtime must not reference host.docker.internal")
credential_key_pattern = /(ACCESS_KEY|SECRET_ACCESS|SESSION_TOKEN|API_TOKEN|PASSWORD)/
check.call(runtime_data.keys.none? { |name| name.match?(credential_key_pattern) }, "scene-runtime must not contain credential-shaped keys")

bucket_name = runtime_data["SCENE_S3_BUCKET"].to_s
bucket_placeholder = bucket_name == "REPLACE_WITH_CLOUDFORMATION_ARTIFACT_BUCKET_NAME"
if bucket_placeholder
  message = "scene-runtime still uses the non-runnable SCENE_S3_BUCKET placeholder"
  options[:strict] ? errors << message : warnings << message
else
  check.call(bucket_name.match?(/\A[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\z/), "SCENE_S3_BUCKET must be a valid concrete S3 bucket name")
end

identity_placeholders = {
  "SCENE_VERSION" => "REPLACE_WITH_RELEASE_VERSION",
  "SCENE_GIT_SHA" => "REPLACE_WITH_FULL_GIT_SHA",
  "SCENE_BUILD_TIME" => "REPLACE_WITH_UTC_BUILD_TIME"
}
identity_placeholders.each do |name, placeholder|
  if runtime_data[name].to_s == placeholder
    message = "scene-runtime still uses the non-runnable #{name} placeholder"
    options[:strict] ? errors << message : warnings << message
  end
end
unless runtime_data["SCENE_VERSION"].to_s == identity_placeholders["SCENE_VERSION"]
  check.call(runtime_data["SCENE_VERSION"].to_s.match?(/\A[A-Za-z0-9][A-Za-z0-9._+\-]{0,63}\z/), "SCENE_VERSION must be a bounded release identifier")
end
unless runtime_data["SCENE_GIT_SHA"].to_s == identity_placeholders["SCENE_GIT_SHA"]
  check.call(runtime_data["SCENE_GIT_SHA"].to_s.match?(/\A[0-9a-f]{40}\z/), "SCENE_GIT_SHA must be a full lowercase Git commit SHA")
end
unless runtime_data["SCENE_BUILD_TIME"].to_s == identity_placeholders["SCENE_BUILD_TIME"]
  begin
    build_time = Time.iso8601(runtime_data["SCENE_BUILD_TIME"].to_s)
    check.call(build_time.utc? && runtime_data["SCENE_BUILD_TIME"].to_s.end_with?("Z"), "SCENE_BUILD_TIME must be an ISO 8601 UTC timestamp ending in Z")
  rescue ArgumentError
    errors << "SCENE_BUILD_TIME must be a valid ISO 8601 UTC timestamp"
  end
end

app = resource.call("Deployment", "scene-app")
dispatcher = resource.call("Deployment", "scene-dispatcher")
runner_job = resource.call("Job", "scene-runner-readiness")

pod_spec_for = lambda do |workload|
  workload.dig("spec", "template", "spec") || {}
end

containers_for = lambda do |workload|
  pod_spec = pod_spec_for.call(workload)
  Array(pod_spec["initContainers"]) + Array(pod_spec["containers"])
end

[app, dispatcher, runner_job].each do |workload|
  kind = workload["kind"] || "Workload"
  name = workload.dig("metadata", "name") || "unknown"
  pod_spec = pod_spec_for.call(workload)
  check.call(pod_spec.dig("nodeSelector", "kubernetes.io/arch") == "amd64", "#{kind}/#{name} must select amd64 nodes")
  check.call(pod_spec.dig("securityContext", "runAsNonRoot") == true, "#{kind}/#{name} must run as non-root")
  check.call(pod_spec.dig("securityContext", "seccompProfile", "type") == "RuntimeDefault", "#{kind}/#{name} must use RuntimeDefault seccomp")
  containers_for.call(workload).each do |container|
    container_name = container["name"] || "unknown"
    image = container["image"].to_s
    check.call(image.match?(/@sha256:[0-9a-f]{64}\z/), "#{kind}/#{name} container #{container_name} must use a sha256 image digest")
    if image.include?("registry.invalid") || image.end_with?("sha256:#{'0' * 64}")
      message = "#{kind}/#{name} container #{container_name} still uses the non-runnable image placeholder"
      options[:strict] ? errors << message : warnings << message
    end
    security = container.fetch("securityContext", {})
    check.call(security["allowPrivilegeEscalation"] == false, "#{kind}/#{name} container #{container_name} must disable privilege escalation")
    check.call(security["readOnlyRootFilesystem"] == true, "#{kind}/#{name} container #{container_name} must use a read-only root filesystem")
    check.call(Array(security.dig("capabilities", "drop")).include?("ALL"), "#{kind}/#{name} container #{container_name} must drop all capabilities")
    %w[requests limits].each do |class_name|
      %w[cpu memory ephemeral-storage].each do |resource_name|
        check.call(container.dig("resources", class_name, resource_name), "#{kind}/#{name} container #{container_name} needs #{class_name}.#{resource_name}")
      end
    end
  end
  check.call(Array(pod_spec["volumes"]).none? { |volume| volume.key?("hostPath") }, "#{kind}/#{name} must not mount host paths or a Docker socket")
end

app_spec = pod_spec_for.call(app)
dispatcher_spec = pod_spec_for.call(dispatcher)
runner_spec = pod_spec_for.call(runner_job)
check.call(app_spec["serviceAccountName"] == "scene-app", "scene-app Deployment must use scene-app ServiceAccount")
check.call(dispatcher_spec["serviceAccountName"] == "scene-dispatcher", "scene-dispatcher Deployment must use scene-dispatcher ServiceAccount")
check.call(runner_spec["serviceAccountName"] == "scene-runner", "runner Job must use scene-runner ServiceAccount")
check.call(app_spec["automountServiceAccountToken"] == false, "scene-app Deployment must disable API token mounting")
check.call(dispatcher_spec["automountServiceAccountToken"] == true, "scene-dispatcher Deployment must mount its bounded API token")
check.call(runner_spec["automountServiceAccountToken"] == false, "runner Job must not mount a Kubernetes API token")
check.call(pull_secret_names.call(app_spec) == ["scene-registry"], "scene-app pod must use only scene-registry for image pulls")
check.call(pull_secret_names.call(dispatcher_spec) == ["scene-registry"], "scene-dispatcher pod must use only scene-registry for image pulls")
check.call(pull_secret_names.call(runner_spec).empty?, "runner readiness must prove image pull inheritance through scene-runner")

secret_refs_for = lambda do |workload|
  containers_for.call(workload).flat_map do |container|
    Array(container["envFrom"]).map { |source| source.dig("secretRef", "name") }.compact
  end.uniq.sort
end
expected_app_secrets = %w[scene-app-auth scene-app-aws]
check.call(secret_refs_for.call(app) == expected_app_secrets, "scene-app must reference only the auth and AWS Secrets")
check.call(secret_refs_for.call(dispatcher) == ["scene-app-aws"], "scene-dispatcher must reference only the AWS Secret")
check.call(secret_refs_for.call(runner_job).empty?, "runner Job must not receive Secrets through envFrom")
secret_env_refs_for = lambda do |workload|
  containers_for.call(workload).flat_map do |container|
    Array(container["env"]).map { |entry| entry.dig("valueFrom", "secretKeyRef", "name") }.compact
  end
end
secret_volume_refs_for = lambda do |workload|
  Array(pod_spec_for.call(workload)["volumes"]).map { |volume| volume.dig("secret", "secretName") }.compact
end
check.call(secret_env_refs_for.call(runner_job).empty?, "runner readiness Job must not receive Secret-backed environment variables")
check.call(secret_volume_refs_for.call(runner_job).empty?, "runner readiness Job must not mount runtime Secrets")
runner_env_names = containers_for.call(runner_job).flat_map { |container| Array(container["env"]).map { |entry| entry["name"] } }
check.call(runner_env_names.none? { |name| name.to_s.start_with?("AWS_") }, "runner Job must not receive AWS environment variables")

app_container = Array(app_spec["containers"]).find { |container| container["name"] == "scene" } || {}
dispatcher_container = Array(dispatcher_spec["containers"]).find { |container| container["name"] == "dispatcher" } || {}
runner_container = Array(runner_spec["containers"]).find { |container| container["name"] == "runner-readiness" } || {}
app_env = Array(app_container["env"]).to_h { |entry| [entry["name"], entry["value"]] }
check.call(app_env["FORWARDED_ALLOW_IPS"] == "*", "scene-app must trust forwarded headers inside its bounded ingress policy")
check.call(app_container.dig("livenessProbe", "httpGet", "path") == "/healthz", "scene-app liveness probe must use /healthz")
check.call(app_container.dig("readinessProbe", "httpGet", "path") == "/readyz", "scene-app readiness probe must use /readyz")
check.call(app_container.dig("readinessProbe", "timeoutSeconds").to_i >= 9, "scene-app readiness timeout must cover both bounded readiness phases")
check.call(app_container.dig("startupProbe", "httpGet", "path") == "/healthz", "scene-app startup probe must use /healthz")
check.call(dispatcher_container["command"] == ["python", "-m", "app.services.dispatcher"], "dispatcher command must use the SCENE dispatcher module")
dispatcher_liveness = Array(dispatcher_container.dig("livenessProbe", "exec", "command")).join(" ")
dispatcher_readiness_command = Array(dispatcher_container.dig("readinessProbe", "exec", "command"))
dispatcher_readiness = dispatcher_readiness_command.join(" ")
check.call(dispatcher_liveness.include?("kill -0 1"), "dispatcher liveness must check only the long-running process")
%w[dispatcher_status heartbeat_at capabilities_checked_at capabilities_ok].each do |contract|
  check.call(dispatcher_readiness.include?(contract), "dispatcher readiness must verify #{contract}")
end
if dispatcher_readiness_command[0, 2] == ["python", "-c"]
  _stdout, syntax_error, syntax_status = Open3.capture3(
    "python3",
    "-c",
    "import sys; compile(sys.argv[1], '<dispatcher-readiness>', 'exec')",
    dispatcher_readiness_command[2].to_s
  )
  check.call(syntax_status.success?, "dispatcher readiness Python is invalid: #{syntax_error.strip}")
else
  errors << "dispatcher readiness must execute Python directly"
end
check.call(app_container["image"] == runtime_data["SCENE_APP_IMAGE"], "rendered app image must match SCENE_APP_IMAGE")
check.call(dispatcher_container["image"] == runtime_data["SCENE_APP_IMAGE"], "rendered dispatcher image must match SCENE_APP_IMAGE")
check.call(runner_container["image"] == runtime_data["SCENE_RUNNER_IMAGE"], "rendered readiness image must match SCENE_RUNNER_IMAGE")

check.call(runner_job.dig("spec", "backoffLimit") == 0, "runner readiness must fail on its first deterministic error")
check.call(runner_job.dig("spec", "completions") == 1 && runner_job.dig("spec", "parallelism") == 1, "runner readiness must execute exactly once")
check.call(runner_job.dig("spec", "activeDeadlineSeconds") == 300, "runner readiness must have a five-minute deadline")
check.call(runner_job.dig("spec", "ttlSecondsAfterFinished") == 3600, "runner readiness must be garbage-collected after one hour")
runner_command = Array(runner_container["command"])
runner_script = Array(runner_container["args"]).join(" ")
check.call(runner_command == ["/bin/sh", "-ec"], "runner readiness must execute its bounded smoke script")
%w[/readyz scene-runner-readiness --browser-smoke --runner-script /opt/scene/runner.py /workspace].each do |contract|
  check.call(runner_script.include?(contract), "runner readiness smoke must verify #{contract}")
end
_stdout, shell_error, shell_status = Open3.capture3("/bin/sh", "-n", stdin_data: runner_script)
check.call(shell_status.success?, "runner readiness shell is invalid: #{shell_error.strip}")

limit_range = resource.call("LimitRange", "scene-runtime")
container_limit = Array(limit_range.dig("spec", "limits")).find { |entry| entry["type"] == "Container" } || {}
check.call(container_limit.dig("default", "ephemeral-storage") == "2Gi", "LimitRange must cap generated runner containers at 2Gi ephemeral storage")
check.call(container_limit.dig("defaultRequest", "ephemeral-storage") == "512Mi", "LimitRange must reserve 512Mi ephemeral storage for generated runner containers")

service = resource.call("Service", "scene")
check.call(service.dig("spec", "ports", 0, "port") == 80, "scene Service must expose port 80")
check.call(service.dig("spec", "ports", 0, "targetPort") == "http", "scene Service must target the named app port")
check.call(service.dig("spec", "selector", "app.kubernetes.io/component") == "app", "scene Service must select app pods")
ingress = resource.call("Ingress", "scene")
host = ingress.dig("spec", "rules", 0, "host")
check.call(host == "scene.135.181.140.68.sslip.io", "Ingress must use the agreed sslip.io host")
check.call(ingress.dig("spec", "ingressClassName") == "traefik", "Ingress class must be traefik")
check.call(ingress.dig("metadata", "annotations", "cert-manager.io/cluster-issuer") == "letsencrypt-prod", "Ingress must request TLS through cert-manager")
check.call(ingress.dig("spec", "tls", 0, "secretName") == "scene-staging-tls", "Ingress must use scene-staging-tls")
middleware = resource.call("Middleware", "scene-staging-auth")
check.call(middleware.dig("spec", "basicAuth", "secret") == "scene-ingress-basic-auth", "Traefik middleware must reference the external basic-auth Secret")
check.call(middleware.dig("spec", "basicAuth", "removeHeader") == true, "Traefik middleware must remove ingress BasicAuth credentials before proxying")

resource.call("PodDisruptionBudget", "scene-app")
pdb = resource.call("PodDisruptionBudget", "scene-app")
check.call(app.dig("spec", "replicas") == 2 && pdb.dig("spec", "minAvailable") == 1, "two app replicas must retain one available pod during voluntary disruption")

default_deny = resource.call("NetworkPolicy", "default-deny")
check.call(default_deny.dig("spec", "podSelector") == {}, "default-deny must select every SCENE pod")
check.call(Array(default_deny.dig("spec", "policyTypes")).sort == %w[Egress Ingress], "default-deny must cover ingress and egress")
dns_policy = resource.call("NetworkPolicy", "allow-dns-egress")
dns_ports = Array(dns_policy.dig("spec", "egress", 0, "ports")).map { |port| [port["protocol"], port["port"]] }.sort
check.call(dns_ports == [["TCP", 53], ["UDP", 53]], "DNS policy must allow only TCP/UDP port 53")
check.call(dns_policy.dig("spec", "egress", 0, "to", 0, "namespaceSelector", "matchLabels", "kubernetes.io/metadata.name") == "kube-system", "DNS policy must target kube-system")
check.call(dns_policy.dig("spec", "egress", 0, "to", 0, "podSelector", "matchLabels", "k8s-app") == "kube-dns", "DNS policy must target only kube-dns pods")

http01_policy = resource.call("NetworkPolicy", "allow-cert-manager-http01-ingress")
check.call(http01_policy.dig("spec", "podSelector", "matchLabels") == { "acme.cert-manager.io/http01-solver" => "true" }, "HTTP-01 policy must select only cert-manager solver pods")
check.call(Array(http01_policy.dig("spec", "policyTypes")) == ["Ingress"], "HTTP-01 policy must grant ingress only")
http01_ingress = Array(http01_policy.dig("spec", "ingress"))
http01_sources = Array(http01_ingress.dig(0, "from"))
http01_ports = Array(http01_ingress.dig(0, "ports"))
check.call(http01_ingress.length == 1 && http01_sources.length == 1, "HTTP-01 policy must contain one bounded Traefik ingress rule")
check.call(http01_sources.dig(0, "namespaceSelector", "matchLabels", "kubernetes.io/metadata.name") == "kube-system", "HTTP-01 ingress must be constrained to kube-system")
check.call(http01_sources.dig(0, "podSelector", "matchLabels") == { "app.kubernetes.io/name" => "traefik" }, "HTTP-01 ingress must be constrained to Traefik pods")
check.call(http01_ports == [{ "port" => 8089, "protocol" => "TCP" }], "HTTP-01 ingress must expose only solver TCP port 8089")

private_ranges = %w[10.0.0.0/8 100.64.0.0/10 127.0.0.0/8 169.254.0.0/16 172.16.0.0/12 192.168.0.0/16]
policy_for = lambda { |name| resource.call("NetworkPolicy", name) }
app_policy = policy_for.call("scene-app")
app_sources = Array(app_policy.dig("spec", "ingress", 0, "from"))
traefik_source = app_sources.find { |source| source.dig("podSelector", "matchLabels", "app.kubernetes.io/name") == "traefik" } || {}
runner_source = app_sources.find { |source| source.dig("podSelector", "matchLabels", "app.kubernetes.io/component") == "runner" } || {}
check.call(app_sources.length == 2, "app ingress must allow only Traefik and SCENE runner pods")
check.call(traefik_source.dig("namespaceSelector", "matchLabels", "kubernetes.io/metadata.name") == "kube-system", "Traefik ingress must be constrained to kube-system")
check.call(runner_source.dig("podSelector", "matchLabels", "app.kubernetes.io/name") == "scene" && !runner_source.key?("namespaceSelector"), "runner callbacks must be constrained to runner pods in the scene namespace")
app_external = Array(app_policy.dig("spec", "egress")).find { |entry| entry.dig("to", 0, "ipBlock", "cidr") == "0.0.0.0/0" } || {}
check.call(Array(app_external.dig("to", 0, "ipBlock", "except")).sort == private_ranges.sort, "app HTTPS egress must exclude private and link-local ranges")
check.call(Array(app_external["ports"]) == [{ "port" => 443, "protocol" => "TCP" }], "app external egress must be HTTPS only")

dispatcher_policy = policy_for.call("scene-dispatcher")
check.call(dispatcher_policy.dig("spec", "ingress") == [], "dispatcher must accept no ingress")
dispatcher_egress = Array(dispatcher_policy.dig("spec", "egress"))
dispatcher_https = dispatcher_egress.find { |entry| entry.dig("to", 0, "ipBlock", "cidr") == "0.0.0.0/0" } || {}
dispatcher_api = dispatcher_egress.find { |entry| entry.dig("to", 0, "ipBlock", "cidr") == "135.181.140.68/32" } || {}
check.call(dispatcher_egress.length == 2, "dispatcher must have only public HTTPS and horse API egress rules")
check.call(Array(dispatcher_https["ports"]) == [{ "port" => 443, "protocol" => "TCP" }], "dispatcher public egress must be HTTPS only")
check.call(Array(dispatcher_api["ports"]) == [{ "port" => 6443, "protocol" => "TCP" }], "dispatcher horse API egress must target TCP port 6443")

runner_policy = policy_for.call("scene-runner")
check.call(runner_policy.dig("spec", "ingress") == [], "runner must accept no ingress")
runner_egress = Array(runner_policy.dig("spec", "egress"))
runner_callback = runner_egress.find { |entry| entry.dig("to", 0, "podSelector", "matchLabels", "app.kubernetes.io/component") == "app" } || {}
runner_external = runner_egress.find { |entry| entry.dig("to", 0, "ipBlock", "cidr") == "0.0.0.0/0" } || {}
callback_ports = Array(runner_callback["ports"]).map { |port| port["port"] }.sort
external_ports = Array(runner_external["ports"]).map { |port| port["port"] }.sort
check.call(callback_ports == [80, 8000], "runner callback egress must be limited to SCENE service/app ports")
check.call(external_ports == [80, 443], "runner target egress must be limited to HTTP and HTTPS")
check.call(Array(runner_external.dig("to", 0, "ipBlock", "except")).sort == private_ranges.sort, "runner target egress must exclude private and link-local ranges")

warnings.uniq.each { |warning| warn "WARNING: #{warning}" }
if errors.empty?
  mode = options[:strict] ? "strict" : "template"
  puts "OK: rendered #{documents.length} resources from #{options[:directory]} and passed #{mode} SCENE k3s checks"
  exit 0
end

errors.uniq.each { |error| warn "ERROR: #{error}" }
warn "FAILED: #{errors.uniq.length} validation error#{errors.uniq.length == 1 ? '' : 's'}"
exit 1
