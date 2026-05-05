package controller

const AnsiblePlaybookYaml = `---
- name: "Setup DFaaS Node"
  hosts: all
  become: true
  vars:
    openfaas_url: "http://127.0.0.1:31112"
    k3s_config: "/etc/rancher/k3s/k3s.yaml"
  
  tasks:
    - name: Disable APT automatic upgrades
      ansible.builtin.copy:
        dest: /etc/apt/apt.conf.d/99-disable-auto-upgrades
        content: |
          APT::Periodic::Enable "0";
          APT::Periodic::Update-Package-Lists "0";
          APT::Periodic::Unattended-Upgrade "0";

    - name: Configure sudo env_keep
      ansible.builtin.copy:
        dest: /etc/sudoers.d/99-preserve-env
        mode: "0440"
        content: |
          Defaults:%sudo env_keep += "KUBECONFIG"
          Defaults !always_set_home

    - name: Install prerequisite packages
      ansible.builtin.apt:
        name: [buildah, curl, git, python3-pip, python3-kubernetes]
        state: present
        update_cache: yes

    - name: Disable UFW firewall 
      community.general.ufw:
        state: disabled

    - name: Ensure netfilter modules are loaded
      community.general.modprobe:
        name: "{{ item }}"
        state: present
      loop:
        - br_netfilter
        - nf_conntrack

    - name: Tune Kernel settings
      ansible.builtin.sysctl:
        name: "{{ item.key }}"
        value: "{{ item.value }}"
        sysctl_file: /etc/sysctl.d/20-k3s-custom.conf
        state: present
        reload: yes
        # Evita che il task fallisca se il file in /proc non esiste ancora
        ignoreerrors: "{{ true if item.key == 'net.netfilter.nf_conntrack_max' else false }}"
      loop:
        - { key: "net.core.somaxconn", value: "8192" }
        - { key: "net.ipv4.tcp_max_syn_backlog", value: "8192" }
        - { key: "net.ipv4.ip_local_port_range", value: "1024 65535" }
        - { key: "net.ipv4.tcp_tw_reuse", value: "1" }
        - { key: "net.netfilter.nf_conntrack_max", value: "262144" }  

    - name: Check if K3s is already installed
      ansible.builtin.stat:
        path: /usr/local/bin/k3s
      register: k3s_bin

    - name: Install K3S (only if missing)
      ansible.builtin.shell: 
        cmd: curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--disable traefik --disable servicelb" sh -
      when: not k3s_bin.stat.exists

    - name: Ensure K3s service is started and enabled
      ansible.builtin.systemd:
        name: k3s
        state: started
        enabled: yes

    - name: Wait for K3s nodes to be ready
      kubernetes.core.k8s_info:
        kind: Node
        kubeconfig: "{{ k3s_config }}"
      register: node_list
      until: node_list.resources | length > 0
      retries: 20
      delay: 5

    - name: Check if Helm is installed
      ansible.builtin.stat:
        path: /usr/local/bin/helm
      register: helm_bin

    - name: Install Helm
      ansible.builtin.shell: 
        cmd: curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
      when: not helm_bin.stat.exists

    - name: Add Helm Repos
      kubernetes.core.helm_repository:
        name: "{{ item.name }}"
        repo_url: "{{ item.url }}"
      loop:
        - { name: "haproxytech", url: "https://haproxytech.github.io/helm-charts" }
        - { name: "prometheus-community", url: "https://prometheus-community.github.io/helm-charts" }
        - { name: "openfaas", url: "https://openfaas.github.io/faas-netes/" }

    - name: Create Namespaces for OpenFaaS
      kubernetes.core.k8s:
        name: "{{ item }}"
        api_version: v1
        kind: Namespace
        state: present
        kubeconfig: "{{ k3s_config }}"
      loop: [openfaas, openfaas-fn, monitoring, haproxy-controller]

    - name: "Create local config directory on nodes"
      ansible.builtin.file:
        path: /opt/dfaas/helm-values
        state: directory
        mode: '0755'

    - name: "Copy Helm values"
      ansible.builtin.copy:
        src: "/opt/helm-values/{{ item }}.yaml"
        dest: "/opt/dfaas/helm-values/{{ item }}.yaml"
      loop: [haproxy, prometheus, openfaas]
      register: helm_values_copy

    - name: Install Helm Charts
      kubernetes.core.helm:
        name: "{{ item.name }}"
        chart_ref: "{{ item.chart }}"
        release_namespace: "{{ item.ns }}"
        create_namespace: yes
        wait: yes
        kubeconfig: "{{ k3s_config }}"
        values_files:
          - "/opt/dfaas/helm-values/{{ item.name }}.yaml"
      loop:
        - { name: "haproxy", chart: "haproxytech/haproxy", ns: "haproxy-controller" }
        - { name: "prometheus", chart: "prometheus-community/prometheus", ns: "monitoring" }
        - { name: "openfaas", chart: "openfaas/openfaas", ns: "openfaas"}
      register: helm_install_result
      retries: 5           # Tenta 5 volte prima di fallire
      delay: 15            # Aspetta 15 secondi tra un tentativo e l'altro
      until: helm_install_result is succeeded

    - name: Check if faas-cli is installed
      ansible.builtin.stat:
        path: /usr/local/bin/faas-cli
      register: faas_cli_bin

    - name: Install faas-cli
      ansible.builtin.shell: curl -sSL https://cli.openfaas.com | sh
      when: not faas_cli_bin.stat.exists

    - name: "Wait for OpenFaaS Gateway"
      ansible.builtin.uri:
        url: "{{ openfaas_url }}/system/functions"
        status_code: [200, 401]
      register: gateway_check
      until: gateway_check.status in [200, 401]
      retries: 40
      delay: 10

    - name: "Get deployed functions"
      ansible.builtin.shell: 
        cmd: "/usr/local/bin/faas-cli list --gateway={{ openfaas_url }} --quiet"
      register: deployed_functions_raw
      retries: 5
      delay: 5
      until: deployed_functions_raw.rc == 0

    - name: "Pruning: Remove old functions"
      ansible.builtin.shell: "/usr/local/bin/faas-cli remove {{ item }} --gateway={{ openfaas_url }}"
      loop: "{{ deployed_functions_raw.stdout_lines | default([]) }}"
      when: 
        - node_specific_functions is defined
        - item != ""
        - item not in (node_specific_functions | map(attribute='name') | list)

    - name: "Deploy functions from CRD"
      ansible.builtin.shell: |
        /usr/local/bin/faas-cli deploy \
          --image={{ item.image }} \
          --name={{ item.name }} \
          --gateway={{ openfaas_url }} \
          --env exec_timeout={{ item.execTimeout }}s \
          --env max_inflight={{ item.maxInflight }} \
          --label dfaas.timeout_ms={{ item.timeoutMs }} \
          --update=true
      loop: "{{ node_specific_functions }}"
      when: node_specific_functions is defined and node_specific_functions | length > 0
      register: function_deploy
      changed_when: "'Updating' in function_deploy.stdout or 'Deployed' in function_deploy.stdout"

    - name: "Setup Kubeconfig for root"
      ansible.builtin.file:
        path: /root/.kube
        state: directory
        mode: '0700'

    - name: "Link K3s config for root"
      ansible.builtin.file:
        src: "{{ k3s_config }}"
        dest: /root/.kube/config
        state: link
        force: yes

    - name: "Install dFaaS Agent via Helm"
      kubernetes.core.helm:
        name: "dfaas-agent"
        chart_ref: "oci://ghcr.io/isired01/dfaas-agent-chart"
        chart_version: "0.1.3"
        release_namespace: "default"
        kubeconfig: "{{ k3s_config }}"
        wait: no
        values:
          image: "ghcr.io/isired01/dfaas-agent:dev"
          imagePullPolicy: "Always"
          privateKey: |
            -----BEGIN PRIVATE KEY-----
            {{ node_priv_key }}
            -----END PRIVATE KEY-----
          config:
            AGENT_DEBUG: "true"
            AGENT_BOOTSTRAP_NODES: "{{ 'true' if is_bootstrap | bool else 'false' }}" 
            AGENT_BOOTSTRAP_NODES_LIST: "{{ bootstrap_address }}"
            AGENT_STRATEGY: "{{ balancing_strategy }}"
          forecaster:
            enabled: false

    - name: "Fix Prometheus host in Gateway"
      kubernetes.core.k8s:
        state: present
        kubeconfig: "{{ k3s_config }}"
        definition:
          apiVersion: apps/v1
          kind: Deployment
          metadata:
            name: gateway
            namespace: openfaas
          spec:
            template:
              spec:
                containers:
                  - name: gateway
                    env:
                      - name: faas_prometheus_host
                        value: "prometheus.monitoring.svc.cluster.local"
                      - name: faas_prometheus_port
                        value: "9090"`

const HaproxyValues = `
image:
  image: docker.io/haproxytech/haproxy-alpine
  tag: "3.2.6"

config: |
  # This is the basic HAProxy configuration, which overwrites the default
  # configuration from the Helm chart.
  global
    # Enable master-worker node to enable hot-reload feature.
    # See: https://docs.haproxy.org/3.2/configuration.html#3.1-master-worker
    master-worker

    # Enable Runtime API. This allows Data Plane API to make some changes
    # without requiding a reload. We expose the socket as TCP rather than UNIX
    # socket to allow Data Plane API to be in a different container than the
    # proxy.
    #
    # See: https://www.haproxy.com/documentation/haproxy-data-plane-api/installation/install-on-haproxy/
    # See: https://www.haproxy.com/documentation/haproxy-runtime-api/installation/
    stats socket ipv4@*:6666 mode 660 level admin expose-fd listeners
    log stdout format raw local0

  # An user is required by Dataplane API to work.
  userlist dataplaneapi
    user admin insecure-password admin

  defaults
    mode http
    option httplog
    log global
    timeout client 60s
    timeout connect 60s
    timeout server 60s

  # Allow Prometheus to scrape metrics.
  # See: https://www.haproxy.com/documentation/haproxy-configuration-tutorials/alerts-and-monitoring/prometheus/
  frontend prometheus
    bind :8405
    http-request use-service prometheus-exporter
    no log

  # This section will be overwritten by the DFaaS Agent with content that
  # depends on the selected offloading strategy.
  frontend main
    bind :80
    http-request return status 503 content-type "text/plain" string "This is a DFaaS node. Proxy is running, but the DFaaS agent is not!\n"

# The original HAProxy configuration cannot be modified, so we placed it in a
# different location.
configMount:
  mountPath: /usr/local/etc/haproxy/haproxy.init.readonly.cfg 
  subPath: haproxy.cfg        

initContainers:
  # Since the initial HAProxy configuration is read-only, we need to copy it to
  # a writable volume. Then, we must configure HAProxy and Data Plane API to use
  # the copied configuration file.
  - name: init-haproxy-config
    image: "busybox:musl"
    command: ["/bin/sh", "-c", "cp /opt/haproxy-config/haproxy.cfg /opt/haproxy-config-writable/haproxy.cfg && chmod 644 /opt/haproxy-config-writable/haproxy.cfg"]
    volumeMounts:
      - name: haproxy-config
        mountPath: /opt/haproxy-config
      - name: haproxy-config-writable
        mountPath: /opt/haproxy-config-writable

extraVolumes:
  # This volume will only contain the writable HAProxy configuration file.
  - name: haproxy-config-writable
    emptyDir: {}

extraVolumeMounts:
  # Mounted on the main container "haproxy".
  - name: haproxy-config-writable
    mountPath: /usr/local/etc/haproxy/config

args:
  # Read the configuration from the writable mount. Also enable master CLI as
  # UNIX socket, used by the Data Plane API. This is not exposed outside the
  # pod.
  #
  # See: https://docs.haproxy.org/3.3/management.html#9.4
  # See also: https://github.com/haproxytech/dataplaneapi/issues/391#issuecomment-3929209196
  defaults: ["-W", "-S", "/usr/local/etc/haproxy/config/haproxy-master.sock,mode,666,level,admin", "-db", "-f", "/usr/local/etc/haproxy/config/haproxy.cfg"]

# We expose HAProxy as a NodePort service to make it accessible from outside the
# cluster. As a result, other internal services (Runtime API, Data Plane API,
# and the Prometheus endpoint) are also exposed. While this may pose a security
# risk, keep in mind that DFaaS is a prototype.
#
# Note that the Runtime API is for the worker process, not the master (master
# CLI)!
service:
  type: NodePort

  # Let Prometheus to automatically scrape HAProxy metrics.
  # See: https://github.com/prometheus-community/helm-charts/tree/main/charts/prometheus#scraping-pod-metrics-via-annotations
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8405"

  # We do not intend to expose the Prometheus port, but Kubernetes will still
  # map it to a random node port regardless.
  nodePorts:
    http: 30080
    dataplaneapi: 30555
    runtimeapi: 30666

# Required to enable Data Plane API sending UNIX signals to the HAProxy master
# node running on the main container.
shareProcessNamespace:
  enabled: true

# Start the Data Plane API in a dedicated container. The API connects to the
# master CLI UNIX socket exposed by HAProxy through the custom shared path between
# the two containers. Reload operations are performed via UNIX signals, which
# is why "shareProcessNamespace" is set to "true".
sidecarContainers:
  - name: dataplaneapi
    image: docker.io/haproxytech/haproxy-alpine:3.2.6
    # We cannot run the Data Plane API directly, as we observed that it crashes
    # on the first attempt. Therefore, we need to execute it within a loop.
    command: ["/bin/sh", "-c"]
    args:
      - |
        while true; do
          /usr/local/bin/dataplaneapi \
            --host 0.0.0.0 \
            --port 5555 \
            --log-level info \
            --master-runtime /usr/local/etc/haproxy/config/haproxy-master.sock \
            --userlist dataplaneapi \
            --config-file /usr/local/etc/haproxy/config/haproxy.cfg \
            --reload-cmd "pkill -SIGUSR2 haproxy" \
            --restart-cmd "pkill -SIGUSR1 haproxy" \
          && break
          echo "Dataplane API failed to start, retrying in 5s..."
          sleep 5
        done
    volumeMounts:
      - name: haproxy-config-writable
        mountPath: /usr/local/etc/haproxy/config

# These ports will be automatically included in the service object and exposed
# for external access (because we selected NodePort service type).
containerPorts:
  http: 80
  https: 443
  runtimeapi: 6666
  dataplaneapi: 5555
  prometheus: 8405
`

const OpenfaasValues = `
alertmanager:
  create: false

prometheus:
  create: false


# Disable authentication for the OpenFaaS Gateway, as we are building a
# prototype. This simplifies the interaction with the gateway.
basic_auth: false
generateBasicAuth: false

# We disable asynchronous function invocation in OpenFaaS. This feature is
# fairly limited in the Community Edition, and doing so avoids the creation and
# instantiation of services (such as NATS) that we would not use anyway.
#
# More info here: https://docs.openfaas.com/reference/async/
async: false

functionNamespace: openfaas-fn

gatewayExternal:
  annotations:
    # Let Prometheus to automatically scrape OpenFaaS metrics.
    # See: https://github.com/prometheus-community/helm-charts/tree/main/charts/prometheus#scraping-pod-metrics-via-annotations
    prometheus.io/scrape: "true"
    prometheus.io/path: /metrics
    prometheus.io/port: "8081"

gateway:
  readinessProbe:
    periodSeconds: 10
    timeoutSeconds: 10
    successThreshold: 1
    failureThreshold: 3

  livenessProbe:
    periodSeconds: 30
    timeoutSeconds: 30
    successThreshold: 1
    failureThreshold: 3

functions:
  # Disable readiness and liveness probes for function pods, since we limit
  # concurrency with max_inflight env var directly in the functions.
  httpProbe: false


`

const PrometheusValues = `
# Install only prometheus-node-exporter and alertmanager services.
prometheus-pushgateway:
  enabled: false
kube-state-metrics:
  enabled: false

server:
  global:
    # Change default (1 minute) to 5 seconds.
    scrape_interval: 5s
    evaluation_interval: 5s
    scrape_timeout: 4s

  # Decrease the default retention time. This is enough for the DFaaS prototype.
  retention: "5d"

  # Enable retention size to 85% of the allocated Persistent Volume.
  retentionSize: "5.1GB"

  persistentVolume:
    # Decrease default volume size (8Gi).
    size: 6Gi

  # Make sure the service is named "prometheus", as this DNS name is hardcoded
  # in the OpenFaaS CE Gateway and cannot be changed.
  fullnameOverride: "prometheus"

  service:
    # This additional port is required because port 9090 is hardcoded in the
    # OpenFaaS CE Gateway. By default there is port 80.
    additionalPorts:
      - name: openfaas
        port: 9090

serverFiles:
  alerting_rules.yml:
    # Copied from official OpenFaaS Helm chart.
    # See: https://github.com/openfaas/faas-netes/blob/87eca610c2c0cd4ed7b7aa37df716a6f98fb2851/chart/openfaas/templates/prometheus-cfg.yaml#L66
    #
    # I have removed the filter {code="200"} from the query.
    groups:
      - name: openfaas
        rules:
        - alert: APIHighInvocationRate
          expr: sum(rate(gateway_function_invocation_total[10s])) BY (function_name) > 5
          for: 5s
          labels:
            service: gateway
            severity: major
          annotations:
            description: High invocation total on "{{ "{{" }}$labels.function_name{{ "}}" }}"
            summary: High invocation total on "{{ "{{" }}$labels.function_name{{ "}}" }}"

# Prometheus's Alertmanager configuration.
alertmanager:
  enabled: true

  config:
    # The whole config is copied from the official OpenFaaS Helm chart.
    # See: https://github.com/openfaas/faas-netes/blob/87eca610c2c0cd4ed7b7aa37df716a6f98fb2851/chart/openfaas/templates/alertmanager-cfg.yaml#L17-L49
    route:
      group_by: ['alertname', 'cluster', 'service']
      group_wait: 5s
      group_interval: 10s
      repeat_interval: 30s
      receiver: scale-up
      routes:
      - match:
          service: gateway
          receiver: scale-up
          severity: major

    inhibit_rules:
      - source_match:
          severity: 'critical'
        target_match:
          severity: 'warning'
        equal: ['alertname', 'cluster', 'service']

    receivers:
      - name: "scale-up"
        webhook_configs:
          - url: http://gateway.openfaas.svc.cluster.local:8080/system/alert
            send_resolved: true

# Extra manifests to deploy as an array.
extraManifests:
  - |
    # This is a utility NodePort service used to expose the Prometheus Web UI
    # outside of the Kubernetes cluster to port 30909. Use for debugging in the
    # PoC version.
    apiVersion: v1
    kind: Service
    metadata:
      name: prometheus-external
    spec:
      type: NodePort
      selector:
        app.kubernetes.io/component: server
        app.kubernetes.io/instance: prometheus
        app.kubernetes.io/name: prometheus
      ports:
        - name: http
          port: 9090
          targetPort: 9090
          nodePort: 30909


`
