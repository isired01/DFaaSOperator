package controller

const AnsiblePlaybookYaml = `---
- name: "Setup DFaaS Node"
  hosts: all
  become: true
  vars:
    openfaas_url: "http://127.0.0.1:31112"
  
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
      ansible.builtin.shell: "ufw disable || true"

    - name: Tune Kernel settings
      ansible.builtin.copy:
        dest: /etc/sysctl.d/20-k3s-custom.conf
        content: |
          net.core.somaxconn = 8192
          net.ipv4.tcp_max_syn_backlog = 8192
          net.ipv4.ip_local_port_range = 1024 65535
          net.ipv4.tcp_tw_reuse = 1
          net.netfilter.nf_conntrack_max = 262144
      register: sysctl_config

    - name: Apply sysctl
      ansible.builtin.command: sysctl --system
      when: sysctl_config.changed

    - name: Install K3S
      ansible.builtin.shell: curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--disable traefik --disable servicelb" sh -
      args:
        creates: /usr/local/bin/k3s

    - name: Wait for K3s nodes
      ansible.builtin.shell: "kubectl get nodes"
      environment:
        KUBECONFIG: /etc/rancher/k3s/k3s.yaml
      register: k3s_ready
      retries: 20
      delay: 5
      until: k3s_ready.rc == 0

    - name: Install Helm
      ansible.builtin.shell: "curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"
      args:
        creates: /usr/local/bin/helm

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
      environment:
        KUBECONFIG: /etc/rancher/k3s/k3s.yaml
      loop:
        - openfaas
        - openfaas-fn
        - monitoring
        - haproxy-controller

    - name: Install Helm Charts
      kubernetes.core.helm:
        name: "{{ item.name }}"
        chart_ref: "{{ item.chart }}"
        release_namespace: "{{ item.ns }}"
        create_namespace: yes
        wait: yes
        values: "{{ item.helm_values | default({}) }}"
      environment:
        KUBECONFIG: /etc/rancher/k3s/k3s.yaml
      loop:
        - { name: "haproxy", chart: "haproxytech/haproxy", ns: "haproxy-controller" }
        - { name: "prometheus", chart: "prometheus-community/prometheus", ns: "monitoring" }
        - { name: "openfaas", chart: "openfaas/openfaas", ns: "openfaas", helm_values: { "functionNamespace": "openfaas-fn", "generateBasicAuth": false, "basic_auth": false } }
        - 

    - name: Install faas-cli
      ansible.builtin.shell: "curl -sSL https://cli.openfaas.com | sh"
      args:
        creates: /usr/local/bin/faas-cli

    - name: "Wait for OpenFaaS Gateway"
      ansible.builtin.uri:
        url: "http://127.0.0.1:31112/system/functions"
        status_code: [200, 401]
      register: gateway_check
      until: gateway_check.status in [200, 401]
      retries: 40
      delay: 10

    # --- LOGICA DINAMICA OPERATORE ---

    - name: "Get deployed functions"
      ansible.builtin.shell: "/usr/local/bin/faas-cli list --gateway={{ openfaas_url }} | tail -n +2 | awk '{print $1}'"
      register: deployed_functions_raw
      changed_when: false

    - name: "Pruning: Remove old functions"
      ansible.builtin.shell: "/usr/local/bin/faas-cli remove {{ item }} --gateway={{ openfaas_url }}"
      loop: "{{ deployed_functions_raw.stdout_lines }}"
      when: 
        - node_specific_functions is defined
        - item != ""
        # Usiamo il filtro 'force_list' o semplicemente verifichiamo il nome
        - item not in (node_specific_functions | map(attribute='nome') | list)

    - name: "Deploy functions from CRD"
      ansible.builtin.shell: |
        /usr/local/bin/faas-cli deploy \
          --image={{ item.immagine }} \
          --name={{ item.nome }} \
          --gateway={{ openfaas_url }} \
          --env exec_timeout={{ item.execTimeout }}s \
          --env max_inflight={{ item.maxInflight }} \
          --label dfaas.timeout_ms={{ item.timeoutMs }} \
          --update=true
      # Rimosso | from_json perché Ansible lo vede già come lista
      loop: "{{ node_specific_functions }}"
      when: node_specific_functions is defined and node_specific_functions | length > 0
`
