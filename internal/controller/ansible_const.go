package controller

const AnsiblePlaybookYaml = `---
- name: "Infrastructure and Platform Setup"
  hosts: all
  become: true
  vars:
    openfaas_url: "http://{{ ansible_host }}:31112"
    helm_env:
      KUBECONFIG: /etc/rancher/k3s/k3s.yaml
      HELM_CONFIG_HOME: /root/.config/helm
      HELM_DATA_HOME: /root/.local/share/helm

  tasks:
    # ... (tutti i task precedenti rimangono invariati fino a dopo il Wait for Gateway) ...

    - name: "Wait for OpenFaaS Gateway"
      ansible.builtin.shell: |
        kubectl rollout status -n dfaas deploy/gateway --timeout=90s
      environment:
        KUBECONFIG: /etc/rancher/k3s/k3s.yaml
      failed_when: false

    # --- NUOVA LOGICA DI RIMOZIONE (PRUNING) ---

    - name: "Get list of currently deployed functions"
      ansible.builtin.shell: "/usr/local/bin/faas-cli list --gateway={{ openfaas_url }} | tail -n +2 | awk '{print $1}'"
      register: deployed_functions_raw
      changed_when: false

    - name: "Remove functions no longer in the CRD"
      ansible.builtin.shell: "/usr/local/bin/faas-cli remove {{ item }} --gateway={{ openfaas_url }}"
      loop: "{{ deployed_functions_raw.stdout_lines }}"
      when: 
        - requested_functions is defined 
        - requested_functions != ""
        - item != ""
        - item not in (requested_functions | from_json | map(attribute='nome') | list)

    # --- TASK DI DEPLOY (AGGIORNATO) ---

    - name: "Deploy requested functions"
      ansible.builtin.shell: |
        /usr/local/bin/faas-cli deploy \
          --image={{ item.immagine }} \
          --name={{ item.nome }} \
          --gateway={{ openfaas_url }} \
          --env exec_timeout={{ item.execTimeout }}s \
          --env max_inflight={{ item.maxInflight }} \
          --label dfaas.timeout_ms={{ item.timeoutMs }} \
          --update=true
      environment:
        KUBECONFIG: /etc/rancher/k3s/k3s.yaml
      loop: "{{ requested_functions | from_json }}"
      when: requested_functions is defined and requested_functions != ""
`
