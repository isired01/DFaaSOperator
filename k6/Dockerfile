# Partiamo dall'immagine ufficiale di k6
FROM grafana/k6:1.4.2

# Passiamo a root per poter installare pacchetti e muovere file
USER root

# Installiamo curl per scaricare il client di MinIO
RUN apk add --no-cache curl

# Scarichiamo il client mc (MinIO Client) 
# NOTA: Se la tua VM è un'architettura diversa, cambia 'linux-arm64' in 'linux-amd64'
RUN curl https://dl.min.io/client/mc/release/linux-arm64/mc \
    --output /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

# Creiamo le cartelle di lavoro
RUN mkdir -p /test /tmp

# Impostiamo la directory di lavoro
WORKDIR /test

# Rimuoviamo l'entrypoint predefinito di k6. 
# Questo è FONDAMENTALE perché ci permette di eseguire script shell 
# (es. "k6 run ... && mc cp ...") invece di far partire solo k6.
ENTRYPOINT [""]