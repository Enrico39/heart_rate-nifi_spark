#!/bin/bash

# Script di installazione automatica di Apache NiFi v1.19.1 su Ubuntu 22 LTS (Dataproc Master Node)
# Creato per la demo Heart Rate Monitoring MVP

set -e

echo "================================================================="
echo "  INSTALLAZIONE E CONFIGURAZIONE DI APACHE NIFI 1.19.1 SU MASTER "
echo "================================================================="

# 1. Verifica dei requisiti (Java)
echo "[1/5] Verifica della presenza di Java..."
if command -v java >/dev/null 2>&1; then
    JAVA_VER=$(java -version 2>&1 | head -n 1)
    echo "   -> Java trovato: $JAVA_VER"
else
    echo "   -> Java non trovato. Installazione di OpenJDK 11 in corso..."
    sudo apt-get update
    sudo apt-get install -y openjdk-11-jdk
    echo "   -> OpenJDK 11 installato con successo."
fi

# 2. Download di Apache NiFi
NIFI_VERSION="1.19.1"
NIFI_ZIP="nifi-${NIFI_VERSION}-bin.zip"
NIFI_URL="https://archive.apache.org/dist/nifi/${NIFI_VERSION}/${NIFI_ZIP}"
INSTALL_DIR="$HOME/nifi"

echo "[2/5] Controllo del pacchetto di Apache NiFi ($NIFI_VERSION)..."
if [ ! -f "$NIFI_ZIP" ]; then
    echo "   -> Download in corso da $NIFI_URL ..."
    if command -v wget >/dev/null 2>&1; then
        wget -q --show-progress "$NIFI_URL" || { echo "Wget fallito. Prova ad effettuare il download locale."; exit 1; }
    elif command -v curl >/dev/null 2>&1; then
        curl -L -O --progress-bar "$NIFI_URL" || { echo "Curl fallito. Prova ad effettuare il download locale."; exit 1; }
    else
        echo "   -> [ERRORE] Nessun tool (wget/curl) disponibile per il download diretto."
        exit 1;
    fi
    echo "   -> Scaricato con successo."
else
    echo "   -> Archivio zip già presente localmente."
fi

# 3. Estrazione
echo "[3/5] Estrazione dell'archivio zip in $INSTALL_DIR..."
# Creiamo la cartella padre se non esiste
mkdir -p "$(dirname "$INSTALL_DIR")"

if command -v unzip >/dev/null 2>&1; then
    unzip -q "$NIFI_ZIP" -d "$(dirname "$INSTALL_DIR")"
else
    echo "   -> unzip non trovato. Estrazione in corso con Python 3 (operazione lenta)..."
    python3 -c "import zipfile; zipfile.ZipFile('$NIFI_ZIP').extractall(path='$(dirname "$INSTALL_DIR")')"
fi

# Il file zip estrae la cartella nifi-1.19.1
# Rinominiamo la cartella in 'nifi' per uniformare i percorsi
if [ -d "$HOME/nifi-${NIFI_VERSION}" ]; then
    rm -rf "$INSTALL_DIR"
    mv "$HOME/nifi-${NIFI_VERSION}" "$INSTALL_DIR"
fi
echo "   -> Estratto con successo in $INSTALL_DIR"

# 4. Configurazione nifi.properties & bootstrap.conf
echo "[4/5] Configurazione dei file di NiFi..."
cd "$INSTALL_DIR"

# Disabilita HTTPS ed abilita HTTP sulla porta 8090
echo "   -> Configurazione nifi.properties (HTTP su porta 8090, Ingestione su 8080 free)..."
# Rimuove valori HTTPS per evitare conflitti
sed -i 's/nifi.web.https.host=.*/nifi.web.https.host=/g' conf/nifi.properties
sed -i 's/nifi.web.https.port=.*/nifi.web.https.port=/g' conf/nifi.properties
# Imposta parametri HTTP
sed -i 's/nifi.web.http.host=.*/nifi.web.http.host=0.0.0.0/g' conf/nifi.properties
sed -i 's/nifi.web.http.port=.*/nifi.web.http.port=8090/g' conf/nifi.properties

# Pulisce completamente i parametri TLS/SSL per evitare errori di avvio del FlowController
sed -i 's/nifi.security.keystore=.*/nifi.security.keystore=/g' conf/nifi.properties
sed -i 's/nifi.security.keystoreType=.*/nifi.security.keystoreType=/g' conf/nifi.properties
sed -i 's/nifi.security.keystorePasswd=.*/nifi.security.keystorePasswd=/g' conf/nifi.properties
sed -i 's/nifi.security.keyPasswd=.*/nifi.security.keyPasswd=/g' conf/nifi.properties
sed -i 's/nifi.security.truststore=.*/nifi.security.truststore=/g' conf/nifi.properties
sed -i 's/nifi.security.truststoreType=.*/nifi.security.truststoreType=/g' conf/nifi.properties
sed -i 's/nifi.security.truststorePasswd=.*/nifi.security.truststorePasswd=/g' conf/nifi.properties
sed -i 's/nifi.remote.input.secure=.*/nifi.remote.input.secure=false/g' conf/nifi.properties

# Ottimizzazione della RAM (max heap size 2GB per evitare crash OOM del master VM)
echo "   -> Configurazione bootstrap.conf (JVM Heap limitato a 2GB)..."
sed -i 's/java.arg.3=-Xmx.*/java.arg.3=-Xmx2048m/g' conf/bootstrap.conf

# 5. Avvio del servizio
echo "[5/5] Avvio di Apache NiFi..."
./bin/nifi.sh start

echo "================================================================="
echo "           APACHE NIFI CONFIGURATO CON SUCCESSO!                 "
echo "================================================================="
echo " NiFi è avviato in background. Il caricamento iniziale può"
echo " richiedere da 1 a 2 minuti."
echo ""
echo " Comandi utili per la gestione (esegui da $INSTALL_DIR):"
echo "  - Controlla stato:   ./bin/nifi.sh status"
echo "  - Vedi log:          tail -f logs/nifi-app.log"
echo "  - Arresta NiFi:      ./bin/nifi.sh stop"
echo "  - Riavvia NiFi:      ./bin/nifi.sh restart"
echo ""
echo " Per accedere alla UI dal tuo browser locale, avvia un tunnel"
echo " SSH dal tuo terminale locale:"
echo "   ssh -N -L 8090:localhost:8090 -L 8080:localhost:8080 enricomadonna0@<IP-ESTERNO-MASTER>"
echo ""
echo " E visita: http://localhost:8090/nifi"
echo "================================================================="
