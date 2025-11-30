#!/bin/sh
set -e

apt update
apt install -y libaio1 wget unzip

# Criar diretório com caminho absoluto
mkdir -p /usr/local/airflow/.oracle
cd /usr/local/airflow/.oracle/

# Download oracle instant client zip
wget --output-document=client.zip https://download.oracle.com/otn_software/linux/instantclient/1923000/instantclient-basiclite-linux.x64-19.23.0.0.0dbru.zip
unzip client.zip
rm client.zip
apt remove -y wget unzip

echo Linking files
cd instantclient_19_23/
ln -fs libclntsh.so.12.1 libclntsh.so
ln -fs libocci.so.12.1 libocci.so

# Garantir permissões corretas
chmod -R 755 /usr/local/airflow/.oracle

echo Oracle Instant Client 12.1 installed at /usr/local/airflow/.oracle/instantclient_19_23/
