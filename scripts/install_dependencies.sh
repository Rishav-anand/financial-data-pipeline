#!/bin/bash

echo "[*] Checking Python3..."
if ! command -v python3 &> /dev/null
then
    echo "[*] Python3 not found. Installing..."
    sudo yum update -y
    sudo yum install python3 -y
else
    echo "[✓] Python3 already installed."
fi

echo "[*] Checking pip3..."
if ! command -v pip3 &> /dev/null
then
    echo "[*] pip3 not found. Installing..."
    sudo dnf install python3-pip -y
else
    echo "[✓] pip3 already installed."
fi

echo "[*] Installing boto3 and requests..."
pip3 install --user requests boto3

echo "[*] Installing all dependencies of padndas file..."
pip3 install pandas
pip3 install fsspec
pip3 install s3fs

echo "[✓] All dependencies installed."


