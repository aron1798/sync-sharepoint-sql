name: Sync Excel to Azure SQL

on:
  schedule:
    - cron: '*/15 * * * *'  # Cada 15 minutos
  workflow_dispatch:
  push:
    paths:
      - 'base_combinada.xlsx'

jobs:
  sync-data:
    runs-on: ubuntu-latest
    
    steps:
    - name: 📥 Descargar código y Excel
      uses: actions/checkout@v4
      
    - name: 🐍 Configurar Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
        
    - name: 📦 Instalar ODBC Driver
      run: |
        curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
        curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
        sudo apt-get update
        sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
        sudo apt-get install -y unixodbc-dev
        
    - name: 📚 Instalar dependencias
      run: |
        pip install -r requirements.txt
        
    - name: 🚀 Ejecutar sincronización
      env:
        SQL_SERVER: ${{ secrets.SQL_SERVER }}
        SQL_DATABASE: ${{ secrets.SQL_DATABASE }}
        SQL_USERNAME: ${{ secrets.SQL_USERNAME }}
        SQL_PASSWORD: ${{ secrets.SQL_PASSWORD }}
      run: python sync_script.py
