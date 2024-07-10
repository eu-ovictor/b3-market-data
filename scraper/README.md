---

# Scraper de Arquivos de Negócios da B3

Este projeto é um scraper criado para baixar automaticamente os arquivos de negócios da B3. Ele pode ser executado localmente ou através do Docker Compose.

## Requisitos

Para executar o scraper localmente, você precisará de:

- Python 3.11
- Poetry

## Instalação e Execução

### Executando Localmente

1. Clone o repositório:
    ```sh
    git clone https://github.com/eu-ovictor/b3-market-data.git
    cd scraper
    ```

2. Instale as dependências usando Poetry:
    ```sh
    poetry install
    ```

3. Execute o script:
    ```sh
    poetry run main.py
    ```

### Variáveis de Ambiente

O script utiliza duas variáveis de ambiente:

- `DOWNLOADS_DIR`: O diretório onde os arquivos baixados serão salvos. O valor padrão é `downloads`.
- `OFFSET`: Permite configurar o período de dias anteriores dos arquivos que deseja baixar. 

Exemplo de como definir as variáveis de ambiente e executar o script:
```sh
export DOWNLOADS_DIR=meus_downloads
export OFFSET=7
poetry run main.py
```

### Executando com Docker Compose

1. Certifique-se de que você tenha o Docker e o Docker Compose instalados.

2. Crie o diretório `downloads`:
    ```sh
    mkdir downloads
    ```

3. Execute o Docker Compose:
    ```sh
    docker-compose run scraper
    ```