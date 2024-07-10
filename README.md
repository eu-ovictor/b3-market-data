---

# B3 Market Data

Este projeto é uma API simples para dados de negócios da B3. A aplicação possui uma toolbox que permite executar o loader para carregar dados baixados no banco de dados e um web server para servir esses dados através de uma API.

## Modelo de Dados

Esta aplicação utiliza o TimescaleDB para lidar com as séries temporais que são os dados de negócio. Tirando vantagem das hypertables e da view materializada para construir os dados de resumo que são consultados. Para executar o loader e a API, você precisará de um TimescaleDB, que pode ser executado localmente usando o Docker Compose.
```sh
    docker-compose up db
```

## Requisitos

Para executar a aplicação localmente, você precisará de:

- Go (Golang) instalado

## Instalação e Execução

### Executando Localmente

1. Clone o repositório:
    ```sh
    git clone https://github.com/eu-ovictor/b3-market-data.git
    cd b3-market-data
    ```

2. Construa o binário usando Go:
    ```sh
    go build -o b3-market-data
    ```

3. Execute o loader para carregar os dados baixados no banco de dados:
    ```sh
    ./b3-market-data load -b <quantidade de linhas a serem inseridas de uma vez> -u <url do banco> -d <diretório contendo arquivos baixados>
    ```
    **Disclaimer:** A execução do loader pode demorar um pouco dependendo da quantidade de arquivos a serem carregados, pois os arquivos são grandes.
    
    Para mais informações sobre como usar o loader, execute:
    ```sh
    ./b3-market-data load --help
    ```

4. Inicie o web server da API:
    ```sh
    ./b3-market-data api -p <porta> -u <url do banco>
    ```
    Para mais informações sobre como usar a API, execute:
    ```sh
    ./b3-market-data api --help
    ```

### Executando com Docker Compose

Para facilitar a execução, você pode usar o Docker Compose.

1. Certifique-se de que você tenha o Docker e o Docker Compose instalados.

2. Crie o diretório `downloads` e coloque os arquivos baixados nele:
    ```sh
    mkdir downloads
    ```

3. Execute o loader usando o Docker Compose:
    ```sh
    docker-compose up loader
    ```
    **Disclaimer:** A execução do loader pode demorar um pouco dependendo da quantidade de arquivos a serem carregados, pois os arquivos são grandes.

4. Inicie a API usando o Docker Compose:
    ```sh
    docker-compose up api
    ```

### Dados Previamente Baixados

O repositório contém alguns arquivos de dados previamente baixados. Esses arquivos podem ser substituídos manualmente ou através do que foi gerado pelo [scraper](https://github.com/eu-ovictor/b3-market-data/tree/main/scraper).

### Testes de Integração

A aplicação contém alguns testes de integração que conectam ao banco de dados. Para executar os testes de integração, use o Docker Compose:

## Documentação da API

### Rotas

#### 1. Buscar Todas as Informações de Negócios

- **Rota:** `/trades`
- **Método:** GET
- **Descrição:** Retorna todas as informações de negócios. Permite um filtro opcional por data.
- **Parâmetros de Query:**
  - `date` (opcional): Data no formato "YYYY-MM-DD". Quando enviado, os valores serão agregados apenas para dias maiores ou iguais a este valor.
- **Exemplo de Requisição:**
  ```sh
  GET /trades?date=2024-07-01
  ```
- **Exemplo de Resposta:**
  ```json
  [
    {
      "ticker": "AAPL",
      "max_range_value": 150.50,
      "max_daily_volume": 100000
    },
    {
      "ticker": "GOOGL",
      "max_range_value": 2800.75,
      "max_daily_volume": 50000
    }
  ]
  ```

#### 2. Buscar Informações de um Negócio Específico

- **Rota:** `/trades/:ticker`
- **Método:** GET
- **Descrição:** Retorna informações de um negócio específico. Permite um filtro opcional por data.
- **Parâmetros de Query:**
  - `date` (opcional): Data no formato "YYYY-MM-DD". Quando enviado, os valores serão agregados apenas para dias maiores ou iguais a este valor.
- **Exemplo de Requisição:**
  ```sh
  GET /trades/AAPL?date=2024-07-01
  ```
- **Exemplo de Resposta:**
  ```json
  {
    "ticker": "AAPL",
    "max_range_value": 150.50,
    "max_daily_volume": 100000
  }
  ```

### Estrutura de Resposta

A resposta da API é um JSON contendo os seguintes campos:
- `ticker`: O identificador do negócio.
- `max_range_value`: O maior valor ao qual foi negociado naquele período.
- `max_daily_volume`: A maior soma de quantidades em um mesmo dia para os dias naquele período.

## Possíveis melhorias

- Implementação de content-negotiation para permitir múltiplos formatos de comunicação, como Protobufs, YAML, CSV, entre outros.
- Melhoria da camada de acesso e configuração do banco de dados.
- Adição de mais métodos na API.