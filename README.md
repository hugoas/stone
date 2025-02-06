# Desafio Stone: Data Engineer

## 📌 Visão Geral
Este projeto tem como objetivo processar os dados abertos da Receita Federal do Brasil sobre empresas e sócios, seguindo o modelo de camadas **Bronze, Silver e Gold** para transformação e estruturação dos dados. O ambiente é totalmente containerizado com Docker e utiliza PostgreSQL para armazenar os resultados finais.

Os dados são extraídos da URL oficial:
🔗 [Receita Federal - Dados Abertos](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)

## 🛠️ Tecnologias Utilizadas
- **Python 3.11**
- **PostgreSQL 17**
- **Pandas**
- **SQLAlchemy**
- **Docker & Docker Compose**
- **TQDM**
- **Python-Dateutil**

## 📌 Estrutura de Arquivos

```
stone/
├── data/                 # Diretório para armazenamento de dados (ex.: JSON de entrada)
├── src/                  # Código-fonte principal
│   ├── etl.py            # Script ETL para processar e carregar os dados
│   ├── database.py       # Configuração e conexão com o banco de dados PostgreSQL
│   ├── transform.py      # Funções de transformação de dados
│   └── utils.py          # Funções utilitárias
├── Dockerfile            # Configuração do container Docker
├── docker-compose.yml    # Orquestração com Docker Compose
├── requirements.txt      # Dependências do projeto
└── README.md             # Documentação do projeto
```

## 📊 Estrutura da Tabela Final (Gold)

A tabela gerada pelo processo ETL possui o seguinte esquema:

| Nome da Coluna        | Tipo de Dado | Descrição                                                                 |
|-----------------------|--------------|---------------------------------------------------------------------------|
| cnpj                 | string       | Contém o número de inscrição no CNPJ (Cadastro Nacional da Pessoa Jurídica). |
| qtde_socios          | int          | Número de sócios participantes no CNPJ.                                  |
| flag_socio_estrangeiro | boolean     | **True**: Contém pelo menos 1 sócio estrangeiro.<br>**False**: Não contém sócios estrangeiros. |
| doc_alvo             | boolean      | **True**: Porte da empresa = 03 e qtde_socios > 1.<br>**False**: Outros. |

## Instruções para Execução

### Pré-requisitos

- [Docker](https://www.docker.com/) e [Docker Compose](https://docs.docker.com/compose/) instalados.

### Passos para Executar

1. Clone o repositório:
   ```bash
   git clone https://github.com/hugoas/stone.git
   cd stone
   ```

2. Construa e inicie os containers:
   ```bash
   docker-compose up --build
   ```

3. Acesse o container do serviço `web` para executar o script ETL:
   ```bash
   docker exec -it stone-web-1 bash
   python src/etl.py
   ```

### Configuração do Banco de Dados

O banco de dados PostgreSQL é configurado no serviço `db` do `docker-compose.yml`:
- Host: `db`
- Usuário: `postgres`
- Senha: `has2582`
- Porta: `5432`
- Banco: `postgres`

A variável de ambiente `DATABASE_URL` é usada para conectar ao banco de dados:
```
postgresql://postgres:has2582@db:5432/postgres
```

## Dependências

As dependências do projeto estão listadas no arquivo `requirements.txt`:

```
pandas
psycopg2-binary
tqdm
SQLAlchemy
python-dateutil
```

Instale as dependências usando o comando:
```bash
pip install -r requirements.txt
```

## Explicação do ETL

O projeto adota o modelo **Medalhão** (Bronze, Silver, Gold):

1. **Bronze (Raw):**
   - Dados crus extraídos diretamente do arquivo JSON localizado no diretório `data/`.

2. **Silver (Refinado):**
   - Dados transformados conforme as regras de negócio fornecidas:
     - Determinação de `flag_socio_estrangeiro` com base na lista de sócios.
     - Cálculo do campo `doc_alvo` baseado no porte da empresa e número de sócios.

3. **Gold (Tabelas Finais):**
   - Dados normalizados e prontos para análise, armazenados no banco de dados PostgreSQL.

## Observações

- Certifique-se de que o arquivo JSON esteja disponível no diretório `data/` antes de executar o ETL.
- O schema da tabela será criado automaticamente durante a execução do script ETL caso ainda não exista.

## Contribuição

Sinta-se à vontade para abrir issues ou enviar pull requests para melhorias.

---

Feito por [Hugo](https://github.com/hugoas).

