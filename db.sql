CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE bronze.empresas (
    cnpj VARCHAR NOT NULL,                    -- Cadastro Nacional da Pessoa Jurídica
    razao_social VARCHAR,                     -- Nome empresarial
    natureza_juridica INT,                    -- Código da natureza jurídica
    qualificacao_responsavel INT,             -- Qualificação do responsável
    capital_social NUMERIC,                   -- Capital social da empresa
    cod_porte VARCHAR,                        -- Código do porte da empresa
    data_ingestao TIMESTAMP NOT NULL,         -- Data de ingestão
    PRIMARY KEY (cnpj, data_ingestao)         -- Incluindo coluna de particionamento
) PARTITION BY RANGE (data_ingestao);

-- Comentários para a tabela e colunas
COMMENT ON TABLE bronze.empresas IS 'Tabela bronze para armazenamento inicial dos dados de empresas.';
COMMENT ON COLUMN bronze.empresas.cnpj IS 'Número único de registro da empresa (CNPJ).';
COMMENT ON COLUMN bronze.empresas.razao_social IS 'Razão social ou nome empresarial da empresa.';
COMMENT ON COLUMN bronze.empresas.natureza_juridica IS 'Código que define a natureza jurídica da empresa.';
COMMENT ON COLUMN bronze.empresas.qualificacao_responsavel IS 'Código que define a qualificação do responsável.';
COMMENT ON COLUMN bronze.empresas.capital_social IS 'Capital social declarado pela empresa.';
COMMENT ON COLUMN bronze.empresas.cod_porte IS 'Código que define o porte da empresa.';
COMMENT ON COLUMN bronze.empresas.data_ingestao IS 'Data e hora em que os dados foram ingeridos na camada bronze.';

-- Partição para janeiro de 2025
CREATE TABLE bronze.empresas_202501 PARTITION OF bronze.empresas
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE silver.empresas (
    cnpj VARCHAR NOT NULL,                    -- Cadastro Nacional da Pessoa Jurídica
    razao_social VARCHAR,                     -- Nome empresarial
    natureza_juridica INT,                    -- Código da natureza jurídica
    capital_social NUMERIC,                   -- Capital social da empresa
    porte_descricao VARCHAR,                  -- Descrição do porte
    data_processamento TIMESTAMP NOT NULL,    -- Data de processamento
    PRIMARY KEY (cnpj, data_processamento)    -- Incluindo coluna de particionamento
) PARTITION BY RANGE (data_processamento);

-- Comentários para a tabela e colunas
COMMENT ON TABLE silver.empresas IS 'Tabela silver com dados de empresas tratados e normalizados.';
COMMENT ON COLUMN silver.empresas.cnpj IS 'Número único de registro da empresa (CNPJ).';
COMMENT ON COLUMN silver.empresas.razao_social IS 'Razão social ou nome empresarial da empresa.';
COMMENT ON COLUMN silver.empresas.natureza_juridica IS 'Código que define a natureza jurídica da empresa.';
COMMENT ON COLUMN silver.empresas.capital_social IS 'Capital social declarado pela empresa.';
COMMENT ON COLUMN silver.empresas.porte_descricao IS 'Descrição textual do porte da empresa.';
COMMENT ON COLUMN silver.empresas.data_processamento IS 'Data e hora em que os dados foram processados na camada silver.';


CREATE TABLE bronze.socios (
    cnpj VARCHAR NOT NULL,                         -- CNPJ da empresa associada
    tipo_socio VARCHAR,                                -- Tipo de sócio
    nome_socio VARCHAR,                            -- Nome do sócio
    documento_socio VARCHAR,                       -- CPF ou CNPJ do sócio (pode estar mascarado)
    codigo_qualificacao_socio VARCHAR,                 -- Código da qualificação do sócio
    data_entrada_sociedade DATE,                   -- Data de entrada do sócio na empresa
    faixa_etaria VARCHAR,                              -- Código da faixa etária do sócio
    pais VARCHAR,                                  -- País do sócio
    representante_legal VARCHAR,                   -- Nome do representante legal do sócio
    nome_representante VARCHAR,                    -- Nome do representante legal do sócio (se houver)
    qualificacao_representante VARCHAR,                -- Código da qualificação do representante
    data_ingestao TIMESTAMP NOT NULL DEFAULT NOW(),-- Data da ingestão dos dados
    PRIMARY KEY (cnpj, documento_socio, data_ingestao) -- Chave primária incluindo a partição
) PARTITION BY RANGE (data_ingestao);

-- Comentários para documentação
COMMENT ON TABLE bronze.socios IS 'Tabela bronze para armazenar os dados de sócios das empresas.';
COMMENT ON COLUMN bronze.socios.cnpj IS 'CNPJ da empresa à qual o sócio está vinculado.';
COMMENT ON COLUMN bronze.socios.tipo_socio IS 'Tipo do sócio, indicando seu papel na empresa.';
COMMENT ON COLUMN bronze.socios.nome_socio IS 'Nome ou razão social do sócio.';
COMMENT ON COLUMN bronze.socios.documento_socio IS 'CPF ou CNPJ do sócio (mascarado no arquivo original).';
COMMENT ON COLUMN bronze.socios.codigo_qualificacao_socio IS 'Código de qualificação do sócio.';
COMMENT ON COLUMN bronze.socios.data_entrada_sociedade IS 'Data em que o sócio entrou na empresa.';
COMMENT ON COLUMN bronze.socios.faixa_etaria IS 'Código da faixa etária do sócio.';
COMMENT ON COLUMN bronze.socios.pais IS 'País de origem do sócio.';
COMMENT ON COLUMN bronze.socios.representante_legal IS 'Nome do representante legal do sócio.';
COMMENT ON COLUMN bronze.socios.nome_representante IS 'Nome do representante legal do sócio (se houver).';
COMMENT ON COLUMN bronze.socios.qualificacao_representante IS 'Código da qualificação do representante do sócio.';
COMMENT ON COLUMN bronze.socios.data_ingestao IS 'Data e hora em que os dados foram ingeridos na camada bronze.';

-- Criando a partição de janeiro de 2025
CREATE TABLE bronze.socios_202501 PARTITION OF bronze.socios
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');


CREATE TABLE silver.socios (
    cnpj VARCHAR NOT NULL,                     -- CNPJ da empresa associada
    tipo_socio VARCHAR,                         -- Tipo de sócio
    nome_socio VARCHAR,                         -- Nome do sócio
    documento_socio VARCHAR,                    -- CPF ou CNPJ do sócio
    codigo_qualificacao_socio VARCHAR,          -- Código da qualificação do sócio
    data_entrada_sociedade DATE,                -- Data de entrada do sócio na empresa
    faixa_etaria VARCHAR,                       -- Código da faixa etária do sócio
    pais VARCHAR,                               -- País do sócio
    representante_legal VARCHAR,                -- Nome do representante legal do sócio
    nome_representante VARCHAR,                 -- Nome do representante legal do sócio
    qualificacao_representante VARCHAR,         -- Código da qualificação do representante
    data_processamento TIMESTAMP NOT NULL,      -- Data de processamento na camada Silver
    PRIMARY KEY (cnpj, documento_socio, data_processamento)
) PARTITION BY RANGE (data_processamento);

-- Comentários para documentação
COMMENT ON TABLE silver.socios IS 'Tabela Silver para armazenar dados tratados de sócios das empresas.';
COMMENT ON COLUMN silver.socios.cnpj IS 'CNPJ da empresa à qual o sócio está vinculado.';
COMMENT ON COLUMN silver.socios.tipo_socio IS 'Tipo do sócio, indicando seu papel na empresa.';
COMMENT ON COLUMN silver.socios.nome_socio IS 'Nome ou razão social do sócio.';
COMMENT ON COLUMN silver.socios.documento_socio IS 'CPF ou CNPJ do sócio (normalizado e tratado).';
COMMENT ON COLUMN silver.socios.codigo_qualificacao_socio IS 'Código de qualificação do sócio.';
COMMENT ON COLUMN silver.socios.data_entrada_sociedade IS 'Data em que o sócio entrou na empresa.';
COMMENT ON COLUMN silver.socios.faixa_etaria IS 'Código da faixa etária do sócio.';
COMMENT ON COLUMN silver.socios.pais IS 'País de origem do sócio.';
COMMENT ON COLUMN silver.socios.representante_legal IS 'Nome do representante legal do sócio.';
COMMENT ON COLUMN silver.socios.nome_representante IS 'Nome do representante legal do sócio (se houver).';
COMMENT ON COLUMN silver.socios.qualificacao_representante IS 'Código da qualificação do representante do sócio.';
COMMENT ON COLUMN silver.socios.data_processamento IS 'Data e hora em que os dados foram processados na camada Silver.';

-- Criando uma partição para Janeiro de 2025
CREATE TABLE silver.socios_202501 PARTITION OF silver.socios
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE gold.empresas (
    cnpj VARCHAR NOT NULL,                    -- Cadastro Nacional da Pessoa Jurídica
    razao_social VARCHAR,                     -- Nome empresarial
    capital_social NUMERIC,                   -- Capital social da empresa
    total_socios INT,                         -- Número total de sócios
    flag_socio_estrangeiro BOOLEAN,           -- Indica se há sócio estrangeiro
    data_analise TIMESTAMP NOT NULL,          -- Data de análise
    PRIMARY KEY (cnpj, data_analise)          -- Incluindo coluna de particionamento
) PARTITION BY RANGE (data_analise);

-- Comentários para a tabela e colunas
COMMENT ON TABLE gold.empresas IS 'Tabela gold consolidada para análise de dados de empresas.';
COMMENT ON COLUMN gold.empresas.cnpj IS 'Número único de registro da empresa (CNPJ).';
COMMENT ON COLUMN gold.empresas.razao_social IS 'Razão social ou nome empresarial da empresa.';
COMMENT ON COLUMN gold.empresas.capital_social IS 'Capital social declarado pela empresa.';
COMMENT ON COLUMN gold.empresas.total_socios IS 'Número total de sócios associados à empresa.';
COMMENT ON COLUMN gold.empresas.flag_socio_estrangeiro IS 'True se houver sócio estrangeiro, False caso contrário.';
COMMENT ON COLUMN gold.empresas.data_analise IS 'Data e hora em que os dados foram consolidados para análise.';

-- Criando a partição para Janeiro de 2025
CREATE TABLE gold.empresas_202501 PARTITION OF gold.empresas
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');




