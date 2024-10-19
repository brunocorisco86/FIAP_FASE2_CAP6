-- Criar a tabela
CREATE TABLE colheita_cana (
    id NUMBER PRIMARY KEY,
    area NUMBER,                    -- Unidade de medida: hectares
    velocidade NUMBER,              -- Unidade de medida: km/h
    rotacao NUMBER,                 -- Unidade de medida: RPM
    horas_sem_afiacao NUMBER,       -- Unidade de medida: horas
    colheitadeira VARCHAR2(1),
    largura_maquina NUMBER,         -- Unidade de medida: metros
    perdas_estimadas NUMBER,        -- Unidade de medida: toneladas
    tempo_estimado NUMBER,          -- Unidade de medida: horas
    consumo_diesel NUMBER,          -- Unidade de medida: litros
    emissao_co2 NUMBER,             -- Unidade de medida: kg
    custo_diesel FLOAT,             -- custo em reais
    data_inicio DATE,
    data_fim DATE
);