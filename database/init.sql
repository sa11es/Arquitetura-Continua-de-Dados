CREATE TABLE notificacoes_dengue (
    id_notificacao SERIAL PRIMARY KEY,
    dt_notificacao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dt_sintomas DATE NOT NULL,
    municipio_residencia VARCHAR(100) NOT NULL,
    codigo_ibge VARCHAR(7) NOT NULL,
    bairro VARCHAR(100) NOT NULL,
    doenca VARCHAR(20) NOT NULL CHECK (
        doenca IN ('POPULAR', 'DENGUE', 'MENINGITE', 'INFLUENZA')
    ),
    cid_10 VARCHAR(10) NOT NULL,
    idade INT NOT NULL,
    sexo CHAR(1) CHECK (sexo IN ('M', 'F', 'I')),
    status_caso VARCHAR(20) DEFAULT 'SUSPEITO',
    data_atualizacao TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION atualiza_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.data_atualizacao = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_atualiza_notificacao
BEFORE UPDATE ON notificacoes_dengue
FOR EACH ROW
EXECUTE FUNCTION atualiza_timestamp();