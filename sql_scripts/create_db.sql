CREATE TABLE IF NOT EXISTS financial_inst_pix (
    ispb VARCHAR(8) PRIMARY KEY,
    nome_juridico VARCHAR(255),
    nome_busca VARCHAR(255),
    categoria VARCHAR(100),
    pais_sede VARCHAR(30),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);   	
