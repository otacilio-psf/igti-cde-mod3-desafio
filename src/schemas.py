from pyspark.sql.types import *

schema_cnae = StructType([
    StructField('cod_cnae', IntegerType()),
    StructField('cnae_desc', StringType())
])

schema_muni = StructType([
    StructField('cod_muni', IntegerType()),
    StructField('municipio_desc', StringType())
])

schema_estab = StructType([
    StructField('cnpj_basico', StringType()),
    StructField('cnpj_ordem', StringType()),
    StructField('cnpj_dv', StringType()),
    StructField('identificador', StringType()),
    StructField('nome_fantasia', StringType()),
    StructField('situacao_cadastral', IntegerType()),
    StructField('data_situacao_cadastral', StringType()),
    StructField('motivo_situacao_cadastral', IntegerType()),
    StructField('nome_cidade_exterior', StringType()),
    StructField('pais', StringType()),
    StructField('data_inicio_atividade', StringType()),
    StructField('cnae_fiscal_principal', IntegerType()),
    StructField('cnae_fiscal_secundario', StringType()),
    StructField('tipo_logadouro', StringType()),
    StructField('logadouro', StringType()),
    StructField('numero', StringType()),
    StructField('complemento', StringType()),
    StructField('bairro', StringType()),
    StructField('cep', IntegerType()),
    StructField('uf', StringType()),
    StructField('municipio', IntegerType()),
    StructField('ddd_1', IntegerType()),
    StructField('tel_1', IntegerType()),
    StructField('ddd_2', IntegerType()),
    StructField('tel_2', IntegerType()),
    StructField('ddd_fax', IntegerType()),
    StructField('fax', IntegerType()),
    StructField('email', StringType()),
    StructField('situacao_especial', StringType()),
    StructField('data_situacao_especial', StringType())
])