from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.schemas import schema_cnae, schema_muni, schema_estab
from src.aux import read_csv, read_parquet, write_parquet

print("=========================== Spark Session ============================")
spark = SparkSession.builder.appName("desafio-de-igti").getOrCreate()
spark.conf.set('spark.sql.legacy.parquet.datetimeRebaseModeInWrite', 'CORRECTED')


class ELT():

    def extract_raw(self):
        print("Extract")
        path_cnae = r"gs://desafio-final/F.K03200$Z.D10710.CNAE.csv"
        self.df_cnae_raw = read_csv(spark, path_cnae, schema_cnae)

        path_muni = r"gs://desafio-final/F.K03200$Z.D10710.MUNIC.csv"
        self.df_muni_raw = read_csv(spark, path_muni, schema_muni)

        path_estab = "gs://desafio-final/estabelecimentos"
        self.df_estab_raw = read_csv(spark, path_estab, schema_estab)

    def load_trusted(self):
        print("Load")
        write_parquet(self.df_cnae_raw, 'gs://igti-bootcamp-otacilio/trusted/cnae')
        write_parquet(self.df_muni_raw, 'gs://igti-bootcamp-otacilio/trusted/municipio')
        write_parquet(self.df_estab_raw, 'gs://igti-bootcamp-otacilio/trusted/estabelecimento')

    def transform_refined(self):
        print("Transform")
        df_cnae_trusted = read_parquet(spark, 'gs://igti-bootcamp-otacilio/trusted/cnae')
        df_muni_trusted = read_parquet(spark, 'gs://igti-bootcamp-otacilio/trusted/municipio')
        df_estab_trusted = read_parquet(spark, 'gs://igti-bootcamp-otacilio/trusted/estabelecimento')

        date_column_list = ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial']
        
        df_situacao_cadastral_map = spark.createDataFrame([(1, 'NULA'), (2, 'ATIVA'), (3, 'SUSPENSA'), (4, 'INAPTA'), (8, 'BAIXADA')], 'situacao_cadastral int, situacao_cadastral_desc string')
        
        df_estab_trusted.printSchema()

        for col_ in date_column_list: # Convert string to date type
            df_estab_trusted = df_estab_trusted.withColumn(col_, to_date(col(col_), 'yyyyMMdd'))

        df_estab_trusted = (
            df_estab_trusted
            .withColumn('identificador_desc', when(col('identificador')==1, lit('MATRIZ')) # map identificador
                                             .when(col('identificador')==2, lit('FILIAL')))
            .join(broadcast(df_situacao_cadastral_map), 'situacao_cadastral', 'left') # map situacao_cadastral
            .join(broadcast(df_cnae_trusted), col('cnae_fiscal_principal')==col('cod_cnae'), 'left').drop(col('cod_cnae')) # map cnae
            .join(broadcast(df_muni_trusted), col('municipio')==col('cod_muni'), 'left').drop(col('cod_muni')) # map municipio
        )

        # Sink data in renined
        write_parquet(df_estab_trusted, 'gs://igti-bootcamp-otacilio/refined/estabelecimento')

    def process(self):
        self.extract_raw()
        self.load_trusted()
        self.transform_refined()

etl = ELT()
etl.process()

# Stop application
spark.stop()