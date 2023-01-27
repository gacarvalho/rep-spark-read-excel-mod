from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import openpyxl

'''
Configuração células Jupyter Notebook

from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))
'''

# Configuração do Spark
conf = SparkConf().setAppName("MeuApp") \
                  .setMaster("local[*]") \
                  .set("spark.executor.memory", "4g") \
                  .set("spark.driver.memory", "2g") \
                  .set("spark.driver.maxResultSize", "1g")

# Criação do contexto Spark
sc = SparkContext.getOrCreate(conf=conf)

# Inicialização do SparkSession
spark = SparkSession(sc)


def read_excel(spark, path):
    try:
        # Leitura do arquivo Excel usando openpyxl
        workbook = openpyxl.load_workbook(path)
        sheet = workbook.active
        rows = sheet.iter_rows()
        headers = [cell.value for cell in next(rows)]
        data = [[cell.value for cell in row] for row in rows]

        # Criação do DataFrame
        df = spark.createDataFrame(data, headers)
        return df
    except Exception as e:
        print("Erro ao ler o Excel: ", e)

path = "/home/gabriel/Downloads/CARS_Excel.xlsx"
df = read_excel(spark,path)
print(' **** Leitura completa dos dados **** ')

def treat_data(df):
    try:
        # Renomeando as colunas
        df = df.withColumnRenamed("Make", "brand") \
                .withColumnRenamed("Model", "model") \
                .withColumnRenamed("Type", "type") \
                .withColumnRenamed("Origin", "origin_country") \
                .withColumnRenamed("DriveTrain", "drive_type")    
          
        # Removendo linhas duplicadas
        df = df.dropDuplicates()
        
        # Removendo linhas com valores nulos
        df = df.dropna()
        
        return df
    except Exception as e:
        print("Erro ao realizar tratamento de dados: ", e)

df = treat_data(df)
print(' **** Tratamento dos dados completa **** ')


def data_quality(df):
    try:
        df.createOrReplaceTempView("data_table")

        spark.sql("SELECT * FROM data_table").show()

        # Verificando valores nulos
        null_counts = spark.sql("""
            SELECT 
            COUNT(CASE WHEN brand IS NULL THEN 1 END) as make_nulls,
            COUNT(CASE WHEN model IS NULL THEN 1 END) as model_nulls,
            COUNT(CASE WHEN type IS NULL THEN 1 END) as type_nulls,
            COUNT(CASE WHEN origin_country IS NULL THEN 1 END) as origin_nulls,
            COUNT(CASE WHEN drive_type IS NULL THEN 1 END) as drivetrain_nulls
            FROM data_table
        """).collect()[0].asDict()
        print("Número de nulls em cada coluna: ", null_counts)

        # Realizando check em linhas duplicadas
        duplicate_count = df.groupBy("brand", "model", "type", "origin_country", "drive_type").count()\
                        .filter("count > 1").count()
        print("Número de linhas duplicadas: ", duplicate_count)


        # Verificando valores unicos
        unique_counts = spark.sql("""
            SELECT 
            COUNT(DISTINCT brand) as make_unique,
            COUNT(DISTINCT model) as model_unique,
            COUNT(DISTINCT type) as type_unique,
            COUNT(DISTINCT origin_country) as origin_unique,
            COUNT(DISTINCT drive_type) as drivetrain_unique
            FROM data_table
        """).collect()[0].asDict()
        print("Numero de valores unicos em cada coluna: ", unique_counts)

        # Verificando intervalo de dados para coluna INT
        data_range = spark.sql("""
            SELECT 
            MIN(drive_type) as drivetrain_min,
            MAX(drive_type) as drivetrain_max
            FROM data_table
        """).collect()[0].asDict()
        print("Intervalo de dados da coluna drivetrain (coluna do tipo int) : ", data_range)

    except Exception as e:
        print("Erro grave de aplicação de qualidade de dados ", e)

print(' **** Qualidade dos dados completa **** ')
data_quality(df)
df.show(truncate=False)

