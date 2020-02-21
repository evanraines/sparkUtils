from pyspark.sql import SparkSession


class SparkWrapper(object):
    def __init__(self):
        self._spark = None

    def __del__(self):
        self._spark.stop()

    def __enter__(self):
        self._spark = SparkSession\
                        .builder\
                        .appName("Standard Module")\
                        .enableHiveSupport()\
                        .config("hive.exec.dynamic.partition", "true")\
                        .config("hive.exec.dynamic.partition.mode",
                                "nonstrict")\
                        .getOrCreate()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._spark.stop()

    @property
    def spark(self):
        return self._spark


def create_temp_table(df, client, tablename):
    df.write.format('orc')\
      .saveAsTable('{client}_prod.{tablename}'
                   .format(client=client, tablename=tablename),
                   mode='overwrite')
    return 1


def drop_temp_table(spark, client, tablename):
    query = """DROP TABLE IF EXISTS
               {client}_prod.{tablename}"""
    spark.sql(query.format(client=client, tablename=tablename))

    return 1


def save_table(df, client, table, mode='append'):
    df.write.jdbc(url="jdbc:mysql://tableau.db.int.mathereconomics.com:3306/tableau_{client}_01"
                  .format(client=client), table=table, mode=mode,
                  properties={'driver': "com.mysql.jdbc.Driver",
                              'user': "tableau",
                              'password': "JLVSc49yBRNpPsZ9",
                              'batchsize': "50000"})
    return 1


def read_mySQL_table(spark, client, table):
    df = spark.read\
        .format("jdbc") \
        .options(
                 url="jdbc:mysql://tableau.db.int.mathereconomics.com:3306/tableau_{client}_01"
                 .format(client=client),
                 driver="com.mysql.jdbc.Driver", dbtable=table,
                 user="tableau", password="JLVSc49yBRNpPsZ9")\
        .load()
    return df
