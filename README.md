```
scala> val noticiasCorp = noticias.withColumn("corpora", concat(col("titulo"), lit("\n"), col("descricao"), lit("\n"), col("conteudo")))
noticiasCorp: org.apache.spark.sql.DataFrame = [categorias: array<string>, conteudo: string ... 6 more fields]
```
```
scala> val corpora = noticiasCorp.select("corpora","dominio","categorias","data")
corpora: org.apache.spark.sql.DataFrame = [corpora: string, dominio: string ... 2 more fields]
```
```
scala> corpora.write.partitionBy("data", "dominio").format("json").save("corpora")
```
```
scala> val corporaPorData = corpora.groupBy("data").count()
corporaPorData: org.apache.spark.sql.DataFrame = [data: date, count: bigint]
```
```
scala> corporaPorData.show
+----------+-----+
|      data|count|
+----------+-----+
|2019-11-18|  156|
|2020-01-21|  110|
|2019-11-01|  134|
|2019-09-22|  139|
|2019-11-21|  138|
|2020-04-30|  112|
|2018-08-08|    1|
|2019-05-27|    1|
|2020-03-07|   94|
|2020-03-13|  111|
|2020-02-04|  108|
|2020-02-15|  108|
|2020-05-23|  117|
|2018-05-26|    1|
|2019-10-05|  135|
|2020-02-12|  114|
|2018-04-18|    2|
|2019-10-24|  154|
|2019-07-08|  122|
|2019-05-14|    2|
+----------+-----+
only showing top 20 rows
```
```
scala> val corporaPorDom = corpora.groupBy("dominio").count()
corporaPorDom: org.apache.spark.sql.DataFrame = [dominio: string, count: bigint]
```
```
scala> corporaPorDom.show
+--------------------+-----+
|             dominio|count|
+--------------------+-----+
|   brasil.elpais.com|   60|
|feeds.folha.uol.c...| 5678|
|       pox.globo.com| 4618|
|  esporte.uol.com.br|12543|
|      rss.uol.com.br| 3676|
| rss.home.uol.com.br| 3423|
+--------------------+-----+
```

