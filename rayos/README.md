# Carga y graficación de datos de NLDN

Los datos vienen en texto plano en archivos gzippeados con nombres parecidos a
`NLDN_flash_Tiles5-6_2009.gz`.

## Cargando los datos

El archivo `create_parquet.py` es un script de utilería que carga uno o mas de
los archivos de datos y los convierte al formato de `Apache Parquet`, que
permite cargar los archivos de forma mas eficiente. un ejemplo de como usarlo
es:

```
python create_parquet.py NLDN_flash_Tiles5-6_2009.gz
```

## Graficando los datos

El archivo `rayos.py` define como graficar los datos puntuales en el formato de
Parquet. Define un documento de bokeh. Para servir la visualización se usa el
comando `bokeh serve rayos.py`.
