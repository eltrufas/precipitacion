import pandas as pd
import numpy as np
import gzip
import dask.dataframe as dd
import io
import logging
from datetime import datetime
import geoviews as gv
import geoviews.feature as gf
from geoviews import opts
from cartopy import crs
import datashader as ds
from holoviews.operation.datashader import datashade, shade, dynspread, rasterize
import holoviews as hv

hv.extension('bokeh', 'matplotlib')

logging.getLogger().setLevel(logging.INFO)

df = dd.read_parquet('NLDN_flash_Tiles5-6_2009.gz-parquet')

logging.info("Created DataFrame")

renderer = hv.renderer('bokeh')

dia = df.loc['2009-01-01 00:00:00':'2009-01-10 00:00:00']

rayos_lonlat = gv.Points(df[['latitud', 'longitud', 'multiplicidad']], vdims='multiplicidad')
layout = (gv.tile_sources.EsriImagery * datashade(rayos_lonlat)).opts(
            width=800, height=600)

        
logging.info("Rendered layout") 

doc = renderer.server_doc(layout)
doc.title = 'HoloViews App'
logging.info("set document")
