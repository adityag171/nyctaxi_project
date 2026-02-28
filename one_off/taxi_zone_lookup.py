# Databricks notebook source
import os
import shutil
import urllib.request

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

dir_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup"

response = urllib.request.urlopen(url)

os.makedirs(dir_path, exist_ok=True)

local_path = dir_path + "/taxi_zone_lookup.csv"

with open(local_path, "wb") as f:
    shutil.copyfileobj(response, f)