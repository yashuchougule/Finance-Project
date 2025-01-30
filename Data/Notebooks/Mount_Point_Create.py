# Databricks notebook source
spark

# COMMAND ----------

dbutils.fs.unmount("/mnt/raw")

# COMMAND ----------

dbutils.fs.unmount("/mnt/silver")

# COMMAND ----------

dbutils.fs.unmount("/mnt/gold")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="Small-Bank-Project-Scope", key="AppID"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="Small-Bank-Project-Scope", key="Secret"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='Small-Bank-Project-Scope', key='TenantID')}/oauth2/token"}

dbutils.fs.mount(
source = "abfss://raw@smallbankstorage1.dfs.core.windows.net/",
mount_point = "/mnt/raw",
extra_configs = configs)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="Small-Bank-Project-Scope", key="AppID"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="Small-Bank-Project-Scope", key="Secret"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='Small-Bank-Project-Scope', key='TenantID')}/oauth2/token"}

dbutils.fs.mount(
source = "abfss://silver@smallbankstorage1.dfs.core.windows.net/",
mount_point = "/mnt/silver",
extra_configs = configs)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="Small-Bank-Project-Scope", key="AppID"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="Small-Bank-Project-Scope", key="Secret"),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='Small-Bank-Project-Scope', key='TenantID')}/oauth2/token"}

dbutils.fs.mount(
source = "abfss://gold@smallbankstorage1.dfs.core.windows.net/",
mount_point = "/mnt/gold",
extra_configs = configs)