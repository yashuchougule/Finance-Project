# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DB Utilities Commands**

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.widgets() commands =**

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.widgets.text**

# COMMAND ----------

dbutils.widgets.text("P_name", "Yash")

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.widgets.get**

# COMMAND ----------

var = dbutils.widgets.get("P_name")
var

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.secrets**

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.secrets.list**

# COMMAND ----------

dbutils.secrets.list(scope='Small-Bank-Project-Scope')

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.secrets.get**

# COMMAND ----------

dbutils.secrets.get(scope='Small-Bank-Project-Scope', key='AppID')