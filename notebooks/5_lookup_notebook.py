# Databricks notebook source
dbutils.widgets.text("weekday","7")

# COMMAND ----------

var = int(dbutils.widgets.get("weekday"))

# COMMAND ----------

dbutils.jobs.taskValues.set(key="weekoutput", value=var)

# COMMAND ----------

var = int(dbutils.widgets.get("weekday"))

# Save as task value for downstream tasks
dbutils.jobs.taskValues.set(key="weekoutput", value=str(var))


# COMMAND ----------

weekday_val = dbutils.jobs.taskValues.get(
    taskKey="Weekday_lookup", 
    key="weekoutput", 
    debugValue="7"   # fallback if upstream didn't run
)
print(weekday_val)