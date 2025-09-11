# Databricks notebook source
var = dbutils.jobs.taskValues.get(
    taskKey="Weekday_lookup",
    key="weekoutput",
    debugValue="default_value"
)

# COMMAND ----------

print(var)