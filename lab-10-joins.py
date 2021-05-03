########################################################################################################
# Title               : Lab-10 (Joins)
# General instruction : Copy-paste below code and run it in PySpark Interative CLI
########################################################################################################


# ---------- STEP 0 ----------
# Create DataFrames to join
# -----------------------------

# person table
person = spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Matei Zaharia", 1, [500, 250, 100]),
    (2, "Michael Armbrust", 1, [250, 100])])\
  .toDF("id", "name", "graduate_program", "spark_status")

person.show()

# graduate program table
graduateProgram = spark.createDataFrame([
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")])\
  .toDF("id", "degree", "department", "school")

graduateProgram.show()

# status table
sparkStatus = spark.createDataFrame([
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")])\
  .toDF("id", "status")

sparkStatus.show()


# ---------- STEP 1 ----------
# Inner joins 
# keep rows with keys that exist in the left and right datasets
# -----------------------------

person.join(
    graduateProgram, 
    person["graduate_program"] == graduateProgram['id']).show()
# -- in SQL
# SELECT * FROM person JOIN graduateProgram
# ON person.graduate_program = graduateProgram.id


# ---------- STEP 2 ----------
# Outer joins 
# keep rows with keys in either the left or right datasets
# -----------------------------

person.join(
    graduateProgram, 
    person["graduate_program"] == graduateProgram['id'], 
    "outer").show()
# -- in SQL
# SELECT * FROM person FULL OUTER JOIN graduateProgram ON graduate_program = graduateProgram.id


# ---------- STEP 3 ----------
# Left outer joins 
# keep rows with keys in the left dataset
# -----------------------------

graduateProgram.join(
    person, 
    person["graduate_program"] == graduateProgram['id'], 
    "left_outer").show()
# -- in SQL
# SELECT * FROM graduateProgram LEFT OUTER JOIN person ON person.graduate_program = graduateProgram.id

 
# ---------- STEP 4 ----------
# Right outer joins 
# keep rows with keys in the right dataset
# -----------------------------

person.join(
    graduateProgram, 
    person["graduate_program"] == graduateProgram['id'], 
    "right_outer").show()
# -- in SQL
# SELECT * FROM person RIGHT OUTER JOIN graduateProgram ON person.graduate_program = graduateProgram.id


# ---------- STEP 5 ----------
# Left semi joins 
# keep the rows in the left, and only the left, dataset where the key appears in the right dataset
# -----------------------------

graduateProgram.join(
    person, 
    person["graduate_program"] == graduateProgram['id'], 
    "left_semi").show()


# ---------- STEP 6 ----------
# Left anti joins 
# keep the rows in the left, and only the left, dataset where they do not appear in the right dataset
# -----------------------------

graduateProgram.join(
    person, 
    person["graduate_program"] == graduateProgram['id'], 
    "left_anti").show()


# ---------- STEP 7 ----------
# Cross (or Cartesian) joins 
# match every row in the left dataset with every row in the right dataset
# -----------------------------

graduateProgram.join(
    person, 
    person["graduate_program"] == graduateProgram['id'], 
    "cross").show()

