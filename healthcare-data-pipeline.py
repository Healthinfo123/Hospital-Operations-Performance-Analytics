import os
import pandas as pd
import numpy as np

# ---------------------------
# Load datasets (safe paths)
# ---------------------------
patients = pd.read_csv("data/patient_admissions.csv")
diagnoses = pd.read_csv("data/diagnoses.csv")
docs = pd.read_csv("data/documentation_logs.csv")
meds = pd.read_csv("data/medications.csv")

# Dataset shapes
print("Patients:", patients.shape)
print("Diagnoses:", diagnoses.shape)
print("Documentation logs:", docs.shape)
print("Medications:", meds.shape)

# Info + Missing values
patients.info()
patients.isnull().sum()

# Remove duplicates
patients = patients.drop_duplicates()

# Convert dates
patients['admission_date'] = pd.to_datetime(patients['admission_date'])
patients['discharge_date'] = pd.to_datetime(patients['discharge_date'])

# Calculate Length of Stay (LOS)
patients['LOS'] = (patients['discharge_date'] - patients['admission_date']).dt.days
patients['LOS'] = patients['LOS'].fillna(0)

# Handle negative LOS
patients.loc[patients['LOS'] < -1, 'LOS'] = np.nan
patients = patients.reset_index(drop=True)

# Print column names
print("PATIENTS:", patients.columns.tolist())
print("DIAGNOSES:", diagnoses.columns.tolist())
print("DOCS:", docs.columns.tolist())
print("MEDS:", meds.columns.tolist())

# ---------------------------
# Merge datasets
# ---------------------------

# 1. Patients + Diagnoses
patients_dx = patients.merge(
    diagnoses,
    left_on="primary_diagnosis_code",
    right_on="diagnosis_code",
    how="left"
)

# 2. Add documentation logs
patients_dx_docs = patients_dx.merge(
    docs,
    on="encounter_id",
    how="left"
)

# 3. Add medications
final_df = patients_dx_docs.merge(
    meds,
    on="encounter_id",
    how="left"
)

# Basic analysis
print(final_df['LOS'].describe())
print(final_df.groupby("diagnosis_category")['LOS'].mean())
print(final_df.groupby("discharge_summary_delay_hours")['LOS'].mean())
print(final_df.groupby("therapeutic_class")['LOS'].mean())

print(final_df[['missing_progress_note','missing_discharge_summary']].sum())

# ---------------------------
# Additional grouped analysis
# ---------------------------
los_summary = final_df['LOS'].describe()
print(los_summary)

dx_los = final_df.groupby('diagnosis_category')['LOS'].agg(['mean','median','count']).sort_values('mean', ascending=False)
print(dx_los)

final_df['delay_bucket'] = pd.cut(final_df['discharge_summary_delay_hours'],
                                  bins=[-1, 0, 24, 72, 168, 10000],
                                  labels=['No delay','0-24h','24-72h','3-7d','>7d'])

delay_los = final_df.groupby('delay_bucket')['LOS'].agg(['mean','median','count'])
print(delay_los)

ther_los = final_df.groupby('therapeutic_class')['LOS'].agg(['mean','median','count']).sort_values('mean', ascending=False)
print(ther_los)

missing_counts = final_df[['missing_progress_note','missing_discharge_summary']].sum()
missing_percent = (missing_counts / final_df['encounter_id'].nunique()) * 100
print(missing_counts)
print(missing_percent)

# ---------------------------
# SQL CONNECTION (Safe via environment variables)
# ---------------------------

from sqlalchemy import create_engine
import urllib.parse

username = os.getenv("DB_USER")
raw_password = os.getenv("DB_PASS")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")

password = urllib.parse.quote(raw_password)

engine = create_engine(
    f"postgresql://{username}:{password}@{host}:{port}/{database}"
)

# Test connection
try:
    with engine.connect() as conn:
        print("Connected successfully!")
except Exception as e:
        print("Connection failed:", e)

# ---------------------------
# Write DB tables (safe)
# ---------------------------
patients.to_sql("patients", engine, if_exists="replace", index=False)
diagnoses.to_sql("diagnoses", engine, if_exists="replace", index=False)
docs.to_sql("documentation_log", engine, if_exists="replace", index=False)
meds.to_sql("medications", engine, if_exists="replace", index=False)

# ---------------------------
# Create final merged dataset for export
# ---------------------------
df = patients.merge(diagnoses, left_on="primary_diagnosis_code", right_on="diagnosis_code", how="left")
df = df.merge(docs, on="encounter_id", how="left")

med_counts = meds.groupby("encounter_id").size().reset_index(name="med_count")
df = df.merge(med_counts, on="encounter_id", how="left")
df["med_count"] = df["med_count"].fillna(0)

df.replace("?", None, inplace=True)
df.fillna(0, inplace=True)

# Save output inside project folder
df.to_csv("output/final_hospital_dataset.csv", index=False)
