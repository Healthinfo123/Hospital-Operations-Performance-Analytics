#!/usr/bin/env python
# coding: utf-8

# In[3]:


import os
os.getcwd()


# In[7]:


patients = pd.read_csv("/Users/naredlasiri/Desktop/patient_admissions.csv")
diagnoses = pd.read_csv("/Users/naredlasiri/Desktop/diagnoses.csv")
docs = pd.read_csv("/Users/naredlasiri/Desktop/documentation_logs.csv")
meds = pd.read_csv("/Users/naredlasiri/Desktop/medications.csv")

patients.head()


# In[9]:


print("Patients:", patients.shape)
print("Diagnoses:", diagnoses.shape)
print("Documentation logs:", docs.shape)
print("Medications:", meds.shape)


# In[11]:


patients.info()


# In[13]:


patients.isnull().sum()


# In[15]:


patients.head()
diagnoses.head()
docs.head()
meds.head()


# In[17]:


patients = patients.drop_duplicates()


# In[19]:


patients['admission_date'] = pd.to_datetime(patients['admission_date'])
patients['discharge_date'] = pd.to_datetime(patients['discharge_date'])


# In[21]:


patients['LOS'] = (patients['discharge_date'] - patients['admission_date']).dt.days


# In[25]:


patients['LOS'] = patients['LOS'].fillna(0)  


# In[29]:


import numpy as np
patients.loc[patients['LOS'] < -1, 'LOS'] = np.nan


# In[31]:


patients = patients.reset_index(drop=True)


# In[33]:


patients.head()


# In[43]:


print("PATIENTS:", patients.columns.tolist())
print("DIAGNOSES:", diagnoses.columns.tolist())
print("DOCS:", docs.columns.tolist())
print("MEDS:", meds.columns.tolist())


# In[45]:


# Merge 1: Patients + Diagnoses
patients_dx = patients.merge(
    diagnoses,
    left_on="primary_diagnosis_code",
    right_on="diagnosis_code",
    how="left"
)

# Merge 2: Add documentation quality
patients_dx_docs = patients_dx.merge(
    docs,
    on="encounter_id",
    how="left"
)

# Merge 3: Add medications
final_df = patients_dx_docs.merge(
    meds,
    on="encounter_id",
    how="left"
)

final_df.head()


# In[49]:


final_df['LOS'].describe()


# In[51]:


final_df.groupby("diagnosis_category")['LOS'].mean()


# In[53]:


final_df.groupby("discharge_summary_delay_hours")['LOS'].mean()


# In[55]:


final_df.groupby("therapeutic_class")['LOS'].mean()


# In[57]:


final_df[['missing_progress_note','missing_discharge_summary']].sum()


# In[59]:


# 1. LOS distribution summary
los_summary = final_df['LOS'].describe()
print(los_summary)

# 2. LOS by diagnosis category (mean, median, count)
dx_los = final_df.groupby('diagnosis_category')['LOS'].agg(['mean','median','count']).sort_values('mean', ascending=False)
print(dx_los)

# 3. Documentation delay impact: bucket delays and compare LOS
final_df['delay_bucket'] = pd.cut(final_df['discharge_summary_delay_hours'],
                                  bins=[-1,0,24,72,168,10000],
                                  labels=['No delay','0-24h','24-72h','3-7d','>7d'])
delay_los = final_df.groupby('delay_bucket')['LOS'].agg(['mean','median','count'])
print(delay_los)

# 4. LOS by therapeutic class
ther_los = final_df.groupby('therapeutic_class')['LOS'].agg(['mean','median','count']).sort_values('mean', ascending=False)
print(ther_los)

# 5. Missing documentation counts and percent
missing_counts = final_df[['missing_progress_note','missing_discharge_summary']].sum()
missing_percent = (missing_counts / final_df['encounter_id'].nunique()) * 100
print(missing_counts)
print(missing_percent)


# In[61]:


get_ipython().system('pip install psycopg2-binary sqlalchemy')


# In[79]:


import urllib.parse
password = urllib.parse.quote("Siri@123")   # Encodes the '@'
password


# In[81]:


from sqlalchemy import create_engine
import pandas as pd

username = "postgresql18"        
password = "Siri@123"
host = "localhost"
port = "5433"
database = "hospital_db"

engine = create_engine(
    f"postgresql://{username}:{password}@{host}:{port}/{database}"
)


# In[87]:


urllib.parse.quote("Siri@123")


# In[89]:


print(f"postgresql://{username}:{password}@{host}:{port}/{database}")


# In[93]:


from sqlalchemy import create_engine
import urllib.parse

username = "postgresql18"
raw_password = "Siri@123"

# Encode password safely
password = urllib.parse.quote(raw_password)

host = "localhost"
port = "5433"
database = "hospital_db"

# Build engine correctly
engine = create_engine(
    f"postgresql://{username}:{password}@{host}:{port}/{database}"
)

print("Connection URL:", f"postgresql://{username}:{password}@{host}:{port}/{database}")


# In[95]:


import urllib.parse
urllib.parse.quote("Siri@123")


# In[101]:


port = 5432


# In[103]:


port = 5433


# In[105]:


from sqlalchemy import create_engine
import urllib.parse

username = "postgresql18"
password = urllib.parse.quote("Siri@123")
host = "localhost"
port = "5433"      # change to 5432 if needed
database = "hospital_db"

engine = create_engine(
    f"postgresql://{username}:{password}@{host}:{port}/{database}"
)


# In[107]:


try:
    with engine.connect() as conn:
        print("Connected successfully!")
except Exception as e:
    print("Connection failed:", e)


# In[109]:


username = "postgres"
password = urllib.parse.quote("Siri@123")
host = "localhost"
port = "5433"
database = "hospital_db"

engine = create_engine(
    f"postgresql://{username}:{password}@{host}:{port}/{database}"
)


# In[113]:


patients.to_sql("patients", engine, if_exists="replace", index=False)
diagnoses.to_sql("diagnoses", engine, if_exists="replace", index=False)
docs.to_sql("documentation_log", engine, if_exists="replace", index=False)
meds.to_sql("medications", engine, if_exists="replace", index=False)


# In[115]:


import pandas as pd

# List all tables in the database
pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema='public';", engine)


# In[117]:


pd.read_sql("SELECT * FROM patients LIMIT 5;", engine)


# In[119]:


pd.read_sql("SELECT * FROM diagnoses LIMIT 5;", engine)


# In[121]:


pd.read_sql("SELECT * FROM documentation_log LIMIT 5;", engine)


# In[123]:


pd.read_sql("SELECT * FROM medications LIMIT 5;", engine)


# In[129]:


import pandas as pd

# ---------------------------
# 1. Merge patients + diagnoses
# ---------------------------
df = patients.merge(
    diagnoses,
    left_on="primary_diagnosis_code",
    right_on="diagnosis_code",
    how="left"
)

# ---------------------------
# 2. Merge documentation logs
# ---------------------------
df = df.merge(
    docs,
    on="encounter_id",
    how="left"
)

# ---------------------------
# 3. Add medication count per encounter
# ---------------------------
med_counts = meds.groupby("encounter_id").size().reset_index(name="med_count")

df = df.merge(
    med_counts,
    on="encounter_id",
    how="left"
)

df["med_count"] = df["med_count"].fillna(0)

# ---------------------------
# 4. Final cleaning
# ---------------------------
df.replace("?", None, inplace=True)
df.fillna(0, inplace=True)

# ---------------------------
# 5. Export final file
# ---------------------------
output_path = "/Users/naredlasiri/Desktop/final_hospital_dataset.csv"
df.to_csv(output_path, index=False)

output_path


# In[ ]:




