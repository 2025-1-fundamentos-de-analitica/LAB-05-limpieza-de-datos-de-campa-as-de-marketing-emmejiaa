import os
import zipfile
import pandas as pd

# Define las rutas de entrada y salida
INPUT_FOLDER = "files/input/"
OUTPUT_FOLDER = "files/output/"

# Crea la carpeta de salida si no existe
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def clean_campaign_data():
    # Listar todos los archivos .zip en la carpeta de entrada
    zip_files = [f for f in os.listdir(INPUT_FOLDER) if f.endswith(".zip")]

    # DataFrames para almacenar los datos procesados
    client_data = []
    campaign_data = []
    economics_data = []

    # Procesar cada archivo .zip
    for zip_file in zip_files:
        with zipfile.ZipFile(os.path.join(INPUT_FOLDER, zip_file), 'r') as z:
            for filename in z.namelist():
                if filename.endswith(".csv"):
                    with z.open(filename) as f:
                        df = pd.read_csv(f)

                        # Limpieza de client.csv
                        client_df = df[[
                            'client_id', 'age', 'job', 'marital', 'education',
                            'credit_default', 'mortgage'
                        ]].copy()

                        client_df['job'] = client_df['job'].str.replace(".", "", regex=False)
                        client_df['job'] = client_df['job'].str.replace("-", "_", regex=False)
                        client_df['education'] = client_df['education'].str.replace(".", "_", regex=False)
                        client_df['education'] = client_df['education'].replace("unknown", pd.NA)
                        client_df['credit_default'] = client_df['credit_default'].apply(lambda x: 1 if x == "yes" else 0)
                        client_df['mortgage'] = client_df['mortgage'].apply(lambda x: 1 if x == "yes" else 0)
                        client_data.append(client_df)

                        # Limpieza de campaign.csv
                        campaign_df = df[[
                            'client_id', 'number_contacts', 'contact_duration',
                            'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome',
                            'day', 'month'
                        ]].copy()

                        campaign_df['previous_outcome'] = campaign_df['previous_outcome'].apply(lambda x: 1 if x == "success" else 0)
                        campaign_df['campaign_outcome'] = campaign_df['campaign_outcome'].apply(lambda x: 1 if x == "yes" else 0)
                        campaign_df['last_contact_date'] = pd.to_datetime(
                            "2022-" + campaign_df['month'].astype(str) + "-" + campaign_df['day'].astype(str),
                            format="%Y-%b-%d",
                            errors="coerce"
                        ).dt.strftime("%Y-%m-%d")
                        campaign_df.drop(columns=['day', 'month'], inplace=True)
                        campaign_data.append(campaign_df)

                        # Limpieza de economics.csv
                        economics_df = df[[
                            'client_id', 'cons_price_idx', 'euribor_three_months'
                        ]].copy()
                        economics_data.append(economics_df)

    # Concatenar y guardar los datos procesados
    pd.concat(client_data).to_csv(os.path.join(OUTPUT_FOLDER, "client.csv"), index=False)
    pd.concat(campaign_data).to_csv(os.path.join(OUTPUT_FOLDER, "campaign.csv"), index=False)
    pd.concat(economics_data).to_csv(os.path.join(OUTPUT_FOLDER, "economics.csv"), index=False)

if __name__ == "__main__":
    clean_campaign_data()