import pandas as pd  # Pandas kitabxanasını idxal et (CSV faylını oxumaq üçün)
from sqlalchemy import create_engine  # SQLAlchemy vasitəsilə verilənlər bazası bağlantısı yaratmaq üçün
import os  # Fayl sisteminə baxış və faylın mövcudluğunu yoxlamaq üçün

def csv_to_mssql(csv_path, server, database, table_name):
    try:
        # CSV faylının mövcudluğunu yoxla
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"❌ CSV faylı tapılmadı: {csv_path}")
        
        # CSV faylını DataFrame kimi oxu
        df = pd.read_csv(csv_path)
        print(f"✅ CSV faylı oxundu: {csv_path}")

        # MSSQL üçün əlaqə sətri yarat
        connection_string = f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

        # SQLAlchemy engine yaratmaqla serverə bağlantı qur
        engine = create_engine(connection_string)
        print("✅ MSSQL server ilə əlaqə yaradıldı")

        # DataFrame-dəki məlumatları SQL cədvəlinə yaz
        # if_exists="replace" - əgər cədvəl varsa, əvvəlki məlumatları sil və yenidən yaz
        # index=False - DataFrame indeksini cədvələ əlavə etmə
        df.to_sql(table_name, con=engine,if_exists="replace",index=False)
        print(f"✅ Məlumatlar '{table_name}' cədvəlinə uğurla əlavə olundu")

    # Fayl tapılmadıqda yaranan xəta burada tutulur
    except FileNotFoundError as fnf_error:
        print(fnf_error)

    # Digər mümkün xətalar burada tutulur
    except Exception as e:
        print(f"❌ Xəta baş verdi: {str(e)}")

#Funksiyanı çağırmaq (CSV fayl yolunu, server, database və cədvəl adını veririk)
csv_to_mssql(
    csv_path=r"C:\Users\alekb\OneDrive\Desktop\ProductBonus.csv",
    server="LAPTOP-T13SKMB2\SQLEXPRESS",
    database="AdventureWorks2022",
    table_name="product_bonus"
)
