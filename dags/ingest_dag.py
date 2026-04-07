# ── Import'lar ────────────────────────────────────────────────

# DAG : iş akışını tanımlayan ana sınıf
# datetime : DAG'ın başlangıç tarihini vermek için
from airflow import DAG
from datetime import datetime, timedelta

# PythonOperator : "bir Python fonksiyonu çalıştır" operatörü
# Airflow'da farklı türde operatörler var:
#   BashOperator    -> terminal komutu çalıştırır
#   PythonOperator  -> Python fonksiyonu çalıştırır
#   EmailOperator   -> e-posta gönderir  (biz kullanmayacağız)
from airflow.operators.python import PythonOperator

# Kendi yazdığımız fonksiyonları içe aktar
# sys.path ile Airflow'a "src klasörünü de tara" diyoruz
import sys
sys.path.insert(0, '/opt/airflow/src')

from generate_data import generate_credit_data, save_to_postgres


# ── Görev Fonksiyonları ───────────────────────────────────────
# Airflow task'larına doğrudan fonksiyon vermek yerine
# küçük "sarmalayıcı" fonksiyonlar yazıyoruz.
# Neden? Çünkü Airflow bazı durumlarda ekstra parametreler inject eder,
# bu sarmalayıcı katman o karmaşıklığı gizler.

def task_generate_and_save():
    """
    Veri üretir ve PostgreSQL'e kaydeder.
    İki işi tek task'ta birleştiriyoruz çünkü
    aralarında geçici veriyi saklamaya gerek yok.
    """
    print("Veri üretimi başlıyor...")
    df = generate_credit_data(n_samples=1000)

    print(f"Üretilen satır sayısı: {len(df)}")
    print(f"İlk 3 satır:\n{df.head(3)}")

    save_to_postgres(df)
    print("Veri başarıyla kaydedildi.")


# ── DAG Tanımı ────────────────────────────────────────────────

# default_args : Tüm task'lara uygulanacak varsayılan ayarlar
# Tek tek her task'a yazmak yerine bir kez burada tanımlıyorsun
default_args = {
    'owner': 'mlops',
    'retries': 1,
    'retry_delay': timedelta(seconds=30),  # ← retry_delay_seconds değil bu!
}

# with DAG(...) as dag :
# Python'un context manager özelliği.
# Bu blok içinde tanımlanan her task otomatik olarak bu DAG'a ait olur.
# Manuel olarak her task'a dag=dag yazmana gerek kalmaz.
with DAG(
    dag_id='credit_ingest',         # Airflow UI'da görünecek isim (benzersiz olmalı)
    default_args=default_args,
    description='Kredi verisi üretir ve PostgreSQL e kaydeder',
    schedule='@daily',              # Her gün çalış
                                    # Alternatifler: '@hourly', '@weekly', '0 9 * * *' (cron)
    start_date=datetime(2024, 1, 1),# "Bu DAG ne zaman başladı sayılsın?"
                                    # Geçmiş bir tarih veriyoruz, Airflow hemen çalıştırabilsin
    catchup=False,                  # catchup=True olsaydı Airflow geçmiş tüm günleri
                                    # tek tek çalıştırmaya çalışırdı — bunu istemiyoruz
    tags=['ingest', 'credit'],      # Airflow UI'da filtreleme için etiketler
) as dag:

    # PythonOperator : bir Python fonksiyonunu task haline getirir
    # task_id        : bu task'ın benzersiz adı (DAG içinde tekrar etmemeli)
    # python_callable: çalıştırılacak Python fonksiyonu (parantez YOK, referans veriyoruz)
    ingest_task = PythonOperator(
        task_id='generate_and_save_data',
        python_callable=task_generate_and_save,
    )

    # Şu an tek task var, bu yüzden bağlantı tanımlamaya gerek yok.
    # İleride birden fazla task olunca şöyle yazacağız:
    #   task_a >> task_b >> task_c
    # Bu "önce a, sonra b, sonra c çalış" demek.
    ingest_task