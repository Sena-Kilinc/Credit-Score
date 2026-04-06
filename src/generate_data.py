# pandas : veri işleme kütüphanesi (Excel gibi düşün ama kodla)
# numpy  : matematiksel işlemler, rastgele sayı üretme
# sqlalchemy : Python'dan veritabanına bağlanma köprüsü
# os     : işletim sistemi değişkenlerine erişim (örn. env variables)

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os

def generate_credit_data(n_samples=1000):
    """
    n_samples adet sahte müşteri verisi üretir.
    Gerçek hayata benzeteceğiz:
    - Yaşlı ve çok çalışmış kişiler genellikle düşük risk
    - Çok geç ödemesi olanlar yüksek risk
    """

    # np.random.seed : Rastgele sayıları 'sabitler'
    # Seed sayesinde her çalıştırdığında AYNI veri üretilir
    # Bu önemli: deneylerin tekrar edilebilir olması lazım
    np.random.seed(42)

    # np.random.randint(min, max, n_samples)
    # min ile max arasında n_samples adet TAM sayı üretir
    age = np.random.randint(22, 65, n_samples)

    # np.random.normal(ortalama, standart_sapma, adet)
    # Gerçek hayatta gelirler 'normal dağılım' gösterir
    # Çoğu insan ortalamaya yakın, çok az kişi çok zengin/fakir
    # .clip(min) : minimum değeri sınırla, negatif gelir olmaz
    income = np.random.normal(75000, 30000, n_samples).clip(15000)

    # Kredi miktarı genellikle gelirle orantılı
    # income * 0.3 = gelirin %30'u kadar kredi çekmiş gibi düşün
    loan_amount = (income * np.random.uniform(0.1, 0.8, n_samples)).clip(5000)

    # Geç ödeme: çoğu kişide az, bazılarında çok
    # np.random.poisson : nadir olaylar için uygun dağılım
    num_late_payments = np.random.poisson(1.5, n_samples)

    num_credit_cards = np.random.randint(1, 8, n_samples)

    employment_years = np.random.randint(0, 35, n_samples)

    # ── Risk Etiketi Hesapla ──────────────────────────
    # Basit bir kural tabanlı sistem kuruyoruz:
    # Her müşteri için 'risk skoru' hesapla, sonra 3 gruba böl

    # np.zeros : sıfırlarla dolu dizi oluştur (başlangıç skoru)
    risk_score = np.zeros(n_samples)

    # Geç ödeme varsa risk artar (en güçlü gösterge)
    risk_score += num_late_payments * 2

    # Gelir yüksekse risk azalır
    # income / 10000 -> 75000 TL gelir = 7.5 puan düşüş
    risk_score -= income / 10000

    # Uzun süredir çalışıyorsa risk azalır (istikrar göstergesi)
    risk_score -= employment_years * 0.3

    # Genç yaş biraz risk ekler (iş hayatı kısa olabilir)
    risk_score += (30 - age).clip(0) * 0.2

    # risk_score'u 3 kategoriye böl:
    # pd.cut : sayısal değeri kategoriye çevirir
    # np.percentile : verinin %33 ve %66'lık noktaları
    # Yani: en düşük 1/3 = düşük risk, orta 1/3 = orta, üst 1/3 = yüksek
    risk_label = pd.cut(
        risk_score,
        bins=[-np.inf,
              np.percentile(risk_score, 33),
              np.percentile(risk_score, 66),
              np.inf],
        labels=[0, 1, 2]   # 0=düşük, 1=orta, 2=yüksek risk
    ).astype(int)

    # pd.DataFrame : tüm dizileri bir araya getirip tablo yap
    # Sütun adları sözlük anahtarları oluyor
    df = pd.DataFrame({
        'age'               : age,
        'income'            : income.round(2),
        'loan_amount'       : loan_amount.round(2),
        'num_late_payments' : num_late_payments,
        'num_credit_cards'  : num_credit_cards,
        'employment_years'  : employment_years,
        'risk_label'        : risk_label
    })

    return df


def save_to_postgres(df):
    """
    DataFrame'i PostgreSQL'e kaydeder.
    """

    # os.getenv : ortam değişkenini oku
    # kinci parametre : değişken yoksa bu değeri kullan (varsayılan)
    # Docker Compose'da DB_CONN=postgresql://... diye tanımlamıştık
    db_url = os.getenv(
        "DB_CONN",
        "postgresql://mlops:mlops123@localhost:5432/creditdb"
    )

    # create_engine : veritabanına bağlantı motoru oluşturur
    # Bunu bir kez kuruyorsun, sonra tekrar tekrar kullanıyorsun
    engine = create_engine(db_url)

    # engine.connect() : gerçek bağlantıyı açar
    with engine.connect() as conn:

        # Tablo yoksa oluştur
        # text() : ham SQL yazmanı sağlar
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS credit_data (
                id                 SERIAL PRIMARY KEY,
                age                INTEGER,
                income             FLOAT,
                loan_amount        FLOAT,
                num_late_payments  INTEGER,
                num_credit_cards   INTEGER,
                employment_years   INTEGER,
                risk_label         INTEGER,
                created_at         TIMESTAMP DEFAULT NOW()
            )
        """))

        # Değişiklikleri kaydet (SQL'de 'commit' denir)
        conn.commit()

    # df.to_sql : DataFrame'i doğrudan tabloya yaz
    # if_exists='append' : tablo varsa üstüne ekle, silme
    # index=False        : DataFrame'in sıra numarasını (0,1,2...) yazma
    df.to_sql(
        name='credit_data',
        con=engine,
        if_exists='append',
        index=False
    )

    print(f"Başarılı! {len(df)} satır 'credit_data' tablosuna yazıldı.")
    print(f"Risk dağılımı:\n{df['risk_label'].value_counts().sort_index()}")


# Bu blok önemli:
# Dosyayı doğrudan çalıştırırsan (python generate_data.py) çalışır
# Başka dosyadan import edersen çalışmaz
# Airflow DAG'dan import ederken istemeden çalışmasın diye
if __name__ == "__main__":
    print("Veri üretiliyor...")
    df = generate_credit_data(n_samples=1000)
    print(f"Üretilen veri önizlemesi:\n{df.head()}")
    print(f"\nstatistikler:\n{df.describe()}")
    save_to_postgres(df)
