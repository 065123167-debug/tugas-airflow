# pipeline_monitoring_server.py
# Pipeline ETL untuk monitoring performa server harian
# Data: historical_server_data.csv (103 server, Jan-Jun 2026)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import csv
import os

# ================================================
# KONFIGURASI DAG
# ================================================
default_args = {
    'owner'           : 'tim_data_engineering',
    'depends_on_past' : False,
    'start_date'      : datetime(2026, 1, 1),
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=5),
}

dag = DAG(
    dag_id='pipeline_monitoring_server',
    default_args=default_args,
    description='Pipeline otomatis monitoring performa 103 server harian',
    schedule_interval='@daily',
    catchup=False,
    tags=['server', 'monitoring', 'etl', 'infrastruktur'],
)

PATH_DATA = '/usr/local/airflow/dags/historical_server_data.csv'


# ================================================
# TASK 1: EKSTRAK DATA
# ================================================
def ekstrak_data_server(**context):
    print("=" * 60)
    print("TASK 1: EKSTRAK DATA SERVER")
    print("=" * 60)

    semua_data = []

    with open(PATH_DATA, 'r') as f:
        reader = csv.DictReader(f)
        for baris in reader:
            semua_data.append({
                'date'       : baris['date'],
                'server'     : baris['server'],
                'power_state': int(baris['power_state']),
                'cpu_avg'    : float(baris['cpu_avg']),
                'cpu_max'    : float(baris['cpu_max']),
                'mem_avg'    : float(baris['mem_avg']),
                'mem_max'    : float(baris['mem_max']),
            })

    total_baris  = len(semua_data)
    total_server = len(set(d['server'] for d in semua_data))
    tgl_mulai    = min(d['date'] for d in semua_data)
    tgl_akhir    = max(d['date'] for d in semua_data)

    print(f"✅ File berhasil dibaca!")
    print(f"   Total baris data  : {total_baris:,}")
    print(f"   Total server      : {total_server}")
    print(f"   Periode data      : {tgl_mulai} s/d {tgl_akhir}")

    context['task_instance'].xcom_push(
        key='raw_data',
        value=json.dumps(semua_data)
    )

    return total_baris


# ================================================
# TASK 2: VALIDASI DATA
# ================================================
def validasi_data(**context):
    print("=" * 60)
    print("TASK 2: VALIDASI & PEMBERSIHAN DATA")
    print("=" * 60)

    raw = context['task_instance'].xcom_pull(
        task_ids='ekstrak_data_server',
        key='raw_data'
    )
    data = json.loads(raw)

    data_bersih     = []
    data_bermasalah = []

    for baris in data:
        masalah = []

        if not (0 <= baris['cpu_avg'] <= 100):
            masalah.append(f"cpu_avg tidak valid: {baris['cpu_avg']}")
        if not (0 <= baris['cpu_max'] <= 100):
            masalah.append(f"cpu_max tidak valid: {baris['cpu_max']}")
        if not (0 <= baris['mem_avg'] <= 100):
            masalah.append(f"mem_avg tidak valid: {baris['mem_avg']}")
        if not (0 <= baris['mem_max'] <= 100):
            masalah.append(f"mem_max tidak valid: {baris['mem_max']}")
        if baris['power_state'] not in [0, 1]:
            masalah.append(f"power_state tidak valid: {baris['power_state']}")

        if masalah:
            baris['masalah'] = masalah
            data_bermasalah.append(baris)
        else:
            data_bersih.append(baris)

    pct_bersih = (len(data_bersih) / len(data)) * 100

    print(f"✅ Validasi selesai!")
    print(f"   Total data masuk     : {len(data):,}")
    print(f"   Data bersih/valid    : {len(data_bersih):,} ({pct_bersih:.1f}%)")
    print(f"   Data bermasalah      : {len(data_bermasalah):,}")

    if not data_bermasalah:
        print(f"   ✅ Semua data dalam kondisi baik!")

    context['task_instance'].xcom_push(
        key='data_bersih',
        value=json.dumps(data_bersih)
    )

    return len(data_bersih)


# ================================================
# TASK 3: DETEKSI SERVER KRITIS
# ================================================
def deteksi_server_kritis(**context):
    print("=" * 60)
    print("TASK 3: DETEKSI SERVER KRITIS")
    print("=" * 60)

    raw = context['task_instance'].xcom_pull(
        task_ids='validasi_data',
        key='data_bersih'
    )
    data = json.loads(raw)

    THRESHOLD_KRITIS  = 85.0
    THRESHOLD_WARNING = 70.0

    server_kritis  = {}
    server_warning = {}

    for baris in data:
        nama_server = baris['server']
        cpu         = baris['cpu_avg']
        mem         = baris['mem_avg']

        if cpu > THRESHOLD_KRITIS or mem > THRESHOLD_KRITIS:
            if nama_server not in server_kritis:
                server_kritis[nama_server] = {
                    'server'                 : nama_server,
                    'jumlah_hari_kritis'     : 0,
                    'cpu_avg_tertinggi'      : 0,
                    'mem_avg_tertinggi'      : 0,
                    'tanggal_terakhir_kritis': '',
                }
            srv = server_kritis[nama_server]
            srv['jumlah_hari_kritis']     += 1
            srv['cpu_avg_tertinggi']       = max(srv['cpu_avg_tertinggi'], cpu)
            srv['mem_avg_tertinggi']       = max(srv['mem_avg_tertinggi'], mem)
            srv['tanggal_terakhir_kritis'] = baris['date']

        elif cpu > THRESHOLD_WARNING or mem > THRESHOLD_WARNING:
            if nama_server not in server_warning:
                server_warning[nama_server] = 0
            server_warning[nama_server] += 1

    list_kritis = sorted(
        server_kritis.values(),
        key=lambda x: x['jumlah_hari_kritis'],
        reverse=True
    )

    print(f"   Threshold KRITIS   : CPU/MEM > {THRESHOLD_KRITIS}%")
    print(f"   Threshold WARNING  : CPU/MEM > {THRESHOLD_WARNING}%")
    print()
    print(f"🔴 Server KRITIS  : {len(server_kritis)} server")
    print(f"🟡 Server WARNING : {len(server_warning)} server")
    print()

    if list_kritis:
        print("   TOP 5 Server Paling Sering Kritis:")
        print(f"   {'Server':<35} {'Hari Kritis':>11} {'Max CPU':>8} {'Max MEM':>8}")
        print(f"   {'-'*35} {'-'*11} {'-'*8} {'-'*8}")
        for s in list_kritis[:5]:
            nama_pendek = s['server'].split('.')[0]
            print(f"   {nama_pendek:<35} {s['jumlah_hari_kritis']:>11} "
                  f"{s['cpu_avg_tertinggi']:>7.1f}% {s['mem_avg_tertinggi']:>7.1f}%")

    hasil_deteksi = {
        'jumlah_server_kritis' : len(server_kritis),
        'jumlah_server_warning': len(server_warning),
        'daftar_kritis'        : list_kritis,
        'threshold_kritis'     : THRESHOLD_KRITIS,
        'threshold_warning'    : THRESHOLD_WARNING,
    }

    context['task_instance'].xcom_push(
        key='hasil_deteksi',
        value=json.dumps(hasil_deteksi)
    )

    return len(server_kritis)


# ================================================
# TASK 4: AGREGASI STATISTIK
# ================================================
def agregasi_statistik(**context):
    print("=" * 60)
    print("TASK 4: AGREGASI STATISTIK INFRASTRUKTUR")
    print("=" * 60)

    raw = context['task_instance'].xcom_pull(
        task_ids='validasi_data',
        key='data_bersih'
    )
    data = json.loads(raw)

    total_baris  = len(data)
    server_aktif = sum(1 for d in data if d['power_state'] == 1)
    server_mati  = total_baris - server_aktif
    rata_cpu_avg = sum(d['cpu_avg'] for d in data) / total_baris
    rata_mem_avg = sum(d['mem_avg'] for d in data) / total_baris

    stats_per_server = {}
    for baris in data:
        srv = baris['server']
        if srv not in stats_per_server:
            stats_per_server[srv] = {'cpu_list': [], 'mem_list': []}
        stats_per_server[srv]['cpu_list'].append(baris['cpu_avg'])
        stats_per_server[srv]['mem_list'].append(baris['mem_avg'])

    ringkasan_server = []
    for srv, vals in stats_per_server.items():
        avg_cpu = sum(vals['cpu_list']) / len(vals['cpu_list'])
        avg_mem = sum(vals['mem_list']) / len(vals['mem_list'])
        ringkasan_server.append({
            'server' : srv,
            'avg_cpu': round(avg_cpu, 2),
            'avg_mem': round(avg_mem, 2),
        })

    server_tersibuk  = max(ringkasan_server, key=lambda x: x['avg_cpu'])
    server_tersantai = min(ringkasan_server, key=lambda x: x['avg_cpu'])

    tren_bulanan = {}
    for baris in data:
        bulan = baris['date'][:7]
        if bulan not in tren_bulanan:
            tren_bulanan[bulan] = {'cpu_list': [], 'mem_list': []}
        tren_bulanan[bulan]['cpu_list'].append(baris['cpu_avg'])
        tren_bulanan[bulan]['mem_list'].append(baris['mem_avg'])

    tren_hasil = {}
    for bulan, vals in sorted(tren_bulanan.items()):
        tren_hasil[bulan] = {
            'avg_cpu': round(sum(vals['cpu_list']) / len(vals['cpu_list']), 2),
            'avg_mem': round(sum(vals['mem_list']) / len(vals['mem_list']), 2),
        }

    print(f"   Total data diproses  : {total_baris:,} baris")
    print(f"   Total server unik    : {len(ringkasan_server)}")
    print(f"   Record server aktif  : {server_aktif:,} (power=1)")
    print(f"   Record server mati   : {server_mati:,} (power=0)")
    print()
    print(f"   Rata-rata CPU global : {rata_cpu_avg:.2f}%")
    print(f"   Rata-rata MEM global : {rata_mem_avg:.2f}%")
    print()
    print(f"   Server paling sibuk  : {server_tersibuk['server'].split('.')[0]}")
    print(f"                          CPU rata-rata {server_tersibuk['avg_cpu']}%")
    print(f"   Server paling santai : {server_tersantai['server'].split('.')[0]}")
    print(f"                          CPU rata-rata {server_tersantai['avg_cpu']}%")
    print()
    print("   Tren Bulanan:")
    print(f"   {'Bulan':<10} {'CPU Avg':>8} {'MEM Avg':>8}")
    print(f"   {'-'*10} {'-'*8} {'-'*8}")
    for bulan, val in tren_hasil.items():
        print(f"   {bulan:<10} {val['avg_cpu']:>7.2f}% {val['avg_mem']:>7.2f}%")

    hasil_agregasi = {
        'total_baris'     : total_baris,
        'total_server'    : len(ringkasan_server),
        'rata_cpu_avg'    : round(rata_cpu_avg, 2),
        'rata_mem_avg'    : round(rata_mem_avg, 2),
        'server_tersibuk' : server_tersibuk,
        'server_tersantai': server_tersantai,
        'tren_bulanan'    : tren_hasil,
    }

    context['task_instance'].xcom_push(
        key='hasil_agregasi',
        value=json.dumps(hasil_agregasi)
    )

    return round(rata_cpu_avg, 2)


# ================================================
# TASK 5: LAPORAN AKHIR
# ================================================
def buat_laporan_akhir(**context):
    print("=" * 60)
    print("TASK 5: LAPORAN AKHIR MONITORING SERVER")
    print("=" * 60)

    raw_deteksi  = context['task_instance'].xcom_pull(
        task_ids='deteksi_server_kritis', key='hasil_deteksi'
    )
    raw_agregasi = context['task_instance'].xcom_pull(
        task_ids='agregasi_statistik', key='hasil_agregasi'
    )

    deteksi  = json.loads(raw_deteksi)
    agregasi = json.loads(raw_agregasi)

    waktu_laporan = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║     LAPORAN MONITORING INFRASTRUKTUR SERVER          ║")
    print(f"║     Dibuat: {waktu_laporan}                  ║")
    print("╠══════════════════════════════════════════════════════╣")
    print(f"║  Total Server Dipantau : {agregasi['total_server']:<5} server               ║")
    print(f"║  Total Data Diproses   : {agregasi['total_baris']:,} baris              ║")
    print(f"║  Rata-rata CPU Global  : {agregasi['rata_cpu_avg']:<6.2f}%                   ║")
    print(f"║  Rata-rata MEM Global  : {agregasi['rata_mem_avg']:<6.2f}%                   ║")
    print("╠══════════════════════════════════════════════════════╣")
    print(f"║  🔴 Server KRITIS  : {deteksi['jumlah_server_kritis']:<3} server                        ║")
    print(f"║  🟡 Server WARNING : {deteksi['jumlah_server_warning']:<3} server                        ║")
    print(f"║  ✅ Server NORMAL  : {agregasi['total_server'] - deteksi['jumlah_server_kritis'] - deteksi['jumlah_server_warning']:<3} server                        ║")
    print("╠══════════════════════════════════════════════════════╣")
    print("║  TOP 3 SERVER PALING KRITIS                         ║")
    print("╠══════════════════════════════════════════════════════╣")

    for i, srv in enumerate(deteksi['daftar_kritis'][:3], 1):
        nama = srv['server'].split('.')[0][:30]
        print(f"║  {i}. {nama:<30}               ║")
        print(f"║     Hari kritis: {srv['jumlah_hari_kritis']:<3} | "
              f"Max CPU: {srv['cpu_avg_tertinggi']:.1f}% | "
              f"Max MEM: {srv['mem_avg_tertinggi']:.1f}%  ║")

    print("╠══════════════════════════════════════════════════════╣")
    print("║  TREN BULANAN                                        ║")
    print("╠══════════════════════════════════════════════════════╣")
    for bulan, val in agregasi['tren_bulanan'].items():
        bar_cpu = '█' * int(val['avg_cpu'] / 5)
        print(f"║  {bulan}  CPU:{val['avg_cpu']:5.1f}% {bar_cpu:<15}          ║")

    print("╠══════════════════════════════════════════════════════╣")
    print("║  ✅ Pipeline selesai! Laporan siap dikirim ke tim IT ║")
    print("╚══════════════════════════════════════════════════════╝")

    return "PIPELINE_SUCCESS"


# ================================================
# SUSUN URUTAN TASK (ALUR DAG)
# ================================================
t1_ekstrak = PythonOperator(
    task_id='ekstrak_data_server',
    python_callable=ekstrak_data_server,
    dag=dag,
)

t2_validasi = PythonOperator(
    task_id='validasi_data',
    python_callable=validasi_data,
    dag=dag,
)

t3_deteksi = PythonOperator(
    task_id='deteksi_server_kritis',
    python_callable=deteksi_server_kritis,
    dag=dag,
)

t4_agregasi = PythonOperator(
    task_id='agregasi_statistik',
    python_callable=agregasi_statistik,
    dag=dag,
)

t5_laporan = PythonOperator(
    task_id='buat_laporan_akhir',
    python_callable=buat_laporan_akhir,
    dag=dag,
)

# Urutan pipeline:
# Ekstrak → Validasi → Deteksi & Agregasi (paralel) → Laporan
t1_ekstrak >> t2_validasi >> [t3_deteksi, t4_agregasi] >> t5_laporan