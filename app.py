from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=8)  # bisa 4, 6, atau 8

from flask import Flask, render_template, request, send_file, jsonify
import pdfplumber
import pandas as pd
from openpyxl import Workbook
from io import BytesIO
import mysql.connector
import threading  # <-- tambahan

app = Flask(__name__)
UPLOAD_LIMIT = 150   # batas upload PDF

# ================================
# PROGRESS STATE (GLOBAL)
# ================================
progress = {"total": 0, "done": 0}
progress_lock = threading.Lock()


# ================================
# KONEKSI MYSQL (Laragon)
# ================================
def db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",        # kalau kamu pakai password di Laragon, isi di sini
        database="db_wilayah"
    )

# ================================
# ENDPOINT DROPDOWN WILAYAH (PAKAI KODE)
# ================================
@app.route("/get_provinsi")
def get_provinsi():
    con = db()
    cur = con.cursor(dictionary=True)
    # province_code + province
    cur.execute("""
        SELECT province_code AS code, province AS nama
        FROM provinces
        ORDER BY province
    """)
    return jsonify(cur.fetchall())

@app.route("/get_kabkot/<prov_code>")
def get_kabkot(prov_code):
    con = db()
    cur = con.cursor(dictionary=True)
    # filter by province_code
    cur.execute("""
        SELECT city_code AS code, city AS nama
        FROM cities
        WHERE province_code = %s
        ORDER BY city
    """, (prov_code,))
    return jsonify(cur.fetchall())

@app.route("/get_kecamatan/<city_code>")
def get_kecamatan(city_code):
    con = db()
    cur = con.cursor(dictionary=True)
    # filter by city_code
    cur.execute("""
        SELECT district_code AS code, district AS nama
        FROM districts
        WHERE city_code = %s
        ORDER BY district
    """, (city_code,))
    return jsonify(cur.fetchall())

@app.route("/get_deskel/<district_code>")
def get_deskel(district_code):
    con = db()
    cur = con.cursor(dictionary=True)
    # filter by district_code
    cur.execute("""
        SELECT village_code AS code, village AS nama
        FROM villages
        WHERE district_code = %s
        ORDER BY village
    """, (district_code,))
    return jsonify(cur.fetchall())


# ================================
# ENDPOINT PROGRESS
# ================================
@app.route("/progress")
def get_progress():
    # cukup kirim dict global
    return jsonify(progress)


# ================================
# FORMAT RT/RW JADI 3 DIGIT TERAKHIR
# ================================
def format_rt_rw_columns(df):
    """
    Ambil 3 digit terakhir dari kolom 'rt' dan 'rw'.
    """
    def format_value(value):
        digits = "".join(filter(str.isdigit, str(value)))
        return digits[-3:] if digits else ""
    df["rt"] = df["rt"].apply(format_value)
    df["rw"] = df["rw"].apply(format_value)
    return df


# ================================
# PDF → DATAFRAME
# ================================
def convert_pdf_to_dataframe(file_stream):

    rows = []

    def is_valid_dpt_row(row):
        try:
            int(str(row[0]).strip())
            return True
        except:
            return False

    with pdfplumber.open(file_stream) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:

                    if not any(row):
                        continue

                    # skip header
                    if str(row[0]).strip().lower() in ["no", "nomor", "nama"]:
                        continue

                    if not is_valid_dpt_row(row):
                        continue

                    rows.append(row)

    # jika tidak ada data
    if not rows:
        return pd.DataFrame(columns=[
            "no","nama","jenis_kelamin","usia",
            "provinsi","kode_provinsi","kabkot","kode_kabkot",
            "kecamatan","kode_kecamatan","deskel","kode_deskel",
            "tps","alamat","rt","rw","ket"
        ])

    # samakan panjang baris
    max_len = max(len(r) for r in rows)
    for r in rows:
        while len(r) < max_len:
            r.append("")

    # ambil 8 kolom pertama dari PDF
    df = pd.DataFrame(rows)
    df = df.iloc[:, :8]
    df.columns = ["no","nama","jenis_kelamin","usia","alamat","rt","rw","ket"]

    # ===============================
    # HAPUS BARIS SAMPAH 1–8
    # ===============================
    if not df.empty:
        first = df.iloc[0].astype(str).str.strip().tolist()
        if first[:8] == [str(i) for i in range(1, 9)]:
            df = df.iloc[1:]
    # ===============================

    # ===============================
    # HAPUS SEMUA ROW TIDAK VALID (SUPER CLEAN)
    # ===============================
    def is_int(val):
        try:
            int(str(val).strip())
            return True
        except:
            return False

    # Hapus row yang kolom 'no' bukan angka
    df = df[df["no"].apply(is_int)]
    df = df.reset_index(drop=True)
    # ===============================

    # tambah kolom kosong untuk wilayah & tps
    insert_cols = [
        "provinsi","kode_provinsi","kabkot","kode_kabkot",
        "kecamatan","kode_kecamatan","deskel","kode_deskel","tps"
    ]
    for col in insert_cols:
        df[col] = ""

    # urutan final kolom
    final_columns = [
        "no","nama","jenis_kelamin","usia",
        "provinsi","kode_provinsi","kabkot","kode_kabkot",
        "kecamatan","kode_kecamatan","deskel","kode_deskel",
        "tps","alamat","rt","rw","ket"
    ]

    # format RT/RW
    df = format_rt_rw_columns(df)

    return df.reindex(columns=final_columns)


# ================================
# HALAMAN UTAMA UPLOAD
# ================================
@app.route("/", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":

        # ambil kode dari dropdown
        prov_code = request.form.get("provinsi")
        kab_code  = request.form.get("kabkot")
        kec_code  = request.form.get("kecamatan")
        des_code  = request.form.get("deskel")

        # ambil detail wilayah berdasarkan CODE
        con = db()
        cur = con.cursor(dictionary=True)

        # PROVINSI
        cur.execute("""
            SELECT province AS nama_prov, province_code AS kode_prov
            FROM provinces
            WHERE province_code = %s
        """, (prov_code,))
        prov = cur.fetchone()

        # KAB/KOTA
        cur.execute("""
            SELECT city AS nama_kab, city_code AS kode_kab
            FROM cities
            WHERE city_code = %s
        """, (kab_code,))
        kab = cur.fetchone()

        # KECAMATAN
        cur.execute("""
            SELECT district AS nama_kec, district_code AS kode_kec
            FROM districts
            WHERE district_code = %s
        """, (kec_code,))
        kec = cur.fetchone()

        # DESA/KEL
        cur.execute("""
            SELECT village AS nama_des, village_code AS kode_des
            FROM villages
            WHERE village_code = %s
        """, (des_code,))
        des = cur.fetchone()

        # file-file PDF
        files = request.files.getlist("pdf_file[]")

        if len(files) == 0:
            return "Tidak ada file upload!"
        if len(files) > UPLOAD_LIMIT:
            return f"Maksimal upload {UPLOAD_LIMIT} file!", 400

        # set progress total & reset done
        with progress_lock:
            progress["total"] = len(files)
            progress["done"] = 0

        # nama file output berdasarkan nama wilayah
        wilayah = f"{prov['nama_prov']}_{kab['nama_kab']}_{kec['nama_kec']}_{des['nama_des']}"
        wilayah = wilayah.replace(" ", "_").upper()

        # workbook XLSX multi-sheet
        wb = Workbook()
        wb.remove(wb.active)

        # PROSES PDF PARALEL (TIDAK DIUBAH)
        dfs = list(executor.map(convert_pdf_to_dataframe, files))

        for df, f in zip(dfs, files):
            pdf_name = f.filename

            # ambil nomor TPS dari nama file
            tps_part = pdf_name.split("TPS")[-1]
            tps_number = "".join(filter(str.isdigit, tps_part)) or "0"
            sheet_name = f"TPS {int(tps_number)}"  # nama sheet pakai angka asli

            # isi kolom wilayah dari hasil query
            df["provinsi"] = prov["nama_prov"]
            df["kode_provinsi"] = prov["kode_prov"]

            df["kabkot"] = kab["nama_kab"]
            df["kode_kabkot"] = kab["kode_kab"]

            df["kecamatan"] = kec["nama_kec"]
            df["kode_kecamatan"] = kec["kode_kec"]

            df["deskel"] = des["nama_des"]
            df["kode_deskel"] = des["kode_des"]

            # TPS 3 digit di kolom tps (001, 012, 134 dst)
            df["tps"] = tps_number.zfill(3)

            # buat sheet
            ws = wb.create_sheet(title=sheet_name)

            # header
            for col_idx, col_name in enumerate(df.columns, start=1):
                ws.cell(row=1, column=col_idx, value=col_name)

            # isi data
            for row_idx, row_data in df.iterrows():
                for col_idx, value in enumerate(row_data, start=1):
                    ws.cell(row=row_idx + 2, column=col_idx, value=value)

            # UPDATE PROGRESS (tambah 1 file selesai)
            with progress_lock:
                progress["done"] += 1

        # simpan ke BytesIO
        output = BytesIO()
        file_name = f"{wilayah}.xlsx"
        wb.save(output)
        output.seek(0)

        return send_file(
            output,
            as_attachment=True,
            download_name=file_name,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    return render_template("index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
