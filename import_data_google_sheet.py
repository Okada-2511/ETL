# ================================================================
# POSTGRESQL STAGING + CURRENT + AUDIT
# Nguồn dữ liệu: Google Sheet URL (thay vì file Excel local)
# ================================================================
# Bảng nghiệp vụ:
#   public.union_all_data_cap_don   ← union_all_data_cap_don (cấp đơn)
#   public.ds_nhan_su_affina        ← ds_nhan_su_affina (nhân sự)
#
# Quy tắc:
# 1) Python chỉ DROP/CREATE ở schema staging, KHÔNG REPLACE bảng nghiệp vụ.
# 2) Bảng public được giữ nguyên object => VIEW/FUNCTION đang tham chiếu vẫn sống.
# 3) Mỗi dòng có technical identity __source_row_ref = sheet::row.
#    Số hợp đồng KHÔNG được dùng làm khóa.
# 4) Khi dòng nguồn thay đổi: UPDATE + trigger ghi old/new vào audit.
# 5) Nhân sự được refresh qua staging, có snapshot tháng.
#
# Cách lấy URL CSV từ Google Sheet:
#   Mở Google Sheet → Chia sẻ (Anyone with link) → lấy SHEET_ID từ URL
#   URL CSV: https://docs.google.com/spreadsheets/d/<SHEET_ID>/export?format=csv&gid=<GID>
#   GID của từng sheet: xem trong URL khi click vào tab sheet đó
# ================================================================

import pandas as pd
import requests
import io
import ast
import operator
import re
import unicodedata
import warnings
from decimal import Decimal
from datetime import date
from sqlalchemy import create_engine, text

# ================================================================
# CẤU HÌNH KẾT NỐI DATABASE
# ================================================================
user     = "postgres"
password = "251104"
host     = "localhost"
port     = "5432"
database = "Sales Aff"

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

# Neon (cloud) - bỏ comment nếu dùng:
# engine = create_engine(
#     "postgresql://neondb_owner:npg_YwLu4OKNx6ej@ep-summer-waterfall-a172uzl8-pooler"
#     ".ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
# )


# ================================================================
# CẤU HÌNH GOOGLE SHEET URL
# ================================================================
# Thay SHEET_ID và GID bằng giá trị thực của bạn.
# GID = 0 thường là sheet đầu tiên.
#
# Ví dụ URL:
#   https://docs.google.com/spreadsheets/d/1aBcDeFgHiJkLmNoPqRsTuVwXyZ/export?format=csv&gid=0

# --- Cấp đơn (union_all_data_cap_don) ---
CAPDON_SHEET_ID = "1qc_QhrvpoLLp6w9RkGBEkm8qBO49GJE8oMlwkCdJOsk"   # ← sửa ở đây

# Mỗi sheet (tab) trong file có GID riêng. Lấy GID bằng cách click vào tab → xem URL.
CAPDON_SHEET_TABS = {
    "Bao hiem suc khoe": "0",      # ← sửa GID
    "Bao hiem oto":      "545072156",     # ← sửa GID
    "Bao hiem xe may":   "1083152355",      # ← sửa GID
    "Du lich":           "1693411265",      # ← sửa GID
    "Trach nhiem san pham": "1869858203",   # ← sửa GID
    "Bao hiem rui ro":   "1705621301",      # ← sửa GID
    "BHYT BHXH":         "1728841055",      # ← sửa GID
}

# --- Nhân sự (ds_nhan_su_affina) ---
NHANSU_SHEET_ID  = "506A9D11FC30ADA1!sdc94a2ac6d5149a7927f7610820bd9f0"  # ← sửa ở đây
NHANSU_SHEET_GID = "0"                             # ← sửa GID (tab DSNS AGENCY 2025)


def _build_gsheet_csv_url(sheet_id: str, gid: str) -> str:
    """Tạo URL export CSV từ Google Sheet ID và GID của tab."""
    return (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        f"/export?format=csv&gid={gid}"
    )


def _read_gsheet(sheet_id: str, gid: str, dtype=None, sheet_label: str = "") -> pd.DataFrame:
    """Tải một tab Google Sheet về DataFrame.
    
    Google Sheet phải được chia sẻ 'Anyone with the link can view'.
    """
    url = _build_gsheet_csv_url(sheet_id, gid)
    print(f"📥 Đang tải {'[' + sheet_label + '] ' if sheet_label else ''}từ Google Sheet...")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text), dtype=dtype)
    print(f"   ✔ {len(df)} dòng, {len(df.columns)} cột")
    return df


# ================================================================
# CONSTANTS
# ================================================================
TECH_MASTER_COLS = {
    'row_id', 'source_sheet', 'source_row_number', 'source_row_ref',
    'row_hash', 'last_synced_at', 'is_active'
}
TECH_STAGE_COLS = {
    '__source_sheet', '__source_row_number', '__source_row_ref', '__ingest_seq', 'row_hash'
}


# ================================================================
# HELPER: SQL UTILS
# ================================================================

def _qident(name):
    """Quote identifier PostgreSQL an toàn."""
    return '"' + str(name).replace('"', '""') + '"'


def _qualified(schema, table):
    return f"{_qident(schema)}.{_qident(table)}"


def _get_columns(conn, schema, table):
    rows = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
        ORDER BY ordinal_position
    """), {'schema': schema, 'table': table}).fetchall()
    return [r[0] for r in rows]


def _business_columns(conn, schema, table):
    return [c for c in _get_columns(conn, schema, table) if c not in TECH_MASTER_COLS]


# ================================================================
# METADATA / AUDIT SETUP  (dùng cho bảng cấp đơn)
# ================================================================

def _ensure_cap_don_metadata(conn, schema='public', table='union_all_data_cap_don'):
    tbl = _qualified(schema, table)
    for col_ddl in [
        f'ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "row_id" BIGINT GENERATED BY DEFAULT AS IDENTITY;',
        f'ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "source_sheet" TEXT;',
        f'ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "source_row_number" BIGINT;',
        f'ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "source_row_ref" TEXT;',
        f'ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "row_hash" TEXT;',
        f'ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "last_synced_at" TIMESTAMPTZ;',
        f'ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS "is_active" BOOLEAN DEFAULT TRUE;',
        f'UPDATE {tbl} SET "is_active" = TRUE WHERE "is_active" IS NULL;',
    ]:
        conn.execute(text(col_ddl))

    conn.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS "ux_{table}_source_row_ref"
        ON {tbl} ("source_row_ref")
        WHERE "source_row_ref" IS NOT NULL;
    """))


def _ensure_cap_don_audit(conn, schema='public', table='union_all_data_cap_don'):
    conn.execute(text('CREATE SCHEMA IF NOT EXISTS "audit"'))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS "audit"."{table}_history" (
            "history_id"     BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            "row_id"         BIGINT,
            "source_row_ref" TEXT,
            "operation"      TEXT NOT NULL,
            "changed_at"     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            "old_data"       JSONB,
            "new_data"       JSONB
        );
    """))

    func_name = f'audit.log_{table}_change'
    history_tbl = f'"audit"."{table}_history"'
    excl_arr = "ARRAY['row_id','source_sheet','source_row_number','source_row_ref','row_hash','last_synced_at','is_active']"

    conn.execute(text(f"""
        CREATE OR REPLACE FUNCTION {func_name}()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                IF OLD."row_hash" IS NOT DISTINCT FROM NEW."row_hash" THEN RETURN NEW; END IF;
                INSERT INTO {history_tbl} ("row_id","source_row_ref","operation","old_data","new_data")
                VALUES (OLD."row_id", COALESCE(NEW."source_row_ref",OLD."source_row_ref"), 'UPDATE',
                    to_jsonb(OLD) - {excl_arr},
                    to_jsonb(NEW) - {excl_arr});
                RETURN NEW;
            ELSIF TG_OP = 'INSERT' THEN
                INSERT INTO {history_tbl} ("row_id","source_row_ref","operation","new_data")
                VALUES (NEW."row_id", NEW."source_row_ref", 'INSERT',
                    to_jsonb(NEW) - {excl_arr});
                RETURN NEW;
            ELSIF TG_OP = 'DELETE' THEN
                INSERT INTO {history_tbl} ("row_id","source_row_ref","operation","old_data")
                VALUES (OLD."row_id", OLD."source_row_ref", 'DELETE',
                    to_jsonb(OLD) - {excl_arr});
                RETURN OLD;
            END IF;
            RETURN NULL;
        END; $$;
    """))

    trig_name = f'trg_{table}_audit'
    conn.execute(text(f'DROP TRIGGER IF EXISTS "{trig_name}" ON {_qualified(schema, table)};'))
    conn.execute(text(f"""
        CREATE TRIGGER "{trig_name}"
        AFTER INSERT OR UPDATE OR DELETE
        ON {_qualified(schema, table)}
        FOR EACH ROW EXECUTE FUNCTION {func_name}();
    """))


# ================================================================
# STAGING  (dùng cho bảng cấp đơn)
# ================================================================

def _prepare_cap_don_stage(conn, schema='public', table='union_all_data_cap_don',
                           stage_schema='staging', stage_table='union_all_data_cap_don'):
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {_qident(stage_schema)}'))
    conn.execute(text(f'DROP TABLE IF EXISTS {_qualified(stage_schema, stage_table)};'))

    business_cols = _business_columns(conn, schema, table)
    if not business_cols:
        raise RuntimeError(f'Không tìm thấy business columns của {schema}.{table}.')

    select_cols = ', '.join(_qident(c) for c in business_cols)
    conn.execute(text(f"""
        CREATE TABLE {_qualified(stage_schema, stage_table)} AS
        SELECT {select_cols}
        FROM {_qualified(schema, table)}
        WHERE FALSE;
    """))
    conn.execute(text(f"""
        ALTER TABLE {_qualified(stage_schema, stage_table)}
        ADD COLUMN "__source_sheet"      TEXT,
        ADD COLUMN "__source_row_number" BIGINT,
        ADD COLUMN "__source_row_ref"    TEXT,
        ADD COLUMN "__ingest_seq"        BIGINT,
        ADD COLUMN "row_hash"            TEXT;
    """))
    return business_cols


def _load_cap_don_stage(df_union, engine,
                        source_name='Danh sách cấp đơn',
                        stage_schema='staging',
                        stage_table='union_all_data_cap_don'):
    with engine.begin() as conn:
        business_cols = _prepare_cap_don_stage(conn,
                            stage_schema=stage_schema, stage_table=stage_table)

    df_stage = df_union.copy()

    if '__source_sheet' not in df_stage.columns:
        df_stage['__source_sheet'] = source_name
    else:
        df_stage['__source_sheet'] = df_stage['__source_sheet'].fillna(source_name)

    if '__source_row_number' not in df_stage.columns:
        df_stage['__source_row_number'] = range(1, len(df_stage) + 1)

    df_stage['__source_row_number'] = pd.to_numeric(
        df_stage['__source_row_number'], errors='raise'
    ).astype('int64')
    df_stage['__source_row_ref'] = (
        df_stage['__source_sheet'].astype(str).str.strip()
        + '::'
        + df_stage['__source_row_number'].astype(str)
    )
    df_stage['__ingest_seq'] = range(1, len(df_stage) + 1)

    load_cols = business_cols + [
        '__source_sheet', '__source_row_number', '__source_row_ref', '__ingest_seq'
    ]
    load_cols = [c for c in load_cols if c in df_stage.columns]

    dup = df_stage['__source_row_ref'][df_stage['__source_row_ref'].duplicated(keep=False)]
    if not dup.empty:
        raise ValueError('Trùng __source_row_ref: ' + ', '.join(map(str, dup.head(10).tolist())))

    df_stage[load_cols].to_sql(
        stage_table, schema=stage_schema, index=False,
        if_exists='append', con=engine, method='multi', chunksize=1000,
    )

    with engine.begin() as conn:
        stage = _qualified(stage_schema, stage_table)
        conn.execute(text(f"""
            UPDATE {stage} AS s
            SET "row_hash" = md5((
                to_jsonb(s) - ARRAY[
                    '__source_sheet','__source_row_number',
                    '__source_row_ref','__ingest_seq','row_hash'
                ]
            )::text);
        """))
        bad = conn.execute(text(f'SELECT COUNT(*) FROM {stage} WHERE "row_hash" IS NULL')).scalar_one()
        if bad:
            raise RuntimeError(f'Không tạo được row_hash cho {bad} dòng stage.')

    return len(df_stage)


def _bootstrap_cap_don_identity(conn,
                                stage_schema='staging', stage_table='union_all_data_cap_don',
                                schema='public',        table='union_all_data_cap_don'):
    master = _qualified(schema, table)
    stage  = _qualified(stage_schema, stage_table)

    master_total    = conn.execute(text(f'SELECT COUNT(*) FROM {master}')).scalar_one()
    master_with_ref = conn.execute(text(f'SELECT COUNT(*) FROM {master} WHERE "source_row_ref" IS NOT NULL')).scalar_one()
    stage_total     = conn.execute(text(f'SELECT COUNT(*) FROM {stage}')).scalar_one()

    if 0 < master_with_ref < master_total:
        raise RuntimeError(
            f'Master ở trạng thái identity dở dang: {master_with_ref}/{master_total} '
            'dòng đã có source_row_ref.'
        )

    if master_with_ref == 0 and master_total > 0:
        if master_total != stage_total:
            raise RuntimeError(
                f'Bootstrap đầu tiên yêu cầu số dòng {schema}.{table} ({master_total}) '
                f'= staging ({stage_total}).'
            )

        conn.execute(text(f"""
            UPDATE {master} AS m
            SET "row_hash" = md5((
                to_jsonb(m) - ARRAY[
                    'row_id','source_sheet','source_row_number',
                    'source_row_ref','row_hash','last_synced_at','is_active'
                ]
            )::text)
            WHERE m."source_row_ref" IS NULL;
        """))

        conn.execute(text(f"""
            WITH s AS (
                SELECT "__source_sheet"      AS source_sheet,
                       "__source_row_number" AS source_row_number,
                       "__source_row_ref"    AS source_row_ref,
                       "row_hash",
                       ROW_NUMBER() OVER (PARTITION BY "row_hash" ORDER BY "__ingest_seq") AS rn
                FROM {stage}
            ),
            m AS (
                SELECT "row_id", "row_hash",
                       ROW_NUMBER() OVER (PARTITION BY "row_hash" ORDER BY "row_id") AS rn
                FROM {master}
                WHERE "source_row_ref" IS NULL
            )
            UPDATE {master} AS target
            SET "source_sheet"      = s.source_sheet,
                "source_row_number" = s.source_row_number,
                "source_row_ref"    = s.source_row_ref,
                "row_hash"          = s.row_hash,
                "last_synced_at"    = NOW(),
                "is_active"         = TRUE
            FROM s
            JOIN m ON m."row_id" = target."row_id"
                 AND m.rn = s.rn
                 AND m."row_hash" = s."row_hash"
            WHERE target."source_row_ref" IS NULL;
        """))

        missing = conn.execute(text(
            f'SELECT COUNT(*) FROM {master} WHERE "source_row_ref" IS NULL'
        )).scalar_one()
        if missing:
            raise RuntimeError(f'Bootstrap thất bại: còn {missing} dòng chưa gắn source_row_ref.')

    elif master_total == 0:
        return


def sync_cap_don(df_union, engine, source_name='Danh sách cấp đơn',
                 schema='public', table='union_all_data_cap_don',
                 stage_schema='staging', stage_table='union_all_data_cap_don'):
    """Stage -> current + audit cho bảng cấp đơn."""
    with engine.begin() as conn:
        _ensure_cap_don_metadata(conn, schema, table)
        _prepare_cap_don_stage(conn, schema, table, stage_schema, stage_table)

    _load_cap_don_stage(df_union, engine, source_name, stage_schema, stage_table)

    with engine.begin() as conn:
        _bootstrap_cap_don_identity(conn, stage_schema, stage_table, schema, table)
        _ensure_cap_don_audit(conn, schema, table)

        master = _qualified(schema, table)
        stage  = _qualified(stage_schema, stage_table)
        business_cols = _business_columns(conn, schema, table)
        col_csv = ', '.join(_qident(c) for c in business_cols)
        set_csv = ', '.join(f'{_qident(c)} = s.{_qident(c)}' for c in business_cols)

        updated = conn.execute(text(f"""
            WITH changed AS (
                SELECT s.*
                FROM {stage} s
                JOIN {master} m ON m."source_row_ref" = s."__source_row_ref"
                WHERE m."row_hash" IS DISTINCT FROM s."row_hash"
            )
            UPDATE {master} AS m
            SET {set_csv},
                "source_sheet"      = s."__source_sheet",
                "source_row_number" = s."__source_row_number",
                "source_row_ref"    = s."__source_row_ref",
                "row_hash"          = s."row_hash",
                "last_synced_at"    = NOW(),
                "is_active"         = TRUE
            FROM changed s
            WHERE m."source_row_ref" = s."__source_row_ref"
            RETURNING m."row_id";
        """)).rowcount

        inserted = conn.execute(text(f"""
            INSERT INTO {master} (
                {col_csv},
                "source_sheet", "source_row_number", "source_row_ref",
                "row_hash", "last_synced_at", "is_active"
            )
            SELECT {', '.join('s.' + _qident(c) for c in business_cols)},
                   s."__source_sheet", s."__source_row_number", s."__source_row_ref",
                   s."row_hash", NOW(), TRUE
            FROM {stage} s
            LEFT JOIN {master} m ON m."source_row_ref" = s."__source_row_ref"
            WHERE m."row_id" IS NULL;
        """)).rowcount

        stage_count   = conn.execute(text(f'SELECT COUNT(*) FROM {stage}')).scalar_one()
        current_count = conn.execute(text(f'SELECT COUNT(*) FROM {master}')).scalar_one()

        return {
            'stage_rows':   stage_count,
            'updated_rows': updated,
            'inserted_rows': inserted,
            'current_rows': current_count,
        }


# ================================================================
# REFRESH TABLE  (dùng cho bảng nhân sự ds_nhan_su_affina)
# ================================================================

def refresh_table_from_stage(df, engine,
                              current_schema='public',
                              current_table='ds_nhan_su_affina',
                              stage_schema='staging',
                              stage_table=None,
                              snapshot_month=None):
    """Nạp bảng nhân sự qua staging. Có snapshot JSONB tháng."""
    if stage_table is None:
        stage_table = current_table + '_stage'

    with engine.begin() as conn:
        if not conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = :schema AND table_name = :table
            )
        """), {'schema': current_schema, 'table': current_table}).scalar_one():
            raise RuntimeError(f'Không tìm thấy bảng {current_schema}.{current_table}.')

        current_cols  = _get_columns(conn, current_schema, current_table)
        business_cols = [c for c in current_cols if c not in TECH_MASTER_COLS]

        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {_qident(stage_schema)}'))
        conn.execute(text(f'DROP TABLE IF EXISTS {_qualified(stage_schema, stage_table)};'))

        select_cols = ', '.join(_qident(c) for c in business_cols)
        conn.execute(text(f"""
            CREATE TABLE {_qualified(stage_schema, stage_table)} AS
            SELECT {select_cols}
            FROM {_qualified(current_schema, current_table)}
            WHERE FALSE;
        """))

    load_cols = [c for c in business_cols if c in df.columns]
    if not load_cols:
        raise RuntimeError(f'DataFrame không có cột phù hợp để nạp {current_schema}.{current_table}.')

    df[load_cols].to_sql(
        stage_table, schema=stage_schema, index=False,
        if_exists='append', con=engine, method='multi', chunksize=1000,
    )

    with engine.begin() as conn:
        current = _qualified(current_schema, current_table)
        stage   = _qualified(stage_schema, stage_table)
        cols_csv       = ', '.join(_qident(c) for c in load_cols)
        stage_cols_csv = ', '.join(f's.{_qident(c)}' for c in load_cols)

        if snapshot_month is None:
            snapshot_month = date.today().replace(day=1)

        conn.execute(text("""
            CREATE SCHEMA IF NOT EXISTS "audit";
            CREATE TABLE IF NOT EXISTS "audit"."ds_nhan_su_affina_snapshot" (
                "snapshot_id"    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                "snapshot_month" DATE NOT NULL,
                "captured_at"    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                "row_data"       JSONB NOT NULL
            );
        """))
        conn.execute(text(f"""
            INSERT INTO "audit"."ds_nhan_su_affina_snapshot" ("snapshot_month", "row_data")
            SELECT :snapshot_month, to_jsonb(s)
            FROM {stage} s;
        """), {'snapshot_month': snapshot_month})

        conn.execute(text(f'DELETE FROM {current};'))
        conn.execute(text(f"""
            INSERT INTO {current} ({cols_csv})
            SELECT {stage_cols_csv} FROM {stage} s;
        """))

        return conn.execute(text(f'SELECT COUNT(*) FROM {current}')).scalar_one()


# ================================================================
# DATA CLEANING UTILS
# ================================================================

def _eval_simple(expr: str):
    """Evaluate simple arithmetic expression (an toàn, không dùng eval)."""
    def _clean_num(s):
        s = s.replace(',', '').replace(' ', '')
        return Decimal(s)

    def _convert(node):
        if isinstance(node, ast.Constant):
            return Decimal(str(node.value))
        elif isinstance(node, ast.BinOp):
            ops = {
                ast.Add: operator.add, ast.Sub: operator.sub,
                ast.Mult: operator.mul, ast.Div: operator.truediv,
            }
            return ops[type(node.op)](_convert(node.left), _convert(node.right))
        elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            return -_convert(node.operand)
        else:
            raise ValueError(f'Không parse được: {ast.dump(node)}')

    try:
        return float(_convert(ast.parse(expr, mode='eval').body))
    except Exception:
        return None


def clean_currency_column(series: pd.Series) -> pd.Series:
    def _clean_one(x):
        if pd.isna(x):
            return None
        s = str(x).strip()
        if s in ('', '-', 'nan', 'None'):
            return None
        s = re.sub(r'\s+', '', s)
        s = s.replace('đ', '').replace('VND', '').replace('VNĐ', '')
        # Phát hiện biểu thức toán học
        if re.search(r'[\+\-\*\/]', s):
            def fix_multi_dot(x: str) -> str:
                parts = x.split('.')
                if len(parts) <= 1:
                    return x
                if len(parts[-1]) != 3:
                    return x.replace('.', '', len(parts) - 2)
                return x.replace('.', '', len(parts) - 1)
            tokens = re.split(r'([\+\-\*\/\(\)])', s)
            new_tokens = []
            for tok in tokens:
                if re.fullmatch(r'[\+\-\*\/\(\)]', tok):
                    new_tokens.append(tok)
                else:
                    new_tokens.append(fix_multi_dot(tok).replace(',', '.'))
            result = _eval_simple(''.join(new_tokens))
            return result
        # Số thường
        if ',' in s and '.' in s:
            if s.index(',') < s.index('.'):
                s = s.replace(',', '')
            else:
                s = s.replace('.', '').replace(',', '.')
        elif ',' in s:
            parts = s.split(',')
            if len(parts) == 2 and len(parts[1]) == 3:
                s = s.replace(',', '')
            else:
                s = s.replace(',', '.')
        elif '.' in s:
            parts = s.split('.')
            if all(len(p) == 3 for p in parts[1:]):
                s = s.replace('.', '')
        try:
            return float(s)
        except ValueError:
            return None

    return series.apply(_clean_one)


def clean_contract(value):
    if pd.isna(value):
        return value
    s = str(value).strip()
    if s.endswith('.0'):
        s = s[:-2]
    return s


def standardize_date_format(date_value):
    if pd.isna(date_value):
        return None
    try:
        if isinstance(date_value, (pd.Timestamp,)):
            return date_value.strftime('%Y-%m-%d')
        s = str(date_value).strip()
        if '/' in s:
            parts = s.split('/')
            if len(parts) == 3:
                d, m, y = parts
                return f"{y.zfill(4)}-{m.zfill(2)}-{d.zfill(2)}"
        parsed = pd.to_datetime(s, dayfirst=True, errors='coerce')
        return parsed.strftime('%Y-%m-%d') if not pd.isna(parsed) else None
    except Exception:
        return None


def fix_inconsistent_dates(date_str):
    try:
        if isinstance(date_str, pd.Timestamp):
            return date_str if date_str.year > 1900 else pd.NaT
        date_str = str(date_str).strip()
        try:
            return pd.to_datetime(date_str, format="%Y-%m-%d %H:%M:%S", errors="raise")
        except Exception:
            pass
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3:
                d, m, y = parts
                if len(y) == 2:
                    y = '20' + y
                return pd.to_datetime(f"{y}-{m.zfill(2)}-{d.zfill(2)}")
        return pd.to_datetime(date_str, dayfirst=True, errors='coerce')
    except Exception:
        return pd.NaT


def clean_whitespace_and_newlines(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace(r'[\n\r]+', ' ', regex=True).str.strip()
    return df


def clean_code_and_name(df, code_cols=('Code sale', 'Code'), name_cols=('Họ tên',)):
    for col in code_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
    for col in name_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    return df


# ================================================================
# LẤY DỮ LIỆU CẤP ĐƠN TỪ GOOGLE SHEET
# ================================================================

def get_all_data():
    """Tải tất cả tab cấp đơn từ Google Sheet và union lại."""

    # --- BHSK ---
    df_BHSK = _read_gsheet(CAPDON_SHEET_ID, CAPDON_SHEET_TABS["Bao hiem suc khoe"],
                            dtype={"Số tiền thanh toán": str},
                            sheet_label="Bao hiem suc khoe")
    df_BHSK['__source_sheet']      = 'Bao hiem suc khoe'
    df_BHSK['__source_row_number'] = df_BHSK.index.to_series().add(2).astype('int64').to_numpy()
    df_BHSK['Số tiền thanh toán']  = clean_currency_column(df_BHSK['Số tiền thanh toán'])
    for col in ['Ngày thanh toán', 'Ngày bắt đầu', 'Ngày kết thúc',
                'Ngày sinh', 'Ngày sinh NMBH', 'Ngày update', 'Ngày duyệt', 'Ngày hoàn phí']:
        if col in df_BHSK.columns:
            df_BHSK[col] = df_BHSK[col].apply(standardize_date_format)
    df_BHSK['Loại bảo hiểm'] = 'BHSK'

    # --- BHOTO ---
    df_BHOTO = _read_gsheet(CAPDON_SHEET_ID, CAPDON_SHEET_TABS["Bao hiem oto"],
                             dtype={"Số tiền": str}, sheet_label="Bao hiem oto")
    if len(df_BHOTO.columns) > 30:
        col_ae = df_BHOTO.columns[30]
        df_BHOTO = df_BHOTO.rename(columns={col_ae: 'Note'})
    else:
        df_BHOTO['Note'] = pd.NA
    df_BHOTO_done = df_BHOTO[['Ngày thanh toán', 'Code sale', 'Chương trình',
                               'Đối tác nhà bảo hiểm', 'Số tiền', 'Channel',
                               'Tên khách hàng', 'Số GCN', 'Biển số',
                               'Ngày bắt đầu hiệu lực', 'Ngày kết thúc hiệu lực',
                               'Số điện thoại', 'email', 'Note']].copy()
    df_BHOTO_done = df_BHOTO_done.rename(columns={
        'Chương trình': 'Sản phẩm', 'Số tiền': 'Số tiền thanh toán',
        'Tên khách hàng': 'Tên Người được BH', 'Số GCN': 'Số hợp đồng',
        'Biển số': 'Biển số xe', 'Ngày bắt đầu hiệu lực': 'Ngày bắt đầu',
        'Ngày kết thúc hiệu lực': 'Ngày kết thúc',
        'Số điện thoại': 'Phone Khách hàng', 'email': 'Email',
    })
    df_BHOTO_done = df_BHOTO_done.loc[:, ~df_BHOTO_done.columns.duplicated()]
    df_BHOTO_done['Tên NMBH']           = df_BHOTO_done['Tên Người được BH']
    df_BHOTO_done['Loại bảo hiểm']      = 'BHOTO'
    df_BHOTO_done['__source_sheet']      = 'Bao hiem oto'
    df_BHOTO_done['__source_row_number'] = df_BHOTO_done.index.to_series().add(2).astype('int64').to_numpy()
    df_BHOTO_done['Số tiền thanh toán']  = clean_currency_column(df_BHOTO_done['Số tiền thanh toán'])
    for col in ['Ngày thanh toán', 'Ngày bắt đầu', 'Ngày kết thúc']:
        if col in df_BHOTO_done.columns:
            df_BHOTO_done[col] = df_BHOTO_done[col].apply(standardize_date_format)

    # --- BHXM (Xe máy) ---
    df_BHXM = _read_gsheet(CAPDON_SHEET_ID, CAPDON_SHEET_TABS["Bao hiem xe may"],
                            sheet_label="Bao hiem xe may")
    # TODO: xử lý tương tự BHOTO, thêm rename columns nếu cần
    df_BHXM['__source_sheet']      = 'Bao hiem xe may'
    df_BHXM['__source_row_number'] = df_BHXM.index.to_series().add(2).astype('int64').to_numpy()
    df_BHXM_done = df_BHXM.copy()

    # --- BHDL (Du lịch) ---
    df_BHDL = _read_gsheet(CAPDON_SHEET_ID, CAPDON_SHEET_TABS["Du lich"],
                            dtype={"Phí bảo hiểm": str}, sheet_label="Du lich")
    df_BHDL = df_BHDL.rename(columns={'Họ Và Tên': 'Tên Người được BH'})
    df_BHDL_done = df_BHDL[['Ngày thanh toán', 'Tên sale', 'Sản phẩm',
                              'Đối tác nhà BH', 'Phí bảo hiểm', 'Channel',
                              'Tên Người được BH', 'Số hợp đồng']].copy()
    df_BHDL_done.columns = ['Ngày thanh toán', 'Code sale', 'Sản phẩm',
                             'Đối tác nhà bảo hiểm', 'Số tiền thanh toán', 'Channel',
                             'Tên Người được BH', 'Số hợp đồng']
    df_BHDL_done['Loại bảo hiểm']      = 'BHDL'
    df_BHDL_done['__source_sheet']      = 'Du lich'
    df_BHDL_done['__source_row_number'] = df_BHDL_done.index.to_series().add(2).astype('int64').to_numpy()
    df_BHDL_done['Số tiền thanh toán']  = clean_currency_column(df_BHDL_done['Số tiền thanh toán'])
    for col in ['Ngày thanh toán']:
        if col in df_BHDL_done.columns:
            df_BHDL_done[col] = df_BHDL_done[col].apply(standardize_date_format)

    # --- TNSP (Trách nhiệm sản phẩm) ---
    df_TNSP = _read_gsheet(CAPDON_SHEET_ID, CAPDON_SHEET_TABS["Trach nhiem san pham"],
                            dtype={"Phí Bảo hiểm": str}, sheet_label="Trach nhiem san pham")
    df_TNSP = df_TNSP.rename(columns={'Nhà Bảo hiểm': 'Đối tác nhà bảo hiểm',
                                      'Phí Bảo hiểm': 'Số tiền thanh toán'})
    df_TNSP['Loại bảo hiểm']      = 'TNDS'
    df_TNSP_done = df_TNSP[['Ngày thanh toán', 'Code sale', 'Sản phẩm',
                              'Đối tác nhà bảo hiểm', 'Số tiền thanh toán',
                              'Channel', 'Loại bảo hiểm']].copy()
    df_TNSP_done['Số tiền thanh toán']  = clean_currency_column(df_TNSP_done['Số tiền thanh toán'])
    df_TNSP_done['__source_sheet']      = 'Trach nhiem san pham'
    df_TNSP_done['__source_row_number'] = df_TNSP_done.index.to_series().add(2).astype('int64').to_numpy()

    # --- BHRR (Bảo hiểm rủi ro) ---
    df_BHRR = _read_gsheet(CAPDON_SHEET_ID, CAPDON_SHEET_TABS["Bao hiem rui ro"],
                            dtype={"Số tiền thanh toán": str}, sheet_label="Bao hiem rui ro")
    df_BHRR = df_BHRR.rename(columns={'Tên khách hàng': 'Tên Người được BH',
                                       'Mã hợp đồng': 'Số hợp đồng'})
    df_BHRR['Loại bảo hiểm'] = 'BHRR'
    df_BHRR_done = df_BHRR[['Ngày thanh toán', 'Code sale', 'Sản phẩm',
                              'Đối tác nhà bảo hiểm', 'Số tiền thanh toán', 'Channel',
                              'Tên Người được BH', 'Số hợp đồng',
                              'Ngày bắt đầu', 'Ngày kết thúc', 'Loại bảo hiểm']].copy()
    df_BHRR_done['Số tiền thanh toán']  = clean_currency_column(df_BHRR_done['Số tiền thanh toán'])
    df_BHRR_done['__source_sheet']      = 'Bao hiem rui ro'
    df_BHRR_done['__source_row_number'] = df_BHRR_done.index.to_series().add(2).astype('int64').to_numpy()

    # --- BHYT/BHXH ---
    df_BHYT = _read_gsheet(CAPDON_SHEET_ID, CAPDON_SHEET_TABS["BHYT BHXH"],
                            sheet_label="BHYT BHXH")
    df_BHYT['__source_sheet']      = 'BHYT BHXH'
    df_BHYT['__source_row_number'] = df_BHYT.index.to_series().add(2).astype('int64').to_numpy()
    df_BHYT['Loại bảo hiểm']       = 'BHYT/BHXH'
    df_BHYT_done = df_BHYT.copy()

    # --- UNION ---
    df_union = pd.concat(
        [df_BHSK, df_BHOTO_done, df_BHXM_done, df_BHDL_done,
         df_TNSP_done, df_BHRR_done, df_BHYT_done],
        axis=0, ignore_index=True
    )
    return df_union


# ================================================================
# LẤY DỮ LIỆU NHÂN SỰ TỪ GOOGLE SHEET
# ================================================================

def convert_columns_channel(row_values_channal, row_values_quan_ly):
    if pd.isna(row_values_channal) or row_values_channal == '':
        return "Core Agency"
    channal = str(row_values_channal).strip()
    channal_lower = channal.lower()
    if channal_lower in ['core agency', 'standard']:
        return "Core Agency"
    if 'tsa' in channal_lower:
        return "CTV_TSA (TSA 2)" if '2' in channal else "TSA"
    if channal == "Elite":     return "Elite"
    if channal == "H.O":       return "H.O"
    if channal == 'Neo':       return "Neo"
    if channal == 'TSA':       return "TSA"
    return "Core Agency"


def get_data_nhan_su_affina():
    """Tải dữ liệu nhân sự từ Google Sheet."""
    df = _read_gsheet(NHANSU_SHEET_ID, NHANSU_SHEET_GID,
                      dtype=str, sheet_label="DSNS AGENCY")
    df = df.dropna(subset=['Họ tên'])
    df['Họ tên'] = df['Họ tên'].str.strip()
    for col in ['QUẢN LÝ CẤP 1 (BDM)', 'QUẢN LÝ CẤP 2 (BDD)', 'Quản lý Cấp 3 (BDH)']:
        if col in df.columns:
            df[col] = df[col].str.replace(r'[\n\r]+', ' ', regex=True).str.strip()
    df = clean_whitespace_and_newlines(df)
    df['Channel'] = df.apply(
        lambda row: convert_columns_channel(
            row.get('Channal', ''), row.get('Quản lý Cấp 3 (BDH)', '')
        ), axis=1
    )
    for col in ['Thời gian bắt đầu', 'Ngày hiệu lực chức danh', 'Ngày Sinh', 'Thời gian kết thúc']:
        if col in df.columns:
            df[col] = df[col].apply(fix_inconsistent_dates)
    for col in ['Điện thoại', 'Code UM', 'Người giới thiệu']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lstrip('0')
    df.columns = df.columns.str.strip()
    return df


# ================================================================
# MAIN
# ================================================================

if __name__ == '__main__':

    # ----------------------------------------------------------
    # 1) SYNC CẤP ĐƠN  →  public.union_all_data_cap_don
    # ----------------------------------------------------------
    print("\n" + "=" * 60)
    print("SYNC CẤP ĐƠN → public.union_all_data_cap_don")
    print("=" * 60)

    df_union = get_all_data()
    rows_before = len(df_union)
    print(f"📊 Số dòng TRƯỚC xử lý: {rows_before}")

    df_union["Số hợp đồng"] = df_union["Số hợp đồng"].apply(clean_contract)

    # BHYT/BHXH: thêm đuôi .0
    mask = (df_union["Loại bảo hiểm"] == "BHYT/BHXH") & (df_union["Số hợp đồng"].notna())
    def add_dot_zero(x):
        x = str(x).strip()
        if x in ('', '-'):  return x
        return x if x.endswith(".0") else x + ".0"
    df_union.loc[mask, "Số hợp đồng"] = df_union.loc[mask, "Số hợp đồng"].apply(add_dot_zero)

    df_union['Ngày thanh toán'] = df_union['Ngày thanh toán'].apply(standardize_date_format)
    df_union = clean_whitespace_and_newlines(df_union)
    for col in ['Ngày bắt đầu', 'Ngày kết thúc', 'Ngày Sinh NNBH', 'Ngày sinh NMBH']:
        if col in df_union.columns:
            df_union[col] = df_union[col].apply(standardize_date_format)

    rows_after = len(df_union)
    print(f"📊 Số dòng SAU xử lý: {rows_after}")
    if rows_before != rows_after:
        print(f"⚠️  CẢNH BÁO: Mất {rows_before - rows_after} dòng!")
    else:
        print(f"✅ Giữ nguyên toàn bộ {rows_after} dòng")

    try:
        result = sync_cap_don(df_union, engine, source_name='Danh sách cấp đơn')
        print('✔ PostgreSQL sync thành công:', result)
    except Exception as e:
        print(f"✖ Lỗi sync cấp đơn: {e}")

    # ----------------------------------------------------------
    # 2) SYNC NHÂN SỰ  →  public.ds_nhan_su_affina
    # ----------------------------------------------------------
    print("\n" + "=" * 60)
    print("SYNC NHÂN SỰ → public.ds_nhan_su_affina")
    print("=" * 60)

    try:
        df_nhan_su = get_data_nhan_su_affina()
        rows_hr = refresh_table_from_stage(
            df_nhan_su,
            engine,
            current_schema='public',
            current_table='ds_nhan_su_affina',
            stage_schema='staging',
            stage_table='ds_nhan_su_affina_stage',
            snapshot_month=None,  # tự lấy tháng hiện tại
        )
        print(f'✔ HR sync thành công. Số dòng current: {rows_hr}')
    except Exception as e:
        print(f"✖ Lỗi sync nhân sự: {e}")

    # ----------------------------------------------------------
    # 3) KIỂM TRA SAU KHI SYNC
    # ----------------------------------------------------------
    print("\n" + "=" * 60)
    print("KIỂM TRA SAU SYNC")
    print("=" * 60)

    with engine.connect() as conn:
        checks = conn.execute(text("""
            SELECT
                (SELECT COUNT(*) FROM public."union_all_data_cap_don")      AS cap_don_rows,
                (SELECT COUNT(*) FROM staging."union_all_data_cap_don")     AS cap_don_stage_rows,
                (SELECT COUNT(DISTINCT "source_row_ref")
                 FROM public."union_all_data_cap_don"
                 WHERE "source_row_ref" IS NOT NULL)                        AS cap_don_distinct_refs,
                (SELECT COUNT(*) FROM audit."union_all_data_cap_don_history") AS cap_don_history_rows,
                (SELECT COUNT(*) FROM public."ds_nhan_su_affina")           AS nhan_su_rows;
        """)).mappings().one()
        print(dict(checks))

        history = conn.execute(text("""
            SELECT "history_id", "row_id", "source_row_ref", "operation", "changed_at"
            FROM audit."union_all_data_cap_don_history"
            ORDER BY "changed_at" DESC, "history_id" DESC
            LIMIT 20;
        """)).mappings().all()

    print("\n20 log cấp đơn gần nhất:")
    for row in history:
        print(dict(row))
