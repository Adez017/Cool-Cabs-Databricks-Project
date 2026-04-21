import requests
import pyodbc
import time
from datetime import datetime
 
# ======================================
# CONFIG
# ======================================
BASE_URL = "https://nsp-v3.europe.naviscloudops.com/inventory/units"
TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJjMS1tZ0s4UV9jLVJfTVZfZnlqNFBDanBuVlVDX3VtVENXQnAzZTRFV2hZIn0.eyJleHAiOjE3NzY0NDExNzAsImlhdCI6MTc3NjQyMzE3MCwianRpIjoiZjlmNTg2M2MtOThiYS00NDA4LTgxODMtMzVmZmViNTczYWIzIiwiaXNzIjoiaHR0cHM6Ly9hdXRoLmV1cm9wZS5uYXZpc2Nsb3Vkb3BzLmNvbS9hdXRoL3JlYWxtcy9uc3AiLCJzdWIiOiIyNTE0MzZlNy1kMTUyLTQ0MzEtYTY3ZC1mYWM1OWFjYmIyOTIiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJuc3AtYWRhbml0cGEyLXByb2QiLCJzZXNzaW9uX3N0YXRlIjoiMWM4YmU5MzAtNDczNy00ODMyLTk4ZTEtZmY2OTMyOGNkMzU4IiwiYWNyIjoiMSIsInNjb3BlIjoibnNwLlBST0QtVFBBMl9UUEEyLmFsbC5hbGwiLCJjbGllbnRIb3N0IjoiMTIyLjE3OS4xNTQuMjQxOjM4MTYwIiwiY2xpZW50SWQiOiJuc3AtYWRhbml0cGEyLXByb2QiLCJjbGllbnRBZGRyZXNzIjoiMTIyLjE3OS4xNTQuMjQxOjM4MTYwIn0.gfC9WQwrqCdogys8u38cJ_jYkVRN7QSZHC9Zlyqziaez25zyw0ys-N9ONNmK3P4JF4XRt1Djr0Cmg3dPGx37aEzK2Gsbn_hZnAcOGPtpBGaNMGtUtRfvjwjpx2bpJxHsvzukkOE8Hjwy5fyV9_jwrXj30XeAm4_4KwBXJVyou319DIvFFZfEhTM2zgXeOYo1mHrgCDv05Tec88o7hvnPWqv1p1FXczFIL47PQeSB9UQdjxkLXk6x5J_SjSTjTYYfYgqtLjfRyHcHlVxgMJWx7txOGLu3K-q3KjPkl3HUWSCgpsiEhbDXkSPfB8o-mObcp1zFnp9dvnynDuLk5Ib-qw"
 
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {TOKEN}"
}
 
PARAMS = {
    "operator": "TPA2",
    "predicateStr": "",
    "page": 0,
    "size": 100
}
 
SERVER = "ADITYA\SQLEXPRESS"
DATABASE = "teagtlvolume"
DRIVER = "ODBC Driver 17 for SQL Server"
 
CLEAR_OLD_DATA = True
 
# ======================================
# DB CONNECTION
# ======================================
def get_connection(db_name="master"):
    return pyodbc.connect(
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={db_name};"
        f"Trusted_Connection=yes;"
        f"TrustServerCertificate=yes;"
    )
 
# ======================================
# CREATE DATABASE
# ======================================
def create_database():
    conn = get_connection("master")
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"""
    IF DB_ID('{DATABASE}') IS NULL
        CREATE DATABASE [{DATABASE}]
    """)
    cursor.close()
    conn.close()
    print(f"Database '{DATABASE}' ready")
 
# ======================================
# CREATE TABLE
# ======================================
def create_tables(conn):
    cursor = conn.cursor()
 
    cursor.execute("""
    IF OBJECT_ID('dbo.inventory_units', 'U') IS NULL
    CREATE TABLE dbo.inventory_units (
        unitGkey BIGINT NULL,
        ufvGkey BIGINT NULL,
        unitId NVARCHAR(100) NULL,
        category NVARCHAR(50) NULL,
        freightKind NVARCHAR(50) NULL,
        transitState NVARCHAR(100) NULL,
        visitState NVARCHAR(100) NULL,
 
        actualIbVisitId NVARCHAR(100) NULL,
        actualIbCarrierMode NVARCHAR(50) NULL,
        actualIbCvOperatorId NVARCHAR(50) NULL,
 
        actualObVisitId NVARCHAR(100) NULL,
        actualObCarrierMode NVARCHAR(50) NULL,
        actualObCvOperatorId NVARCHAR(50) NULL,
 
        eqGkey BIGINT NULL,
        eqtypeId NVARCHAR(50) NULL,
        eqtypeGkey BIGINT NULL,
        eqClass NVARCHAR(50) NULL,
        isoGroup NVARCHAR(50) NULL,
        line NVARCHAR(50) NULL,
        role NVARCHAR(50) NULL,
 
        requiresPower BIT NULL,
        isOog BIT NULL,
        isHazardous BIT NULL,
 
        nominalLength NVARCHAR(50) NULL,
        nominalHeight NVARCHAR(50) NULL,
        restowType NVARCHAR(50) NULL,
 
        lastKnownPosLocType NVARCHAR(50) NULL,
        lastKnownPosLocId NVARCHAR(100) NULL,
        lastKnownPosName NVARCHAR(255) NULL,
        lastKnownPosBin BIGINT NULL,
        lastKnownPosTier INT NULL,
 
        declaredIbVisitId NVARCHAR(100) NULL,
        declaredObVisitId NVARCHAR(100) NULL,
        intendedObVisitId NVARCHAR(100) NULL,
        routingActualIbVisitId NVARCHAR(100) NULL,
        routingActualObVisitId NVARCHAR(100) NULL,
        pod1Id NVARCHAR(50) NULL,
        pod2Id NVARCHAR(50) NULL,
 
        goodsAndCtrWtKg FLOAT NULL,
        grossWeightSource NVARCHAR(50) NULL,
 
        destination NVARCHAR(100) NULL,
        blNbr NVARCHAR(100) NULL,
        deckRqmnt NVARCHAR(50) NULL,
        isBundle BIT NULL,
        isCtrSealed BIT NULL,
 
        sealNbr1 NVARCHAR(100) NULL,
        sealNbr2 NVARCHAR(100) NULL,
        sealNbr3 NVARCHAR(100) NULL,
        sealNbr4 NVARCHAR(100) NULL,
 
        createTime DATETIME NULL,
        timeLastStateChange DATETIME NULL,
        changed DATETIME NULL,
 
        eqOperatorId NVARCHAR(50) NULL,
        eqOwnerId NVARCHAR(50) NULL,
        damageSeverity NVARCHAR(50) NULL,
        isFolded BIT NULL,
        isReserved BIT NULL,
 
        unitFlexString01 NVARCHAR(255) NULL,
        unitFlexString02 NVARCHAR(255) NULL,
        unitFlexString10 NVARCHAR(255) NULL,
        unitFlexString11 NVARCHAR(255) NULL,
 
        ufvFlexString10 NVARCHAR(255) NULL,
 
        timeOfLastMove DATETIME NULL,
 
        facility NVARCHAR(50) NULL,
        scope_facility_id NVARCHAR(50) NULL,
        scope_yard_id NVARCHAR(50) NULL
    )
    """)
 
    conn.commit()
    cursor.close()
    print("Table 'inventory_units' ready")
 
# ======================================
# CLEAR OLD DATA
# ======================================
def clear_old_data(conn):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dbo.inventory_units")
    conn.commit()
    cursor.close()
    print("Old data cleared")
 
# ======================================
# HELPERS
# ======================================
def parse_epoch_ms(value):
    if value in (None, "", "null"):
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000)
        if isinstance(value, str) and value.isdigit():
            return datetime.fromtimestamp(int(value) / 1000)
    except Exception:
        return None
    return None
 
def extract_records_from_response(data):
    if isinstance(data, dict) and isinstance(data.get("content"), list):
        return data["content"]
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        return data["data"]
    if isinstance(data, list):
        return data
    return []
 
def get_nested(dct, *keys):
    current = dct
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current
 
def to_bit(value):
    if value is None:
        return None
    return 1 if bool(value) else 0
 
# ======================================
# FETCH DATA
# ======================================
def fetch_data():
    all_records = []
    page = 0
 
    while True:
        PARAMS["page"] = page
        print(f"Fetching page {page}...")
 
        try:
            res = requests.get(BASE_URL, headers=HEADERS, params=PARAMS, timeout=60)
        except Exception as e:
            print("Request failed:", e)
            break
 
        if res.status_code != 200:
            print("API Error:", res.status_code, res.text)
            break
 
        try:
            data = res.json()
        except Exception as e:
            print("Invalid JSON:", e)
            break
 
        records = extract_records_from_response(data)
 
        if not records:
            print("No more data")
            break
 
        all_records.extend(records)
        print(f"Fetched {len(records)} records")
 
        if len(records) < PARAMS["size"]:
            break
 
        page += 1
        if page == 5:
            break
        time.sleep(0.2)
 
    return all_records
 
# ======================================
# MAP ONE UNIT RECORD
# ======================================
def extract_unit(record):
    return (
        record.get("unitGkey"),
        record.get("ufvGkey"),
        record.get("unitId"),
        record.get("category"),
        record.get("freightKind"),
        record.get("transitState"),
        record.get("visitState"),
 
        get_nested(record, "actualIbVisit", "visitId"),
        get_nested(record, "actualIbVisit", "carrierMode"),
        record.get("actualIbCvOperatorId"),
 
        get_nested(record, "actualObVisit", "visitId"),
        get_nested(record, "actualObVisit", "carrierMode"),
        record.get("actualObCvOperatorId"),
 
        record.get("eqGkey"),
        record.get("eqtypeId"),
        record.get("eqtypeGkey"),
        record.get("eqClass"),
        record.get("isoGroup"),
        record.get("line"),
        record.get("role"),
 
        to_bit(record.get("requiresPower")),
        to_bit(record.get("isOog")),
        to_bit(record.get("isHazardous")),
 
        record.get("nominalLength"),
        record.get("nominalHeight"),
        record.get("restowType"),
 
        get_nested(record, "lastKnownPosition", "posLocType"),
        get_nested(record, "lastKnownPosition", "posLocId"),
        get_nested(record, "lastKnownPosition", "posName"),
        get_nested(record, "lastKnownPosition", "posBin"),
        get_nested(record, "lastKnownPosition", "posTier"),
 
        get_nested(record, "routing", "declaredIbVisit", "visitId"),
        get_nested(record, "routing", "declaredObVisit", "visitId"),
        get_nested(record, "routing", "intendedObVisit", "visitId"),
        get_nested(record, "routing", "actualIbVisit", "visitId"),
        get_nested(record, "routing", "actualObVisit", "visitId"),
        get_nested(record, "routing", "pod1Id"),
        get_nested(record, "routing", "pod2Id"),
 
        get_nested(record, "contents", "goodsAndCtrWtKg"),
        get_nested(record, "contents", "grossWeightSource"),
 
        record.get("destination"),
        record.get("blNbr"),
        record.get("deckRqmnt"),
        to_bit(record.get("isBundle")),
        to_bit(record.get("isCtrSealed")),
 
        get_nested(record, "seals", "sealNbr1"),
        get_nested(record, "seals", "sealNbr2"),
        get_nested(record, "seals", "sealNbr3"),
        get_nested(record, "seals", "sealNbr4"),
 
        parse_epoch_ms(record.get("createTime")),
        parse_epoch_ms(record.get("timeLastStateChange")),
        parse_epoch_ms(record.get("changed")),
 
        record.get("eqOperatorId"),
        record.get("eqOwnerId"),
        record.get("damageSeverity"),
        to_bit(record.get("isFolded")),
        to_bit(record.get("isReserved")),
 
        get_nested(record, "unitFlex", "unitFlexString01"),
        get_nested(record, "unitFlex", "unitFlexString02"),
        get_nested(record, "unitFlex", "unitFlexString10"),
        get_nested(record, "unitFlex", "unitFlexString11"),
 
        get_nested(record, "ufvFlex", "flexString10"),
 
        parse_epoch_ms(get_nested(record, "timestamps", "timeOfLastMove")),
 
        record.get("facility"),
        get_nested(record, "scope", "facility_id"),
        get_nested(record, "scope", "yard_id")
    )
 
# ======================================
# INSERT DATA
# ======================================
def insert_data(conn, records):
    cursor = conn.cursor()
 
    insert_sql = """
    INSERT INTO dbo.inventory_units (
        unitGkey, ufvGkey, unitId, category, freightKind, transitState, visitState,
        actualIbVisitId, actualIbCarrierMode, actualIbCvOperatorId,
        actualObVisitId, actualObCarrierMode, actualObCvOperatorId,
        eqGkey, eqtypeId, eqtypeGkey, eqClass, isoGroup, line, role,
        requiresPower, isOog, isHazardous,
        nominalLength, nominalHeight, restowType,
        lastKnownPosLocType, lastKnownPosLocId, lastKnownPosName, lastKnownPosBin, lastKnownPosTier,
        declaredIbVisitId, declaredObVisitId, intendedObVisitId, routingActualIbVisitId, routingActualObVisitId, pod1Id, pod2Id,
        goodsAndCtrWtKg, grossWeightSource,
        destination, blNbr, deckRqmnt, isBundle, isCtrSealed,
        sealNbr1, sealNbr2, sealNbr3, sealNbr4,
        createTime, timeLastStateChange, changed,
        eqOperatorId, eqOwnerId, damageSeverity, isFolded, isReserved,
        unitFlexString01, unitFlexString02, unitFlexString10, unitFlexString11,
        ufvFlexString10,
        timeOfLastMove,
        facility, scope_facility_id, scope_yard_id
    )
    VALUES (
        ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?,
        ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?,
        ?,
        ?, ?, ?
    )
    """
 
    for r in records:
        cursor.execute(insert_sql, extract_unit(r))
 
    conn.commit()
    cursor.close()
    print("Data inserted successfully")
 
# ======================================
# MAIN
# ======================================
def main():
    try:
        create_database()
 
        conn = get_connection(DATABASE)
        print(f"Connected to database '{DATABASE}'")
 
        create_tables(conn)
 
        if CLEAR_OLD_DATA:
            clear_old_data(conn)
 
        records = fetch_data()
        print(f"Total fetched: {len(records)}")
 
        if records:
            first_row = extract_unit(records[0])
            print("First mapped row:", first_row)
            insert_data(conn, records)
        else:
            print("No records to insert")
 
        conn.close()
        print("Done")
 
    except Exception as e:
        print("Error:", e)
 
if __name__ == "__main__":
    main()