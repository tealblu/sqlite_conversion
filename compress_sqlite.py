import gzip
import shutil
import sys
import sqlite3
import re

def clean_int(value: str) -> int:
    """Converts a string to an integer by removing non-numeric characters and decimals."""
    value = re.sub(r'[^0-9]', '', value.split('.')[0])
    return int(value) if value else -1

def compress_sqlite_db(db_path: str, output_path: str = None):
    """Compresses a SQLite database file using gzip after modifying data."""
    if output_path is None:
        output_path = db_path + ".gz"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Remove dtc_info description field
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dtc_info_temp AS
        SELECT id, make_id, ecm_id, fault_code, spn, fmi, pid, summary FROM dtc_info;
    """
    )
    cursor.execute("DROP TABLE dtc_info;")
    cursor.execute("ALTER TABLE dtc_info_temp RENAME TO dtc_info;")
    
    # Convert spn, fmi, pid fields to integers
    cursor.execute("SELECT id, spn, fmi, pid FROM dtc_info;")
    rows = cursor.fetchall()
    for row in rows:
        print("Editing row ", row)
        id, spn, fmi, pid = row
        spn = clean_int(spn)
        fmi = clean_int(fmi)
        pid = clean_int(pid)
        cursor.execute("UPDATE dtc_info SET spn=?, fmi=?, pid=? WHERE id=?;", (spn, fmi, pid, id))
    
    conn.commit()
    conn.close()
    
    with open(db_path, 'rb') as f_in, gzip.open(output_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    
    print(f"Compressed {db_path} to {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python compress_sqlite.py <database_file> [output_file]")
        sys.exit(1)
    
    db_file = sys.argv[1]
    out_file = sys.argv[2] if len(sys.argv) > 2 else None
    compress_sqlite_db(db_file, out_file)