import os
import re
import base64
import sqlite3
from pathlib import Path
import pandas as pd
import msoffcrypto
import io
import argparse

def process_excel_files(data_directory, output_db, password):
    """
    Process all Excel .ci files and migrate data to SQLite database
    
    Args:
        data_directory (str): Directory containing .ci files
        output_db (str): Path to output SQLite database
        password (str): Password for encrypted Excel files
    """
    # Create/connect to SQLite database
    conn = sqlite3.connect(output_db)
    
    # Create tables
    create_tables(conn)
    
    # Get all .ci files
    ci_files = list(Path(data_directory).glob("*.ci"))
    total_files = len(ci_files)
    
    print(f"Found {total_files} .ci files to process")
    
    # Process each file
    for i, file_path in enumerate(ci_files, 1):
        try:
            filename = file_path.stem
            # Parse make and ECM from filename using regex pattern
            match = re.match(r"([^-]+)\s+-\s+(.+)", filename)
            
            if not match:
                print(f"Skipping file with invalid format: {filename}")
                continue
                
            make = match.group(1).strip()
            ecm = match.group(2).strip()
            
            print(f"Processing file {i}/{total_files}: {make} - {ecm}")
            
            # Process the Excel file and insert data
            process_file(conn, file_path, make, ecm, password)
            
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    # Create indices for better query performance
    create_indices(conn)
    
    # Commit and close connection
    conn.commit()
    conn.close()
    print(f"Migration completed. Database saved to {output_db}")

def create_tables(conn):
    """Create the necessary tables in the SQLite database"""
    cursor = conn.cursor()
    
    # Makes table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS makes (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )
    ''')
    
    # ECMs table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ecms (
        id INTEGER PRIMARY KEY,
        make_id INTEGER,
        name TEXT,
        UNIQUE(make_id, name),
        FOREIGN KEY(make_id) REFERENCES makes(id)
    )
    ''')
    
    # DTC Info table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dtc_info (
        id INTEGER PRIMARY KEY,
        make_id INTEGER,
        ecm_id INTEGER,
        fault_code TEXT,
        pid TEXT,
        spn TEXT,
        fmi TEXT,
        summary TEXT,
        description TEXT,
        FOREIGN KEY(make_id) REFERENCES makes(id),
        FOREIGN KEY(ecm_id) REFERENCES ecms(id)
    )
    ''')
    
    # Collections table for collection descriptions
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS collections (
        id INTEGER PRIMARY KEY,
        make_id INTEGER,
        ecm_id INTEGER,
        description TEXT,
        FOREIGN KEY(make_id) REFERENCES makes(id),
        FOREIGN KEY(ecm_id) REFERENCES ecms(id)
    )
    ''')
    
    conn.commit()

def create_indices(conn):
    """Create indices for better query performance"""
    cursor = conn.cursor()
    
    # Create indices for common lookup patterns
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_dtc_make_ecm ON dtc_info(make_id, ecm_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_dtc_fault_code ON dtc_info(fault_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ecm_make ON ecms(make_id)')
    
    conn.commit()

def get_or_create_make(conn, make_name):
    """Get make ID or create if it doesn't exist"""
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM makes WHERE name = ?', (make_name,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    
    cursor.execute('INSERT INTO makes (name) VALUES (?)', (make_name,))
    conn.commit()
    return cursor.lastrowid

def get_or_create_ecm(conn, make_id, ecm_name):
    """Get ECM ID or create if it doesn't exist"""
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM ecms WHERE make_id = ? AND name = ?', (make_id, ecm_name))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    
    cursor.execute('INSERT INTO ecms (make_id, name) VALUES (?, ?)', (make_id, ecm_name))
    conn.commit()
    return cursor.lastrowid

def format_display_string(text):
    """Format display text similar to the C# code"""
    if text is None:
        return ""
    # Replace multiple spaces, newlines with a single space
    return re.sub(r' *(\r\n|\n|  )', ' ', str(text)).strip()

def decrypt_excel_file(file_path, password):
    """Decrypt the Excel file and return a file-like object"""
    temp_file = io.BytesIO()
    
    with open(file_path, 'rb') as f:
        excel_file = msoffcrypto.OfficeFile(f)
        excel_file.load_key(password=password)
        excel_file.decrypt(temp_file)
    
    temp_file.seek(0)
    return temp_file

def process_file(conn, file_path, make, ecm, password):
    """Process a single Excel file and insert the data into SQLite"""
    
    # Decrypt the Excel file
    decrypted_file = decrypt_excel_file(file_path, password)
    
    # Read the Excel file into a DataFrame
    try:
        # First try to read using pandas which handles most Excel files
        df = pd.read_excel(decrypted_file, sheet_name=0, header=None)
        
        # Get collection description from cell A1
        collection_description = str(df.iloc[0, 0]) if pd.notna(df.iloc[0, 0]) else ""
        
        # Get make and ECM IDs
        make_id = get_or_create_make(conn, make)
        ecm_id = get_or_create_ecm(conn, make_id, ecm)
        
        # Store collection description
        cursor = conn.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO collections (make_id, ecm_id, description) VALUES (?, ?, ?)',
            (make_id, ecm_id, collection_description)
        )
        
        # Skip header row and process data
        dtc_records = []
        
        for _, row in df.iloc[1:].iterrows():
            fault_code = format_display_string(row[0])
            pid = format_display_string(row[1])
            spn = format_display_string(row[2])
            fmi = format_display_string(row[3])
            summary = format_display_string(row[4])
            description = str(row[5]).strip() if pd.notna(row[5]) else ""
            
            dtc_records.append((
                make_id, ecm_id, fault_code, pid, spn, fmi, summary, description
            ))
        
        # Batch insert records
        cursor.executemany(
            '''INSERT INTO dtc_info 
               (make_id, ecm_id, fault_code, pid, spn, fmi, summary, description) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
            dtc_records
        )
        
        conn.commit()
        print(f"Successfully imported {len(dtc_records)} records for {make} - {ecm}")
        
    except Exception as e:
        print(f"Error reading Excel data for {make} - {ecm}: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Migrate DTC Excel files to SQLite database')
    parser.add_argument('--data-dir', required=True, help='Directory containing .ci files')
    parser.add_argument('--output', default='dtc_database.db', help='Output SQLite database file')
    parser.add_argument('--password', default=None, help='Password for encrypted Excel files')
    
    args = parser.parse_args()
    
    # If password not provided, use the one from the C# code
    if args.password is None:
        encoded_password = ""  # From the C# code
        args.password = base64.b64decode(encoded_password).decode('utf-8')
    
    process_excel_files(args.data_dir, args.output, args.password)

if __name__ == "__main__":
    main()
