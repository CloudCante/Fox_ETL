#!/usr/bin/env python3
"""
Convert XLS files to XLSX format using LibreOffice and reorganize into expected directory structure.
"""
import os
import subprocess
from pathlib import Path
import sys

def find_soffice():
    """Find LibreOffice executable on Windows"""
    possible_paths = [
        r"C:\Program Files\LibreOffice\program\soffice.exe",
        r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return path
            
    return None

def ensure_dir(directory):
    """Create directory if it doesn't exist"""
    Path(directory).mkdir(parents=True, exist_ok=True)

def convert_using_libreoffice(input_file, output_file):
    """Convert file using LibreOffice in headless mode"""
    output_dir = os.path.dirname(output_file)
    
    # Ensure output directory exists
    ensure_dir(output_dir)
    
    # Find LibreOffice executable
    soffice_path = find_soffice()
    if not soffice_path:
        print("❌ Error: LibreOffice not found! Please install LibreOffice.")
        sys.exit(1)
    
    # Build LibreOffice command
    cmd = [
        soffice_path,
        '--headless',
        '--convert-to', 'xlsx',
        '--outdir', output_dir,
        input_file
    ]
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"LibreOffice conversion error: {e.stderr}")
        return False

def convert_and_organize_files():
    # Base paths - use raw strings for Windows paths
    base_dir = os.path.abspath(os.path.join(os.getcwd(), "input", "data log"))
    
    # Conversion mappings
    conversions = [
        {
            'source_dir': os.path.join(base_dir, "testboardrecord"),
            'target_dir': os.path.join(base_dir, "testboardrecord_xlsx"),
            'file_prefix': 'test_board_record_report'
        },
        {
            'source_dir': os.path.join(base_dir, "workstationreport"),
            'target_dir': os.path.join(base_dir, "workstationreport_xlsx"),
            'file_prefix': 'workstationOutputReport'
        }
    ]
    
    total_converted = 0
    
    for conv in conversions:
        source_dir = conv['source_dir']
        print(f"\n🔄 Processing files from {source_dir}")
        
        if not os.path.exists(source_dir):
            print(f"❌ Source directory not found: {source_dir}")
            continue
            
        # Walk through the source directory
        for root, _, files in os.walk(source_dir):
            for file in files:
                if file.endswith('.xls'):
                    # Get month folder name from source path
                    rel_path = os.path.relpath(root, source_dir)
                    
                    # Create corresponding output directory
                    output_dir = os.path.join(conv['target_dir'], rel_path)
                    ensure_dir(output_dir)
                    
                    # Setup input and output paths
                    input_file = os.path.join(root, file)
                    output_file = os.path.join(output_dir, file)
                    
                    try:
                        print(f"Converting: {file}")
                        if convert_using_libreoffice(input_file, output_file):
                            total_converted += 1
                            print(f"  ✅ Saved to: {output_file.replace('.xls', '.xlsx')}")
                        else:
                            print(f"  ❌ Failed to convert {file}")
                    except Exception as e:
                        print(f"  ❌ Error converting {file}: {e}")
                        continue

    print(f"\n📊 Total files converted: {total_converted}")

if __name__ == "__main__":
    print("🚀 Starting XLS to XLSX conversion using LibreOffice...")
    convert_and_organize_files() 