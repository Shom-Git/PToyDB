#!/usr/bin/env python3
"""Clean up database files."""

import shutil
import os

dirs_to_clean = ['./data', './wal']

for dir_path in dirs_to_clean:
    if os.path.exists(dir_path):
        print(f"Removing {dir_path}...")
        shutil.rmtree(dir_path)
        print(f"  ✓ Cleaned {dir_path}")
    else:
        print(f"  - {dir_path} doesn't exist, skipping")

print("\n✓ Database cleaned!")
