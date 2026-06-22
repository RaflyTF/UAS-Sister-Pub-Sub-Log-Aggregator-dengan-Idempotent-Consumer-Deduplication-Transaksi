"""
Root conftest.py
Menambahkan root direktori ke sys.path agar pytest dapat menemukan
package 'aggregator' saat menjalankan test dari direktori mana saja.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
