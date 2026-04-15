"""
run_dart.py
T-1 전 영업일 특별 공시만 빠르게 확인

사용:
  python run_dart.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import cmd_dart_only

if __name__ == "__main__":
    cmd_dart_only()
