# scripts/tests/conftest.py
import sys
from pathlib import Path

# Add scripts dir to path so imports work
sys.path.insert(0, str(Path(__file__).parent.parent))
