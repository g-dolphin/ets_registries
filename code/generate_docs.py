"""Generate documentation pages derived from the codebase.

Usage:
  python scripts/generate_docs.py
"""

from pathlib import Path
import sys

# Ensure repo_root/code is on PYTHONPATH so `import registry_processing` works
CODE_DIR = Path(__file__).resolve().parent  # .../ets_registries/code
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))
    
from docsgen import generate_all


if __name__ == "__main__":
    generate_all(Path(__file__).resolve().parents[1])
