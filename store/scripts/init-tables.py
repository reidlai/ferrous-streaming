#!/usr/bin/env python3
"""Wrapper to allow invoking init-tables.py with hyphenated name."""
from init_tables import main

if __name__ == "__main__":
    import sys
    sys.exit(main())
