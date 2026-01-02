#!/usr/bin/env python3
"""
Worker entry point for Poolboy.

This script resolves the naming conflict between the 'operator' directory
and Python's stdlib 'operator' module by:
1. Starting from a neutral directory (not /opt/app-root/operator)
2. Importing Celery FIRST (before our code is in sys.path)
3. Adding our operator directory to sys.path
4. Then loading our Celery app

Usage:
    python poolboy_worker.py worker --loglevel=info
    python poolboy_worker.py beat --loglevel=info
"""
import os
import sys

# Ensure we're not importing from operator directory initially
# This allows Celery and its dependencies to load without conflict
operator_path = '/opt/app-root/operator'
if operator_path in sys.path:
    sys.path.remove(operator_path)

# Now import Celery (and all stdlib dependencies like 'operator' module)
from celery.__main__ import main as celery_main

# Add our operator directory to path for our app imports
sys.path.insert(0, operator_path)

# Change to operator directory for relative imports in our code
os.chdir(operator_path)

if __name__ == '__main__':
    celery_main()

