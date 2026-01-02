"""
Celery tasks for Poolboy resource management.

Task modules follow the same naming convention as the main Poolboy modules:
- resourcepool.py
- resourceclaim.py
- resourcehandle.py
- resourceprovider.py
- resourcewatch.py
- cleanup.py
"""

from . import resourcepool

# Placeholder imports for other task types (to be implemented in future phases)
try:
    from . import resourceclaim
except ImportError:
    pass
try:
    from . import resourcehandle
except ImportError:
    pass
try:
    from . import resourceprovider
except ImportError:
    pass
try:
    from . import resourcewatch
except ImportError:
    pass
try:
    from . import cleanup
except ImportError:
    pass
