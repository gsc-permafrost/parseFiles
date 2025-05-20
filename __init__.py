#parsefiles/__init__.py
try:
    from . import parseCSI
    from . import parseCSV
except:
    import parseCSI
    import parseCSV