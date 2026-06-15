"""Compatibility entrypoint.

The web dashboard now lives in `dashboard.py`. This file keeps older imports
and local commands that expect `app.py` working.
"""

from dashboard import app


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
