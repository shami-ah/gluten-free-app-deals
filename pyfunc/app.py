from flask import jsonify, Request
from main import main
from datetime import datetime

def get_gluten_free_deals(request: Request):
    """
    HTTP Cloud Function entry point.
    Runs `main()` and returns JSON.
    """
    try:
        deals = main()
        return jsonify({
            "generatedAt": datetime.utcnow().isoformat() + "Z",
            "count": len(deals),
            "deals": deals
        }), 200
    except Exception as e:
        # Return a 500 with the error message
        return jsonify({"error": str(e)}), 500