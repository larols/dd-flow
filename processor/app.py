from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

@app.route("/process", methods=["POST"])
def process_netflow():
    """Receives NetFlow data and logs it."""
    try:
        netflow_data = request.get_json()
        if not netflow_data:
            return jsonify({"error": "Invalid NetFlow data"}), 400
        
        # Log received data
        app.logger.info(f"Received NetFlow data: {netflow_data}")

        # Placeholder for processing logic
        processed_data = {"message": "NetFlow data processed successfully"}

        return jsonify(processed_data), 200
    except Exception as e:
        app.logger.error(f"Error processing NetFlow data: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
