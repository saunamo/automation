"""
Health check endpoint for Netlify
"""
import json

def handler(event, context):
    """Health check handler"""
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({'status': 'ok', 'message': 'Pipedrive-Katana sync is running'})
    }
