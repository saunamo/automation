import json

def handler(event, context):
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps({'status': 'ok', 'message': 'Pipedrive-Katana sync is running'})
    }
