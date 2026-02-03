import json
import os
from datetime import datetime, timedelta

try:
    import requests
except ImportError:
    requests = None

KATANA_API_KEY = os.environ.get('KATANA_API_KEY', '7cf78308-1e55-4d88-9c68-e06e8dc093fa')
KATANA_BASE_URL = 'https://api.katanamrp.com/v1'
PIPEDRIVE_API_TOKEN = os.environ.get('PIPEDRIVE_API_TOKEN', '3c66a18f3cf4e8f5564c1134611c3fc834985ecf')
PIPEDRIVE_COMPANY_DOMAIN = os.environ.get('PIPEDRIVE_COMPANY_DOMAIN', 'saunamo')
SKU_FIELD_KEY = '43a32efde94b5e07af24690d5b8db5dc18f5680a'

DEFAULT_LOCATION_ID = 166154
DEFAULT_TAX_RATE_ID_EUR = 423653
DEFAULT_TAX_RATE_ID_GBP = 459884
CUSTOM_ITEM_VARIANT_ID = 38207669

def katana_request(endpoint, method='GET', data=None, params=None):
    if not requests:
        return None, 'requests module not available'
    url = f'{KATANA_BASE_URL}/{endpoint}'
    headers = {'Authorization': f'Bearer {KATANA_API_KEY}', 'Content-Type': 'application/json'}
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, params=params, timeout=30)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=data, timeout=30)
        else:
            return None, f'Unsupported method: {method}'
        if response.status_code in [200, 201]:
            return response.json(), None
        return None, f'{response.status_code}: {response.text[:500]}'
    except Exception as e:
        return None, str(e)

def pipedrive_request(endpoint, params=None):
    if not requests:
        return {}
    url = f'https://{PIPEDRIVE_COMPANY_DOMAIN}.pipedrive.com/api/v1/{endpoint}'
    if params is None:
        params = {}
    params['api_token'] = PIPEDRIVE_API_TOKEN
    response = requests.get(url, params=params)
    return response.json()

def find_katana_order_by_number(order_no):
    result, error = katana_request('sales_orders', params={'limit': 1000})
    if error:
        return None
    orders = result.get('data', []) or result.get('results', [])
    for order in orders:
        if str(order.get('order_no', '')).strip() == str(order_no).strip():
            return order
    return None

def find_or_create_customer(name, email=None):
    result, error = katana_request('customers', params={'limit': 1000})
    if not error and result:
        customers = result.get('data', []) or result.get('results', [])
        for customer in customers:
            if customer.get('name', '').strip().upper() == name.strip().upper():
                return customer
    customer_data = {'name': name, 'email': email or '', 'currency': 'EUR'}
    result, error = katana_request('customers', method='POST', data=customer_data)
    if error:
        return None
    return result.get('data', {}) if isinstance(result, dict) else result

def find_variant_by_sku(sku):
    if not sku:
        return None
    result, error = katana_request('variants', params={'limit': 1000})
    if error:
        return None
    variants = result.get('data', []) or result.get('results', [])
    for variant in variants:
        if variant.get('sku', '').upper().strip() == sku.upper().strip():
            return variant
    return None

def find_or_create_product(product_name, sku):
    result, error = katana_request('products', params={'limit': 1000})
    if not error and result:
        products = result.get('data', []) or result.get('results', [])
        for product in products:
            if product.get('name', '').strip().upper() == product_name.strip().upper():
                return product
    product_data = {'name': product_name, 'code': sku, 'unit': 'piece'}
    result, error = katana_request('products', method='POST', data=product_data)
    if error:
        return None
    return result.get('data', {}) if isinstance(result, dict) else result

def find_or_create_variant_by_sku(sku, product_name, price, vat_rate):
    variant = find_variant_by_sku(sku)
    if variant:
        return variant
    product = find_or_create_product(product_name, sku)
    if not product:
        return None
    tax_rate_id = DEFAULT_TAX_RATE_ID_EUR if vat_rate >= 20 else DEFAULT_TAX_RATE_ID_GBP
    variant_data = {'product_id': product.get('id'), 'sku': sku, 'price': price, 'tax_rate_id': tax_rate_id}
    variant_result, error = katana_request('variants', method='POST', data=variant_data)
    if error:
        return None
    return variant_result.get('data', {}) if isinstance(variant_result, dict) else variant_result

def get_tax_rate_id(vat_rate, currency='EUR'):
    if currency == 'GBP' or vat_rate == 20:
        return DEFAULT_TAX_RATE_ID_GBP
    return DEFAULT_TAX_RATE_ID_EUR

def format_katana_date(won_time):
    if 'T' in won_time:
        dt = datetime.fromisoformat(won_time.replace('Z', '+00:00'))
    else:
        dt = datetime.strptime(won_time, '%Y-%m-%d %H:%M:%S')
    return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

def calculate_delivery_date(won_time, days=14):
    if 'T' in won_time:
        dt = datetime.fromisoformat(won_time.replace('Z', '+00:00'))
    else:
        dt = datetime.strptime(won_time, '%Y-%m-%d %H:%M:%S')
    delivery = dt + timedelta(days=days)
    return delivery.strftime('%Y-%m-%dT%H:%M:%S.000Z')

def build_additional_info(custom_items):
    if not custom_items:
        return ''
    lines = ['CUSTOM ITEMS (from Pipedrive):']
    for item in custom_items:
        sku_info = f" (Pipedrive SKU: {item['sku']})" if item.get('sku') else ""
        lines.append(f"  - Row {item['row']}: {item['name']}{sku_info} (Qty: {item['quantity']}, Price: {item['price']:.2f})")
    return '\n'.join(lines)

def handler(event, context):
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': {'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'POST, OPTIONS'}, 'body': ''}
    
    if event.get('httpMethod') != 'POST':
        return {'statusCode': 405, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'success': False, 'error': 'Method not allowed'})}
    
    try:
        body = event.get('body', '{}')
        data = json.loads(body) if isinstance(body, str) else body
        
        if not data:
            return {'statusCode': 400, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'success': False, 'error': 'No data provided'})}
        
        deal_id = data.get('deal_id')
        if not deal_id:
            return {'statusCode': 400, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'success': False, 'error': 'deal_id required'})}
        
        if not data.get('products'):
            products_result = pipedrive_request(f'deals/{deal_id}/products')
            deal_products = products_result.get('data', [])
            if not deal_products:
                return {'statusCode': 400, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'success': False, 'error': 'No products in deal'})}
            
            products = []
            for dp in deal_products:
                product_id = dp.get('product_id')
                if not product_id:
                    continue
                product_details = pipedrive_request(f'products/{product_id}').get('data', {})
                if not product_details:
                    continue
                sku = (product_details.get(SKU_FIELD_KEY, '') or product_details.get('code', '') or '').strip()
                vat_rate = product_details.get('tax') or product_details.get('vat') or 23
                products.append({'name': dp.get('name') or product_details.get('name', 'Unknown'), 'sku': sku, 'quantity': int(dp.get('quantity', 0)), 'price_per_unit': float(dp.get('item_price', 0)), 'vat_rate': int(vat_rate), 'currency': data.get('currency', 'EUR')})
            data['products'] = products
        
        if not data.get('won_time'):
            deal_result = pipedrive_request(f'deals/{deal_id}')
            deal = deal_result.get('data', {})
            if deal:
                data['won_time'] = deal.get('won_time', '')
                if not data.get('deal_title'):
                    data['deal_title'] = deal.get('title', '')
                if not data.get('currency'):
                    data['currency'] = deal.get('currency', 'EUR')
        
        if not data.get('won_time'):
            return {'statusCode': 400, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'success': False, 'error': 'won_time required'})}
        
        order_no = str(deal_id)
        existing_order = find_katana_order_by_number(order_no)
        if existing_order:
            return {'statusCode': 409, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'success': False, 'error': 'Order already exists', 'order_id': existing_order['id'], 'order_no': existing_order.get('order_no')})}
        
        customer_name = data.get('customer', {}).get('name') or data.get('deal_title', '')
        customer_email = data.get('customer', {}).get('email')
        customer = find_or_create_customer(customer_name, customer_email)
        if not customer:
            return {'statusCode': 500, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'success': False, 'error': 'Failed to create customer'})}
        
        products = data.get('products', [])
        order_rows = []
        custom_items = []
        
        for idx, product in enumerate(products, 1):
            sku = (product.get('sku') or '').strip().upper()
            name = product['name']
            quantity = int(product.get('quantity', 0))
            price = float(product.get('price_per_unit', 0))
            vat_rate = product.get('vat_rate', 23)
            currency = product.get('currency', data.get('currency', 'EUR'))
            
            if quantity == 0:
                continue
            
            if not sku:
                row = {'variant_id': CUSTOM_ITEM_VARIANT_ID, 'quantity': quantity, 'price_per_unit': max(0, price), 'tax_rate_id': get_tax_rate_id(vat_rate, currency), 'location_id': DEFAULT_LOCATION_ID}
                order_rows.append(row)
                custom_items.append({'row': len(order_rows), 'name': name, 'quantity': quantity, 'price': price})
            else:
                variant = find_or_create_variant_by_sku(sku, name, price, vat_rate)
                if variant:
                    row = {'variant_id': variant.get('id'), 'quantity': quantity, 'price_per_unit': price, 'tax_rate_id': get_tax_rate_id(vat_rate, currency), 'location_id': DEFAULT_LOCATION_ID}
                    order_rows.append(row)
                else:
                    row = {'variant_id': CUSTOM_ITEM_VARIANT_ID, 'quantity': quantity, 'price_per_unit': max(0, price), 'tax_rate_id': get_tax_rate_id(vat_rate, currency), 'location_id': DEFAULT_LOCATION_ID}
                    order_rows.append(row)
                    custom_items.append({'row': len(order_rows), 'name': name, 'sku': sku, 'quantity': quantity, 'price': price})
        
        if not order_rows:
            return {'statusCode': 400, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'success': False, 'error': 'No products to add'})}
        
        won_time = data.get('won_time')
        order_data = {'order_no': order_no, 'customer_id': customer.get('id'), 'customer_ref': data.get('deal_title', ''), 'order_created_date': format_katana_date(won_time), 'delivery_date': calculate_delivery_date(won_time), 'currency': data.get('currency', 'EUR'), 'location_id': DEFAULT_LOCATION_ID, 'source': 'pipedrive', 'sales_order_rows': order_rows}
        
        additional_info = build_additional_info(custom_items)
        if additional_info:
            order_data['additional_info'] = additional_info
        
        result, error = katana_request('sales_orders', method='POST', data=order_data)
        
        if error:
            return {'statusCode': 500, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'success': False, 'error': error})}
        
        order_id = result.get('id') if isinstance(result, dict) else result.get('data', {}).get('id')
        order_no_result = result.get('order_no') if isinstance(result, dict) else result.get('data', {}).get('order_no')
        
        return {'statusCode': 201, 'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}, 'body': json.dumps({'success': True, 'order_id': order_id, 'order_no': order_no_result, 'custom_items_count': len(custom_items)})}
        
    except Exception as e:
        return {'statusCode': 500, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps({'success': False, 'error': str(e)})}
