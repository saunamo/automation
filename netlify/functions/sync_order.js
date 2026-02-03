const KATANA_API_KEY = process.env.KATANA_API_KEY || '7cf78308-1e55-4d88-9c68-e06e8dc093fa';
const KATANA_BASE_URL = 'https://api.katanamrp.com/v1';
const PIPEDRIVE_API_TOKEN = process.env.PIPEDRIVE_API_TOKEN || '3c66a18f3cf4e8f5564c1134611c3fc834985ecf';
const PIPEDRIVE_COMPANY_DOMAIN = process.env.PIPEDRIVE_COMPANY_DOMAIN || 'saunamo';
const SKU_FIELD_KEY = '43a32efde94b5e07af24690d5b8db5dc18f5680a';

const DEFAULT_LOCATION_ID = 166154;
const CUSTOM_ITEM_VARIANT_ID = 38207669;

// Katana Tax Rate IDs - mapped by VAT percentage
const TAX_RATES = {
  23: 423653,  // PT VAT (Portugal) - Default
  22: 461343,  // Italy
  21: 423654,  // ES IVA (Spain)
  20: 459884,  // FR/UK (France/UK)
  18: 456470,  // MT VAT (Malta)
  6: 437610,   // Custom
  0: 461342,   // Zero VAT
};
const DEFAULT_TAX_RATE_ID = 423653; // 23% PT VAT (default)

async function katanaRequest(endpoint, method = 'GET', data = null) {
  const url = `${KATANA_BASE_URL}/${endpoint}`;
  const options = {
    method,
    headers: {
      'Authorization': `Bearer ${KATANA_API_KEY}`,
      'Content-Type': 'application/json'
    }
  };
  if (data) options.body = JSON.stringify(data);
  
  try {
    const response = await fetch(url, options);
    if (response.ok) {
      return { data: await response.json(), error: null };
    }
    return { data: null, error: `${response.status}: ${await response.text()}` };
  } catch (e) {
    return { data: null, error: e.message };
  }
}

async function pipedriveRequest(endpoint) {
  const url = `https://${PIPEDRIVE_COMPANY_DOMAIN}.pipedrive.com/api/v1/${endpoint}?api_token=${PIPEDRIVE_API_TOKEN}`;
  const response = await fetch(url);
  return response.json();
}

async function findKatanaOrderByNumber(orderNo) {
  const { data, error } = await katanaRequest('sales_orders?limit=1000');
  if (error) return null;
  const orders = data?.data || data?.results || [];
  return orders.find(o => String(o.order_no).trim() === String(orderNo).trim()) || null;
}

async function findOrCreateCustomer(name, email = null) {
  const { data, error } = await katanaRequest('customers?limit=1000');
  if (!error && data) {
    const customers = data?.data || data?.results || [];
    const existing = customers.find(c => c.name?.trim().toUpperCase() === name?.trim().toUpperCase());
    if (existing) return existing;
  }
  
  const customerData = { name, email: email || '', currency: 'EUR' };
  const result = await katanaRequest('customers', 'POST', customerData);
  if (result.error) return null;
  return result.data?.data || result.data;
}

async function findVariantBySku(sku) {
  if (!sku) return null;
  const { data, error } = await katanaRequest('variants?limit=1000');
  if (error) return null;
  const variants = data?.data || data?.results || [];
  return variants.find(v => v.sku?.toUpperCase().trim() === sku.toUpperCase().trim()) || null;
}

async function findOrCreateProduct(productName, sku) {
  const { data, error } = await katanaRequest('products?limit=1000');
  if (!error && data) {
    const products = data?.data || data?.results || [];
    const existing = products.find(p => p.name?.trim().toUpperCase() === productName?.trim().toUpperCase());
    if (existing) return existing;
  }
  
  const productData = { name: productName, code: sku, unit: 'piece' };
  const result = await katanaRequest('products', 'POST', productData);
  if (result.error) return null;
  return result.data?.data || result.data;
}

async function findOrCreateVariantBySku(sku, productName, price, vatRate) {
  const variant = await findVariantBySku(sku);
  if (variant) return variant;
  
  const product = await findOrCreateProduct(productName, sku);
  if (!product) return null;
  
  const taxRateId = getTaxRateId(vatRate);
  const variantData = { product_id: product.id, sku, price, tax_rate_id: taxRateId };
  const result = await katanaRequest('variants', 'POST', variantData);
  if (result.error) return null;
  return result.data?.data || result.data;
}

function getTaxRateId(vatRate) {
  // Look up exact VAT rate in our mapping
  const rate = parseInt(vatRate) || 23;
  
  // Return exact match if available, otherwise find closest or default to 23%
  if (TAX_RATES[rate]) {
    return TAX_RATES[rate];
  }
  
  // For rates not in our mapping, find the closest match
  const rates = Object.keys(TAX_RATES).map(Number).sort((a, b) => b - a);
  for (const r of rates) {
    if (rate >= r) {
      return TAX_RATES[r];
    }
  }
  
  return DEFAULT_TAX_RATE_ID; // 23% PT VAT
}

function formatKatanaDate(wonTime) {
  // Pipedrive won_time can be in format: "2024-01-15 10:30:00" or ISO format
  // Ensure we always get the exact time from Pipedrive
  let dt;
  if (typeof wonTime === 'string' && wonTime.includes('T')) {
    // ISO format: "2024-01-15T10:30:00Z" or "2024-01-15T10:30:00+00:00"
    dt = new Date(wonTime);
  } else if (typeof wonTime === 'string' && wonTime.includes(' ')) {
    // Pipedrive format: "2024-01-15 10:30:00"
    dt = new Date(wonTime.replace(' ', 'T') + 'Z');
  } else {
    dt = new Date(wonTime);
  }
  
  // Return in Katana format: "2024-01-15T10:30:00.000Z"
  return dt.toISOString().replace(/\.\d{3}Z$/, '.000Z');
}

function calculateDeliveryDate(wonTime, days = 14) {
  // Use same parsing as formatKatanaDate
  let dt;
  if (typeof wonTime === 'string' && wonTime.includes('T')) {
    dt = new Date(wonTime);
  } else if (typeof wonTime === 'string' && wonTime.includes(' ')) {
    dt = new Date(wonTime.replace(' ', 'T') + 'Z');
  } else {
    dt = new Date(wonTime);
  }
  
  dt.setDate(dt.getDate() + days);
  return dt.toISOString().replace(/\.\d{3}Z$/, '.000Z');
}

function buildAdditionalInfo(customItems) {
  if (!customItems.length) return '';
  const lines = ['CUSTOM ITEMS (from Pipedrive):'];
  customItems.forEach(item => {
    const skuInfo = item.sku ? ` (Pipedrive SKU: ${item.sku})` : '';
    lines.push(`  - Row ${item.row}: ${item.name}${skuInfo} (Qty: ${item.quantity}, Price: ${item.price.toFixed(2)})`);
  });
  return lines.join('\n');
}

exports.handler = async (event, context) => {
  // CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'POST, OPTIONS' },
      body: ''
    };
  }
  
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ success: false, error: 'Method not allowed' }) };
  }
  
  try {
    const data = JSON.parse(event.body || '{}');
    
    if (!data || !data.deal_id) {
      return { statusCode: 400, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ success: false, error: 'deal_id required' }) };
    }
    
    const dealId = data.deal_id;
    
    // Fetch products from Pipedrive if not provided
    if (!data.products) {
      const productsResult = await pipedriveRequest(`deals/${dealId}/products`);
      const dealProducts = productsResult.data || [];
      
      if (!dealProducts.length) {
        return { statusCode: 400, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ success: false, error: 'No products in deal' }) };
      }
      
      const products = [];
      for (const dp of dealProducts) {
        if (!dp.product_id) continue;
        const productDetails = (await pipedriveRequest(`products/${dp.product_id}`)).data || {};
        const sku = (productDetails[SKU_FIELD_KEY] || productDetails.code || '').trim();
        
        // VAT: Use deal product line item VAT if available, otherwise product VAT, default 23%
        // Pipedrive deal products can have different VAT than the base product
        const vatRate = dp.tax || dp.vat || productDetails.tax || productDetails.vat || 23;
        
        // Price: item_price is already the final price per unit AFTER discount
        // Pipedrive calculates: item_price = (base_price * (1 - discount_percentage/100))
        const finalPrice = parseFloat(dp.item_price || 0);
        
        // Track discount for reference (if available)
        const discountPercentage = parseFloat(dp.discount_percentage || dp.discount || 0);
        
        products.push({
          name: dp.name || productDetails.name || 'Unknown',
          sku,
          quantity: parseInt(dp.quantity || 0),
          price_per_unit: finalPrice, // Already includes discount
          vat_rate: parseInt(vatRate),
          discount_percentage: discountPercentage,
          currency: data.currency || 'EUR'
        });
      }
      data.products = products;
    }
    
    // Get won_time if not provided
    if (!data.won_time) {
      const dealResult = await pipedriveRequest(`deals/${dealId}`);
      const deal = dealResult.data || {};
      data.won_time = deal.won_time || '';
      if (!data.deal_title) data.deal_title = deal.title || '';
      if (!data.currency) data.currency = deal.currency || 'EUR';
    }
    
    if (!data.won_time) {
      return { statusCode: 400, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ success: false, error: 'won_time required' }) };
    }
    
    // Check for duplicates
    const orderNo = String(dealId);
    const existingOrder = await findKatanaOrderByNumber(orderNo);
    if (existingOrder) {
      return { statusCode: 409, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ success: false, error: 'Order already exists', order_id: existingOrder.id, order_no: existingOrder.order_no }) };
    }
    
    // Find or create customer
    const customerName = data.customer?.name || data.deal_title || '';
    const customerEmail = data.customer?.email;
    const customer = await findOrCreateCustomer(customerName, customerEmail);
    if (!customer) {
      return { statusCode: 500, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ success: false, error: 'Failed to create customer' }) };
    }
    
    // Process products
    const orderRows = [];
    const customItems = [];
    
    for (const product of (data.products || [])) {
      const sku = (product.sku || '').trim().toUpperCase();
      const name = product.name;
      const quantity = parseInt(product.quantity || 0);
      const price = parseFloat(product.price_per_unit || 0);
      const vatRate = product.vat_rate || 23;
      const currency = product.currency || data.currency || 'EUR';
      
      if (quantity === 0) continue;
      
      if (!sku) {
        orderRows.push({ variant_id: CUSTOM_ITEM_VARIANT_ID, quantity, price_per_unit: Math.max(0, price), tax_rate_id: getTaxRateId(vatRate), location_id: DEFAULT_LOCATION_ID });
        customItems.push({ row: orderRows.length, name, quantity, price });
      } else {
        const variant = await findOrCreateVariantBySku(sku, name, price, vatRate);
        if (variant) {
          orderRows.push({ variant_id: variant.id, quantity, price_per_unit: price, tax_rate_id: getTaxRateId(vatRate), location_id: DEFAULT_LOCATION_ID });
        } else {
          orderRows.push({ variant_id: CUSTOM_ITEM_VARIANT_ID, quantity, price_per_unit: Math.max(0, price), tax_rate_id: getTaxRateId(vatRate), location_id: DEFAULT_LOCATION_ID });
          customItems.push({ row: orderRows.length, name, sku, quantity, price });
        }
      }
    }
    
    if (!orderRows.length) {
      return { statusCode: 400, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ success: false, error: 'No products to add' }) };
    }
    
    // Build order data
    const orderData = {
      order_no: orderNo,
      customer_id: customer.id,
      customer_ref: data.deal_title || '',
      order_created_date: formatKatanaDate(data.won_time),
      delivery_date: calculateDeliveryDate(data.won_time),
      currency: data.currency || 'EUR',
      location_id: DEFAULT_LOCATION_ID,
      sales_order_rows: orderRows
    };
    
    const additionalInfo = buildAdditionalInfo(customItems);
    if (additionalInfo) orderData.additional_info = additionalInfo;
    
    // Create order
    const { data: result, error } = await katanaRequest('sales_orders', 'POST', orderData);
    
    if (error) {
      return { statusCode: 500, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ success: false, error }) };
    }
    
    const orderId = result?.id || result?.data?.id;
    const orderNoResult = result?.order_no || result?.data?.order_no;
    
    return {
      statusCode: 201,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({ success: true, order_id: orderId, order_no: orderNoResult, custom_items_count: customItems.length })
    };
    
  } catch (e) {
    return { statusCode: 500, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ success: false, error: e.message }) };
  }
};
