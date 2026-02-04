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
  
  // Search with pagination to find SKU across all variants
  const searchSku = sku.toUpperCase().trim();
  let start = 0;
  const limit = 1000;
  
  while (true) {
    const { data, error } = await katanaRequest(`variants?start=${start}&limit=${limit}`);
    if (error) return null;
    
    const variants = data?.data || data?.results || [];
    if (!variants || variants.length === 0) break;
    
    // Search in current batch
    const found = variants.find(v => v.sku?.toUpperCase().trim() === searchSku);
    if (found) return found;
    
    // If we got fewer results than limit, we've reached the end
    if (variants.length < limit) break;
    
    start += limit;
  }
  
  return null;
}

async function findOrCreateVariantBySku(sku, productName, price, vatRate) {
  if (!sku || !sku.trim()) {
    console.log('findOrCreateVariantBySku: Empty SKU provided');
    return null;
  }
  
  // First, try to find existing variant by SKU
  const variant = await findVariantBySku(sku);
  if (variant && variant.id) {
    console.log(`Found existing variant: SKU=${sku}, ID=${variant.id}`);
    return variant;
  }
  
  // Not found - create product WITH variant in one call (Katana requires this)
  console.log(`Creating new product for SKU=${sku}, name=${productName}`);
  const productData = {
    name: productName || `Product ${sku}`,
    variants: [{
      sku: sku.trim(),
      sales_price: Math.max(0, price || 0),
      purchase_price: Math.max(0, (price || 0) * 0.5)
    }]
  };
  
  const result = await katanaRequest('products', 'POST', productData);
  if (result.error) {
    console.log('Failed to create product:', result.error);
    return null;
  }
  
  // Return the created variant from the response
  // Katana returns the product directly (not wrapped in data.data)
  const createdProduct = result.data;
  console.log('Product creation response:', JSON.stringify(createdProduct).substring(0, 200));
  
  if (createdProduct?.variants && createdProduct.variants.length > 0) {
    const createdVariant = createdProduct.variants[0];
    if (createdVariant && createdVariant.id) {
      console.log(`Created variant: SKU=${sku}, ID=${createdVariant.id}`);
      return createdVariant;
    }
  }
  
  console.log(`Failed to get variant from created product for SKU=${sku}`);
  return null;
}

function getTaxRateId(vatRate) {
  // Look up exact VAT rate in our mapping
  // Handle 0% VAT explicitly (0 is falsy in JS, so we need special handling)
  const parsedRate = parseInt(vatRate);
  const rate = isNaN(parsedRate) ? 23 : parsedRate;
  
  // Return exact match if available
  if (TAX_RATES[rate] !== undefined) {
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
    
    // Fetch deal data from Pipedrive first (needed for deal-level discount calculation)
    const dealResult = await pipedriveRequest(`deals/${dealId}`);
    const dealData = dealResult.data || {};
    
    // Fetch products from Pipedrive if not provided
    if (!data.products) {
      const productsResult = await pipedriveRequest(`deals/${dealId}/products`);
      const dealProducts = productsResult.data || [];
      const additionalData = productsResult.additional_data || {};
      
      if (!dealProducts.length) {
        return { statusCode: 400, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ success: false, error: 'No products in deal' }) };
      }
      
      // Calculate deal-level discount from the difference between products sum and deal value
      // Pipedrive's "Additional discounts" section is reflected in this difference
      const productsSumTotal = parseFloat(additionalData.products_sum_total || 0);
      const dealValue = parseFloat(dealData.value || 0);
      let dealLevelDiscountPercent = 0;
      
      if (productsSumTotal > 0 && dealValue > 0 && dealValue < productsSumTotal) {
        // Deal-level discount = (products_sum - deal_value) / products_sum * 100
        dealLevelDiscountPercent = ((productsSumTotal - dealValue) / productsSumTotal) * 100;
        // Round to 2 decimal places
        dealLevelDiscountPercent = Math.round(dealLevelDiscountPercent * 100) / 100;
      }
      
      const products = [];
      for (const dp of dealProducts) {
        if (!dp.product_id) continue;
        const productDetails = (await pipedriveRequest(`products/${dp.product_id}`)).data || {};
        
        // Get SKU: First try the dedicated SKU field, then extract from product name (after "|")
        // Don't use 'code' field as fallback - it's used for country/region codes like "FR"
        let sku = (productDetails[SKU_FIELD_KEY] || '').trim();
        
        // If no SKU in dedicated field, try to extract from product name (format: "Product Name | SKU")
        if (!sku) {
          const productName = dp.name || productDetails.name || '';
          const pipeIndex = productName.lastIndexOf('|');
          if (pipeIndex > 0) {
            sku = productName.substring(pipeIndex + 1).trim();
          }
        }
        
        // VAT: Use deal product line item VAT if available, otherwise product VAT, default 23%
        // Handle 0% VAT explicitly (can't use || because 0 is falsy)
        const vatRate = dp.tax !== undefined && dp.tax !== null ? dp.tax 
          : dp.vat !== undefined && dp.vat !== null ? dp.vat
          : productDetails.tax !== undefined && productDetails.tax !== null ? productDetails.tax
          : productDetails.vat !== undefined && productDetails.vat !== null ? productDetails.vat
          : 23;
        
        // Price: item_price is the price per unit
        const itemPrice = parseFloat(dp.item_price || 0);
        const quantity = parseInt(dp.quantity || 1);
        const rowTotal = itemPrice * quantity;
        
        // Line-level discount: Check discount_type to interpret correctly
        // - "amount": discount is a currency value (e.g., £500)
        // - "percentage": discount is a percentage value
        const discountValue = parseFloat(dp.discount || 0);
        const discountType = dp.discount_type || 'percentage';
        
        let lineDiscountPercent = 0;
        if (discountValue > 0) {
          if (discountType === 'amount') {
            // Convert currency amount to percentage
            // Percentage = (discount_amount / row_total) * 100
            if (rowTotal > 0) {
              lineDiscountPercent = (discountValue / rowTotal) * 100;
            }
          } else {
            // It's already a percentage
            lineDiscountPercent = discountValue;
          }
        }
        
        // Cap line discount at 100%
        lineDiscountPercent = Math.min(lineDiscountPercent, 100);
        
        // Combine line-level and deal-level discounts
        // Total discount = line_discount + deal_discount * (1 - line_discount/100)
        // This compounds the discounts correctly
        let totalDiscountPercent = lineDiscountPercent;
        if (dealLevelDiscountPercent > 0 && lineDiscountPercent < 100) {
          // Apply deal-level discount on the remaining value after line discount
          totalDiscountPercent = lineDiscountPercent + (dealLevelDiscountPercent * (1 - lineDiscountPercent / 100));
        }
        
        // Cap total discount at 100% max (allow 100% for free items)
        totalDiscountPercent = Math.min(Math.round(totalDiscountPercent * 100) / 100, 100);
        
        products.push({
          name: dp.name || productDetails.name || 'Unknown',
          sku,
          quantity: quantity,
          price_per_unit: itemPrice,
          vat_rate: parseInt(vatRate),
          discount_percent: totalDiscountPercent, // Combined line + deal discount
          currency: data.currency || 'EUR'
        });
      }
      data.products = products;
      data.dealLevelDiscount = dealLevelDiscountPercent; // Store for reference
    }
    
    // Get won_time from deal data (already fetched above)
    if (!data.won_time) {
      data.won_time = dealData.won_time || '';
      if (!data.deal_title) data.deal_title = dealData.title || '';
      if (!data.currency) data.currency = dealData.currency || 'EUR';
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
      const originalPrice = parseFloat(product.price_per_unit || 0);
      const vatRate = product.vat_rate || 23;
      const discountPercent = parseFloat(product.discount_percent || 0);
      
      if (quantity === 0) continue;
      
      // Calculate discounted price - Katana's total_discount doesn't affect row totals
      // So we apply the discount to the price_per_unit directly
      let finalPrice = originalPrice;
      if (discountPercent > 0) {
        if (discountPercent >= 100) {
          // 100% discount = free item
          finalPrice = 0;
        } else {
          finalPrice = originalPrice * (1 - discountPercent / 100);
        }
      }
      // Ensure price is never negative
      finalPrice = Math.max(0, finalPrice);
      
      // Build base row data with discounted price
      // Note: We apply discount directly to price_per_unit, NOT using total_discount
      // because Katana's total_discount doesn't affect the row total calculation
      const baseRow = {
        quantity,
        price_per_unit: finalPrice,
        tax_rate_id: getTaxRateId(vatRate),
        location_id: DEFAULT_LOCATION_ID
      };
      
      if (!sku) {
        orderRows.push({ ...baseRow, variant_id: CUSTOM_ITEM_VARIANT_ID });
        customItems.push({ row: orderRows.length, name, quantity, price: finalPrice, discount: discountPercent });
      } else {
        const variant = await findOrCreateVariantBySku(sku, name, originalPrice, vatRate);
        if (variant && variant.id && typeof variant.id === 'number') {
          console.log(`Using variant ID ${variant.id} for SKU ${sku}`);
          orderRows.push({ ...baseRow, variant_id: variant.id });
        } else {
          console.log(`Falling back to CUSTOM-ITEM for SKU ${sku} (variant result: ${JSON.stringify(variant)})`);
          orderRows.push({ ...baseRow, variant_id: CUSTOM_ITEM_VARIANT_ID });
          customItems.push({ row: orderRows.length, name, sku, quantity, price: finalPrice, discount: discountPercent });
        }
      }
    }
    
    if (!orderRows.length) {
      return { statusCode: 400, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ success: false, error: 'No products to add' }) };
    }
    
    // Log order rows for debugging
    console.log(`Order rows to create: ${JSON.stringify(orderRows.map(r => ({ variant_id: r.variant_id, qty: r.quantity, price: r.price_per_unit })))}`);
    
    // Validate all order rows have valid variant_ids
    for (let i = 0; i < orderRows.length; i++) {
      const row = orderRows[i];
      if (!row.variant_id || typeof row.variant_id !== 'number') {
        console.log(`Invalid variant_id at row ${i}: ${JSON.stringify(row)}`);
        return { statusCode: 400, headers: { 'Content-Type': 'application/json' }, 
          body: JSON.stringify({ success: false, error: `Invalid variant_id at row ${i}: ${row.variant_id}` }) };
      }
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
