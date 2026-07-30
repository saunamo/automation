/**
 * Retired 2026-07-30.
 *
 * Pipedrive is no longer a Saunamo system of record. Saunamo OS now creates
 * projects and inventory sales orders when a CRM deal is won, while Shopify
 * order/create webhooks are handled by the direct katana-automation service.
 *
 * Keep this tombstone deployed so any forgotten caller receives an explicit,
 * observable failure instead of silently creating an incomplete order.
 */
exports.handler = async () => ({
  statusCode: 410,
  headers: {
    'Content-Type': 'application/json',
    'Cache-Control': 'no-store',
  },
  body: JSON.stringify({
    ok: false,
    retired: true,
    error: 'pipedrive_katana_sync_retired',
    message: 'This legacy Pipedrive-to-Katana order sync has been retired. Use Saunamo OS.',
  }),
});
