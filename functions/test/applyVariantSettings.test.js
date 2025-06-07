const assert = require("assert");
const { applyVariantInventorySettings } = require("../batchOptimizer-v3");

// Quantity range test
(() => {
  const variant = { price: "10" };
  const settings = {
    qty_min: 2,
    qty_max: 5,
    variant_inventory_policy: "allow",
    track_quantity: true
  };
  applyVariantInventorySettings(variant, settings);
  assert(variant.inventory_quantity >= 2 && variant.inventory_quantity <= 5);
  assert.strictEqual(variant.inventory_policy, "allow");
  assert.strictEqual(variant.inventory_management, "shopify");
})();

// Price adjust and rounding test
(() => {
  const variant = { price: "10.00" };
  const settings = {
    qty_min: 0,
    qty_max: 0,
    adjustPrices: true,
    adjustmentAmount: 5,
    roundPrices: true,
    roundingNumber: 0.5,
    currency: "USD"
  };
  applyVariantInventorySettings(variant, settings);
  assert.strictEqual(variant.price, "15.50");
  assert.strictEqual(variant.price_currency, "USD");
})();

// Compare-at price strategies
(() => {
  const variant = { price: "10.00" };
  const settings = {
    qty_min: 0,
    qty_max: 0,
    compare_at_strategy: "+",
    compare_at_amount: 5,
    currency: "EUR"
  };
  applyVariantInventorySettings(variant, settings);
  assert.strictEqual(variant.compare_at_price, "15.00");
  assert.strictEqual(variant.compare_at_price_currency, "EUR");
})();

console.log("All tests passed.");
