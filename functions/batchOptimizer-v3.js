// batchOptimizer-v3.js - new batch optimizer with dynamic prompts

// FIREBASE
const functions = require("firebase-functions");
const { onRequest } = require("firebase-functions/v2/https");
const logger = require("firebase-functions/logger");

function logChange(productId, field, before, after) {
  if (before !== after) {
    logger.debug(`Product ${productId} ${field} updated`, { before, after });
  }
}

// GOOGLE
const { SecretManagerServiceClient } = require("@google-cloud/secret-manager");
const { CloudTasksClient } = require("@google-cloud/tasks");
const tasksClient = new CloudTasksClient();
const secretClient = new SecretManagerServiceClient();

// SUPABASE
const { createClient } = require("@supabase/supabase-js");
let supabaseClient = null;
let supabaseServiceKey = "";
const supabaseUrl = "https://yhrgezxmxjoxpkhpywfw.supabase.co";

async function initSupabase() {
  if (!supabaseClient) {
    const [version] = await secretClient.accessSecretVersion({
      name: "projects/734399878923/secrets/SUPABASE_SERVICE_KEY/versions/latest",
    });
    supabaseServiceKey = version.payload.data.toString();
    supabaseClient = createClient(supabaseUrl, supabaseServiceKey);
  }
  return supabaseClient;
}

async function createOrUpdateSupabaseHistory(supabaseUserId, bulkeditid, newData) {
  const supabase = await initSupabase();
  const { data: existing } = await supabase
    .from("history_items")
    .select("*")
    .eq("user_id", supabaseUserId)
    .eq("bulkeditid", bulkeditid)
    .single();

  if (!existing) {
    const { data } = await supabase
      .from("history_items")
      .insert([{ user_id: supabaseUserId, bulkeditid, ...newData }])
      .select()
      .single();
    return data;
  } else {
    const updateData = { ...newData, updated_at: new Date().toISOString() };
    const { data } = await supabase
      .from("history_items")
      .update(updateData)
      .eq("user_id", supabaseUserId)
      .eq("bulkeditid", bulkeditid)
      .select()
      .single();
    return data;
  }
}

async function insertSupabaseProductData({
  supabase,
  userId,
  originalProductId,
  productData,
  sourceType = "Import",
  sourcePlatform = null,
  sourceCountry = null,
  editType = "ai-edit",
  importId = null,
  storeId = null,
  inAppTags = [],
  language = null,
}) {
  const product = productData.product;
  const insertPayload = {
    userid: userId,
    title: product?.title || "",
    price: parseFloat(product?.variants?.[0]?.price || 0),
    image: product?.image?.src || "",
    source_type: sourceType,
    source_platform: sourcePlatform,
    source_country: sourceCountry,
    store_id: storeId || null,
    source_domain: product?.source_domain || null,
    in_app_tags: inAppTags,
    language,
    ranking: null,
    edit_type: editType,
    import_id: importId || null,
    original_product_id: originalProductId,
    product_data: productData,
  };

  const { data, error } = await supabase
    .from("products")
    .insert([insertPayload])
    .select();

  if (error) throw error;
  return data?.[0];
}

function fillTemplate(str, context) {
  if (!str) return "";
  return str.replace(/\{\{(.*?)\}\}/g, (_, key) => {
    const trimmed = key.trim();
    return context[trimmed] !== undefined ? context[trimmed] : "";
  });
}

function resolveMessages(messages, context) {
  return messages.map((msg) => ({
    role: msg.role,
    content: msg.content.map((part) => {
      if (part.type === "text") {
        return { ...part, text: fillTemplate(part.text, context) };
      }
      return part;
    }),
  }));
}

function slugify(str) {
  return (str || "")
    .toString()
    .normalize("NFD")
    .replace(/[^\p{L}\p{N}]+/gu, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase();
}

async function applyEdit(edit, context, openai) {
  if (!edit) return null;
  if (edit.edit_type === "dynamic_template") {
    return fillTemplate(edit.settings.template, context);
  }
  if (edit.edit_type === "ai_edit") {
    const messages = resolveMessages(edit.settings.messages || [], context);
    const completion = await openai.chat.completions.create({
      model: edit.settings.model,
      messages,
      temperature: edit.settings.temperature,
      top_p: edit.settings.top_p,
      max_tokens: edit.settings.max_completion_tokens,
      frequency_penalty: edit.settings.frequency_penalty,
      presence_penalty: edit.settings.presence_penalty,
      response_format: edit.settings.response_format,
    });
    return completion.choices?.[0]?.message?.content?.trim() || null;
  }
  return null;
}

async function generateKeywords() {
  // TODO: replace this stub with real keyword generation
  // Returning an array so callers can assign directly to product.keywords
  return [];
}

function applyVariantInventorySettings(variant, inv, productId = null, index = null) {
  const before = { ...variant };
  const minQty = isNaN(inv.qty_min) ? 0 : inv.qty_min;
  const maxQty = isNaN(inv.qty_max) ? minQty : inv.qty_max;

  variant.inventory_policy = inv.variant_inventory_policy || "deny";

  if (inv.variant_weight_unit) {
    variant.weight_unit = inv.variant_weight_unit;
  }

  variant.inventory_management = inv.track_quantity ? "shopify" : null;

  if (maxQty > minQty) {
    variant.inventory_quantity =
      Math.floor(Math.random() * (maxQty - minQty + 1)) + minQty;
  } else {
    variant.inventory_quantity = minQty;
  }

  if (inv.currency) {
    variant.price_currency = inv.currency;
  }

  let price = parseFloat(variant.price || "0");
  if (inv.adjustPrices && !isNaN(inv.adjustmentAmount)) {
    price += inv.adjustmentAmount;
  }

  if (inv.roundPrices && !isNaN(inv.roundingNumber)) {
    const whole = Math.floor(price);
    price = whole + inv.roundingNumber;
  }
  variant.price = price.toFixed(2);

  const basePrice = parseFloat(variant.price);
  let comparePrice = NaN;
  if (!isNaN(inv.compare_at_amount)) {
    if (inv.compare_at_strategy === "=") {
      comparePrice = inv.compare_at_amount;
    } else if (inv.compare_at_strategy === "+") {
      comparePrice = basePrice + inv.compare_at_amount;
    } else if (inv.compare_at_strategy === "x") {
      comparePrice = basePrice * inv.compare_at_amount;
    }
  }

  if (!isNaN(comparePrice) && comparePrice > basePrice) {
    variant.compare_at_price = comparePrice.toFixed(2);
    if (inv.currency) {
      variant.compare_at_price_currency = inv.currency;
    }
  }

  logger.debug("Variant inventory applied", {
    productId,
    variantIndex: index,
    before,
    after: variant,
  });
}

const optimizeProductsByIdsBatchV3 = functions.https.onRequest((req, res) => {
  return require("cors")()(req, res, async () => {
    try {
      logger.debug("Incoming payload", req.body);
      const { productIds, user, settings } = req.body;
      if (!Array.isArray(productIds) || !user?.UID || !settings) {
        return res.status(400).json({ success: false, message: "Invalid input" });
      }

      const incomingSettings = JSON.parse(JSON.stringify(settings));

      logger.info("Start optimizeProductsByIdsBatchV3", {
        uid: user.UID,
        totalProducts: productIds.length,
      });

      const bulkeditid = Date.now().toString();
      const batchSize = 10;
      settings.bulkeditid = bulkeditid;
      settings.startIndex = 0;

      await createOrUpdateSupabaseHistory(user.UID, bulkeditid, {
        status: "Processing",
        type: "AI edit",
        name: settings.general?.name || "AI-edit",
        total_products: productIds.length,
        tokens: 0,
        products_processed: 0,
        output_file: "",
        edit_settings: incomingSettings,
      });

      const batches = [];
      for (let i = 0; i < productIds.length; i += batchSize) {
        batches.push(productIds.slice(i, i + batchSize));
      }

      const parent = tasksClient.queuePath("ecomai-3730f", "us-central1", "my-queue");
      for (let i = 0; i < batches.length; i++) {
        const batchSettings = { ...settings, startIndex: i * batchSize, total_products: productIds.length };
        const taskPayload = { productIds: batches[i], user, settings: batchSettings };
        const task = {
          httpRequest: {
            httpMethod: "POST",
            url: "https://us-central1-ecomai-3730f.cloudfunctions.net/processOptimizeProductsBatchTaskV3",
            headers: { "Content-Type": "application/json" },
            body: Buffer.from(JSON.stringify(taskPayload)).toString("base64"),
          },
        };
        const [cloudTask] = await tasksClient.createTask({ parent, task });
        logger.info(`Batch ${i + 1}/${batches.length} task created`, { name: cloudTask.name });
      }

      logger.info("All batch tasks created", { count: batches.length, bulkeditid });

      return res.status(200).json({ success: true, bulkeditid, message: `Batches aangemaakt: ${batches.length}` });
    } catch (err) {
      logger.error("optimizeProductsByIdsBatchV3 error", err);
      return res.status(500).json({ success: false, error: err.message });
    }
  });
});

const processOptimizeProductsBatchTaskV3 = onRequest({ timeoutSeconds: 300 }, async (req, res) => {
  const cors = require("cors")({ origin: true });
  return cors(req, res, async () => {
    try {
      logger.debug("Task payload", req.body);
      const { productIds, user, settings } = req.body;
      logger.info("Start processOptimizeProductsBatchTaskV3", {
        uid: user.UID,
        count: productIds.length,
        startIndex: settings.startIndex,
      });
      const supabase = await initSupabase();
      const [version] = await secretClient.accessSecretVersion({
        name: "projects/734399878923/secrets/openAiSecret/versions/latest",
      });
      const openAiKey = version.payload.data.toString();
      const { OpenAI } = require("openai");
      const openai = new OpenAI({ apiKey: openAiKey });

      const { data: products, error } = await supabase
        .from("products")
        .select("*")
        .in("id", productIds);
      if (error || !products?.length) {
        throw new Error("Cannot fetch products from Supabase");
      }

      let createdCount = 0;
      let lastReported = 0;

      for (const original of products) {
        logger.debug("Processing product", { id: original.id });
        let raw = typeof original.product_data === "string" ? JSON.parse(original.product_data) : original.product_data;
        if (!raw?.product?.title) continue;
        const product = raw.product;
        const context = {
          ...product,
          title: product.title,
          "seo-title": product.seo_title,
          description: product.body_html,
          "seo-description": product.seo_description,
          OGtitle: product.title,
          OGseo_title: product.seo_title,
          "OGseo-title": product.seo_title,
          OGdescription: product.body_html,
          OGseo_description: product.seo_description,
          "OGseo-description": product.seo_description,
          now: Date.now().toString(),
        };

        // Organization/vendortags etc
        const prevVendor = product.vendor;
        product.vendor = settings.organization?.vendor || product.vendor;
        logChange(original.id, "vendor", prevVendor, product.vendor);

        const newTags = await applyEdit(settings.organization?.tags, context, openai);
        if (settings.organization?.tagAction === "clear") {
          const beforeTags = product.tags;
          product.tags = "";
          logChange(original.id, "tags", beforeTags, product.tags);
        } else if (settings.organization?.tagAction === "replace") {
          const beforeTags = product.tags;
          product.tags = newTags || "";
          logChange(original.id, "tags", beforeTags, product.tags);
        } else if (settings.organization?.tagAction === "add" && newTags) {
          const existing = (product.tags || "").split(",").map((t) => t.trim()).filter(Boolean);
          const incoming = newTags.split(",").map((t) => t.trim()).filter(Boolean);
          const beforeTags = product.tags;
          product.tags = Array.from(new Set([...existing, ...incoming])).join(", ");
          logChange(original.id, "tags", beforeTags, product.tags);
        }
        const prevPublished = product.published;
        product.published = settings.organization?.published;
        logChange(original.id, "published", prevPublished, product.published);
        const prevStatus = product.status;
        product.status = settings.organization?.status || product.status;
        logChange(original.id, "status", prevStatus, product.status);
        const template = await applyEdit(settings.organization?.theme_template, context, openai);
        if (template) {
          const before = product.theme_template;
          product.theme_template = template;
          logChange(original.id, "theme_template", before, template);
        }

        // Inventory
        if (Array.isArray(product.variants)) {
          const inv = { ...(settings.inventoryPrices || {}) };
          inv.qty_min = Number(inv.variant_inventory_qty_min);
          inv.qty_max = Number(inv.variant_inventory_qty_max);
          inv.adjustmentAmount = Number(inv.adjustmentAmount);
          inv.roundingNumber = Number(inv.roundingNumber);
          inv.compare_at_amount = Number(inv.compare_at_amount);

          for (const variant of product.variants) {
            if (inv.sku) {
              const sku = await applyEdit(inv.sku, context, openai);
              if (sku) {
                const before = variant.sku;
                variant.sku = sku;
                logChange(original.id, "variant.sku", before, sku);
              }
            }
            if (inv.barcode) {
              const bc = await applyEdit(inv.barcode, context, openai);
              if (bc) {
                const before = variant.barcode;
                variant.barcode = bc;
                logChange(original.id, "variant.barcode", before, bc);
              }
            }

            applyVariantInventorySettings(
              variant,
              inv,
              original.id,
              product.variants.indexOf(variant)
            );
          }
        }

        // Copywriting fields
        const title = await applyEdit(settings.copywriting?.title, context, openai);
        if (title) {
          const before = product.title;
          product.title = title;
          logChange(original.id, "title", before, title);
          context.title = title;
          context["title"] = title;
          if (!context.OGtitle) context.OGtitle = title;
          if (!context["OGtitle"]) context["OGtitle"] = title;
        }
        const description = await applyEdit(settings.copywriting?.description, context, openai);
        if (description) {
          const before = product.body_html;
          product.body_html = description;
          logChange(original.id, "body_html", before, description);
          context.description = description;
          context["description"] = description;
          if (!context.OGdescription) context.OGdescription = description;
          if (!context["OGdescription"]) context["OGdescription"] = description;
        }
        const seoTitle = await applyEdit(settings.copywriting?.seo_title, context, openai);
        if (seoTitle) {
          const before = product.seo_title;
          product.seo_title = seoTitle;
          logChange(original.id, "seo_title", before, seoTitle);
          context.seo_title = seoTitle;
          context["seo-title"] = seoTitle;
          if (!context.OGseo_title) context.OGseo_title = seoTitle;
          if (!context["OGseo-title"]) context["OGseo-title"] = seoTitle;
        }
        const seoDesc = await applyEdit(settings.copywriting?.seo_description, context, openai);
        if (seoDesc) {
          const before = product.seo_description;
          product.seo_description = seoDesc;
          logChange(original.id, "seo_description", before, seoDesc);
          context.seo_description = seoDesc;
          context["seo-description"] = seoDesc;
          if (!context.OGseo_description) context.OGseo_description = seoDesc;
          if (!context["OGseo-description"]) context["OGseo-description"] = seoDesc;
        }
        const handle = await applyEdit(settings.copywriting?.handle, context, openai);
        if (handle) {
          const before = product.handle;
          product.handle = slugify(handle);
          logChange(original.id, "handle", before, product.handle);
        }

        // Google fields
        const gCat = await applyEdit(settings.google?.product_category, context, openai);
        if (gCat) {
          const before = product.g_category;
          product.g_category = gCat;
          logChange(original.id, "g_category", before, gCat);
        }
        const gGender = await applyEdit(settings.google?.gender, context, openai);
        if (gGender) {
          const before = product.g_gender;
          product.g_gender = gGender;
          logChange(original.id, "g_gender", before, gGender);
        }
        const beforeCond = product.g_condition;
        product.g_condition = settings.google?.condition || product.g_condition;
        logChange(original.id, "g_condition", beforeCond, product.g_condition);
        const beforeAge = product.g_age_group;
        product.g_age_group = settings.google?.ageGroup || product.g_age_group;
        logChange(original.id, "g_age_group", beforeAge, product.g_age_group);
        const beforeCustom = product.g_custom_product;
        product.g_custom_product = settings.google?.customProduct || product.g_custom_product;
        logChange(original.id, "g_custom_product", beforeCustom, product.g_custom_product);
        if (Array.isArray(settings.google?.custom_labels)) {
          settings.google.custom_labels = await Promise.all(
            settings.google.custom_labels.map((cl) => applyEdit(cl, context, openai))
          );
          const beforeLabels = [
            product.g_label0,
            product.g_label1,
            product.g_label2,
            product.g_label3,
            product.g_label4,
          ];
          product.g_label0 = settings.google.custom_labels[0] || product.g_label0;
          product.g_label1 = settings.google.custom_labels[1] || product.g_label1;
          product.g_label2 = settings.google.custom_labels[2] || product.g_label2;
          product.g_label3 = settings.google.custom_labels[3] || product.g_label3;
          product.g_label4 = settings.google.custom_labels[4] || product.g_label4;
          const afterLabels = [
            product.g_label0,
            product.g_label1,
            product.g_label2,
            product.g_label3,
            product.g_label4,
          ];
          logChange(original.id, "g_labels", beforeLabels, afterLabels);
        }

        // General gender field
        const genGender = await applyEdit(settings.general?.gender, context, openai);
        if (genGender) {
          const before = product.gender;
          product.gender = genGender;
          logChange(original.id, "gender", before, genGender);
        }

        if (Array.isArray(settings.customMetafields)) {
          product.metafields = product.metafields || [];
          for (const mf of settings.customMetafields) {
            if (!mf) continue;
            const val = await applyEdit(mf.value, context, openai);
            if (val) {
              product.metafields.push({ ...mf, value: val });
              logChange(original.id, `metafield_${mf.key}`, null, val);
            }
          }
        }

        // Keywords placeholder
        product.keywords = await generateKeywords(product, settings.keywords, openai);

        if (!product.handle) {
          const generated = slugify(product.title);
          product.handle = generated;
          logChange(original.id, "handle", null, generated);
        }

        raw.product = product;

        await insertSupabaseProductData({
          supabase,
          userId: user.UID,
          originalProductId: original.id,
          productData: raw,
          sourceType: user.source_type || original.source_type,
          sourcePlatform: original.source_platform,
          sourceCountry: user.source_country || original.source_country,
          importId: settings.bulkeditid,
          storeId: user.store_id || null,
          inAppTags: settings.general?.in_app_tags || [],
          language: settings.general?.Language || null,
        });
        logger.debug("Product stored", { id: original.id });

        createdCount++;
        const shouldUpdate = createdCount - lastReported >= 5;
        const lastOne = createdCount === products.length;
        if (shouldUpdate || lastOne) {
          const inc = createdCount - lastReported;
          await supabase.rpc("increment_products_processed", {
            uid_input: user.UID,
            bulkeditid_input: settings.bulkeditid,
            increment_by: inc,
          });
          lastReported = createdCount;
        }
      }

      const { data: history } = await supabase
        .from("history_items")
        .select("products_processed, total_products")
        .eq("user_id", user.UID)
        .eq("bulkeditid", settings.bulkeditid)
        .maybeSingle();
      if (history?.products_processed >= history?.total_products) {
        await supabase
          .from("history_items")
          .update({ status: "Completed", updated_at: new Date().toISOString() })
          .eq("user_id", user.UID)
          .eq("bulkeditid", settings.bulkeditid);
      }

      logger.info("Batch processed", { processed: createdCount });
      return res.status(200).json({ success: true, message: "Batch processed" });
    } catch (err) {
      logger.error("processOptimizeProductsBatchTaskV3 error", err);
      return res.status(500).json({ success: false, error: err.message });
    }
  });
});

module.exports = {
  initSupabase,
  createOrUpdateSupabaseHistory,
  insertSupabaseProductData,
  optimizeProductsByIdsBatchV3,
  processOptimizeProductsBatchTaskV3,
  applyVariantInventorySettings,
};
