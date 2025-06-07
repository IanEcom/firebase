// batchOptimizer-v3.js - new batch optimizer with dynamic prompts

// FIREBASE
const functions = require("firebase-functions");
const { onRequest } = require("firebase-functions/v2/https");

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
    const { data } = await supabase
      .from("history_items")
      .update({ ...newData, updated_at: new Date().toISOString() })
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

const optimizeProductsByIdsBatchV3 = functions.https.onRequest((req, res) => {
  return require("cors")()(req, res, async () => {
    try {
      const { productIds, user, settings } = req.body;
      if (!Array.isArray(productIds) || !user?.UID || !settings) {
        return res.status(400).json({ success: false, message: "Invalid input" });
      }

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
        await tasksClient.createTask({ parent, task });
      }

      return res.status(200).json({ success: true, bulkeditid, message: `Batches aangemaakt: ${batches.length}` });
    } catch (err) {
      console.error("optimizeProductsByIdsBatchV3 error", err);
      return res.status(500).json({ success: false, error: err.message });
    }
  });
});

const processOptimizeProductsBatchTaskV3 = onRequest({ timeoutSeconds: 300 }, async (req, res) => {
  const cors = require("cors")({ origin: true });
  return cors(req, res, async () => {
    try {
      const { productIds, user, settings } = req.body;
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
        let raw = typeof original.product_data === "string" ? JSON.parse(original.product_data) : original.product_data;
        if (!raw?.product?.title) continue;
        const product = raw.product;
        const context = { ...product, now: Date.now().toString() };

        // Organization/vendortags etc
        product.vendor = settings.organization?.vendor || product.vendor;
        if (settings.organization?.tagAction === "clear") {
          product.tags = "";
        } else if (settings.organization?.tagAction === "replace") {
          product.tags = settings.organization?.tags || "";
        } else if (settings.organization?.tagAction === "add" && settings.organization?.tags) {
          const existing = (product.tags || "").split(",").map((t) => t.trim()).filter(Boolean);
          const incoming = settings.organization.tags.split(",").map((t) => t.trim()).filter(Boolean);
          product.tags = Array.from(new Set([...existing, ...incoming])).join(", ");
        }
        product.published = settings.organization?.published;
        product.status = settings.organization?.status || product.status;
        if (settings.organization?.theme_template) {
          product.theme_template = settings.organization.theme_template;
        }

        // Inventory
        if (Array.isArray(product.variants)) {
          for (const variant of product.variants) {
            if (settings.inventoryPrices?.sku) {
              const sku = await applyEdit(settings.inventoryPrices.sku, context, openai);
              if (sku) variant.sku = sku;
            }
            if (settings.inventoryPrices?.barcode) {
              const bc = await applyEdit(settings.inventoryPrices.barcode, context, openai);
              if (bc) variant.barcode = bc;
            }
          }
        }

        // Copywriting fields
        const title = await applyEdit(settings.copywriting?.title, context, openai);
        if (title) { product.title = title; context.title = title; }
        const description = await applyEdit(settings.copywriting?.description, context, openai);
        if (description) { product.body_html = description; context.description = description; }
        const seoTitle = await applyEdit(settings.copywriting?.seo_title, context, openai);
        if (seoTitle) { product.seo_title = seoTitle; context.seo_title = seoTitle; }
        const seoDesc = await applyEdit(settings.copywriting?.seo_description, context, openai);
        if (seoDesc) { product.seo_description = seoDesc; context.seo_description = seoDesc; }
        const handle = await applyEdit(settings.copywriting?.handle, context, openai);
        if (handle) { product.handle = handle; }

        // Google fields
        const gCat = await applyEdit(settings.google?.product_category, context, openai);
        if (gCat) product.g_category = gCat;
        const gGender = await applyEdit(settings.google?.gender, context, openai);
        if (gGender) product.g_gender = gGender;
        product.g_condition = settings.google?.condition || product.g_condition;
        product.g_age_group = settings.google?.ageGroup || product.g_age_group;
        product.g_custom_product = settings.google?.customProduct || product.g_custom_product;
        if (Array.isArray(settings.google?.custom_labels)) {
          settings.google.custom_labels = await Promise.all(settings.google.custom_labels.map((cl) => applyEdit(cl, context, openai)));
          product.g_label0 = settings.google.custom_labels[0] || product.g_label0;
          product.g_label1 = settings.google.custom_labels[1] || product.g_label1;
          product.g_label2 = settings.google.custom_labels[2] || product.g_label2;
          product.g_label3 = settings.google.custom_labels[3] || product.g_label3;
          product.g_label4 = settings.google.custom_labels[4] || product.g_label4;
        }

        // General gender field
        const genGender = await applyEdit(settings.general?.gender, context, openai);
        if (genGender) product.gender = genGender;

        if (Array.isArray(settings.customMetafields)) {
          product.metafields = product.metafields || [];
          for (const mf of settings.customMetafields) {
            if (!mf) continue;
            const val = await applyEdit(mf.value, context, openai);
            if (val) {
              product.metafields.push({ ...mf, value: val });
            }
          }
        }

        // Keywords placeholder
        product.keywords = await generateKeywords(product, settings.keywords, openai);

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

      return res.status(200).json({ success: true, message: "Batch processed" });
    } catch (err) {
      console.error("processOptimizeProductsBatchTaskV3 error", err);
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
};
