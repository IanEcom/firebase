const admin = require("firebase-admin");
admin.initializeApp();

const functions = require("firebase-functions");
const { onRequest } = require("firebase-functions/v2/https");
const { SecretManagerServiceClient } = require("@google-cloud/secret-manager");
const { CloudTasksClient } = require("@google-cloud/tasks");
const { createClient } = require("@supabase/supabase-js");

const secretClient = new SecretManagerServiceClient();
const tasksClient = new CloudTasksClient();

const supabaseUrl = "https://yhrgezxmxjoxpkhpywfw.supabase.co";
let supabaseClient = null;
let supabaseServiceKey = "";

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

async function createOrUpdateSupabaseHistory(userId, bulkeditid, newData) {
  const supabase = await initSupabase();
  const { data: existing } = await supabase
    .from("history_items")
    .select("*")
    .eq("user_id", userId)
    .eq("bulkeditid", bulkeditid)
    .single();

  if (!existing) {
    const { data } = await supabase
      .from("history_items")
      .insert([{ user_id: userId, bulkeditid, ...newData }])
      .select()
      .single();
    return data;
  } else {
    const { data } = await supabase
      .from("history_items")
      .update({ ...newData, updated_at: new Date().toISOString() })
      .eq("user_id", userId)
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
    in_app_tags: [],
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

function applyTemplate(str, context) {
  if (!str) return "";
  return str.replace(/{{(.*?)}}/g, (_, k) => {
    const key = k.trim();
    return context[key] !== undefined ? context[key] : "";
  });
}

async function runAiEdit(config, context, openai) {
  if (!config || !config.model || !Array.isArray(config.messages)) return null;
  const messages = config.messages.map((m) => {
    const txt = (m.content || [])
      .map((c) => (c.type === "text" ? c.text : ""))
      .join("");
    return { role: m.role, content: applyTemplate(txt, context) };
  });
  const completion = await openai.chat.completions.create({
    model: config.model,
    messages,
    response_format: config.response_format,
    temperature: config.temperature,
    max_tokens: config.max_completion_tokens,
    top_p: config.top_p,
    frequency_penalty: config.frequency_penalty,
    presence_penalty: config.presence_penalty,
  });
  return completion.choices?.[0]?.message?.content?.trim() || null;
}

async function generateKeywords(context, keywordsConf, openai) {
  if (!keywordsConf) return [];
  const seed = applyTemplate(keywordsConf.seed_builder || "", context);
  let seedOut = seed;
  if (keywordsConf.settings?.seed_ai) {
    seedOut =
      (await runAiEdit(keywordsConf.settings.seed_ai, { ...context, seed }, openai)) ||
      seed;
  }
  let finalText = seedOut;
  if (keywordsConf.settings?.keyword_ai) {
    finalText =
      (await runAiEdit(
        keywordsConf.settings.keyword_ai,
        { ...context, keywords: seedOut },
        openai,
      )) || seedOut;
  }
  return finalText
    .split(/[,;\n]+/)
    .map((k) => k.trim())
    .filter(Boolean);
}

exports.optimizeProductsByIdsBatchV3 = functions.https.onRequest((req, res) => {
  return require("cors")()(req, res, async () => {
    try {
      const { productIds, user, settings } = req.body;
      if (!Array.isArray(productIds) || !user?.UID || !settings) {
        return res.status(400).json({ success: false, message: "Invalid input" });
      }
      const userId = user.UID;
      const bulkeditid = Date.now().toString();
      settings.bulkeditid = bulkeditid;
      settings.startIndex = 0;

      await createOrUpdateSupabaseHistory(userId, bulkeditid, {
        status: "Processing",
        type: "AI edit",
        name: settings.general?.name || "AI-edit",
        total_products: productIds.length,
        tokens: 0,
        products_processed: 0,
        output_file: "",
      });

      const batchSize = 10;
      const batches = [];
      for (let i = 0; i < productIds.length; i += batchSize) {
        batches.push(productIds.slice(i, i + batchSize));
      }

      const parent = tasksClient.queuePath("ecomai-3730f", "us-central1", "my-queue");
      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        const batchSettings = { ...settings, startIndex: i * batchSize, total_products: productIds.length };
        const taskPayload = { productIds: batch, settings: batchSettings, user };
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

      return res.status(200).json({ success: true, bulkeditid, message: `Created ${batches.length} tasks` });
    } catch (err) {
      console.error("optimizeProductsByIdsBatchV3 error", err);
      return res.status(500).json({ success: false, message: err.message });
    }
  });
});

exports.processOptimizeProductsBatchTaskV3 = onRequest({ timeoutSeconds: 300 }, async (req, res) => {
  const cors = require("cors")({ origin: true });
  return cors(req, res, async () => {
    try {
      const { productIds, settings, user } = req.body;
      const userId = user.UID;
      const supabase = await initSupabase();
      const [version] = await secretClient.accessSecretVersion({
        name: "projects/734399878923/secrets/openAiSecret/versions/latest",
      });
      const { OpenAI } = require("openai");
      const openAiKey = version.payload.data.toString();
      const openai = new OpenAI({ apiKey: openAiKey });

      const { data: products } = await supabase.from("products").select("*").in("id", productIds);
      if (!products || products.length === 0) throw new Error("No products found");

      let createdCount = 0;
      let lastReported = 0;

      for (const original of products) {
        let raw = typeof original.product_data === "string" ? JSON.parse(original.product_data) : original.product_data;
        if (!raw?.product?.title) continue;
        const product = raw.product;

        if (settings.store_id) {
          await supabase.rpc("append_store_to_edited_list", { original_product_id: original.id, store_id_to_add: settings.store_id });
        }

        // Organization/general settings
        product.vendor = settings.organization?.vendor || product.vendor;
        product.published = settings.organization?.published ?? product.published;
        product.status = settings.organization?.status || product.status;

        if (settings.organization) {
          const action = settings.organization.tagAction;
          if (action === "clear") product.tags = "";
          else if (action === "replace") product.tags = settings.organization.tags || "";
          else if (action === "add" && settings.organization.tags) {
            const existing = (product.tags || "").split(",").map((t) => t.trim()).filter(Boolean);
            const addTags = settings.organization.tags.split(",").map((t) => t.trim()).filter(Boolean);
            product.tags = Array.from(new Set([...existing, ...addTags])).join(", ");
          }
        }

        const context = {
          title: product.title,
          description: product.body_html || "",
          seo_title: product.seo_title || "",
          seo_description: product.seo_description || "",
          now: Date.now().toString(),
        };

        // General gender
        if (settings.general?.gender) {
          const field = settings.general.gender;
          let val = null;
          if (field.edit_type === "ai_edit") {
            val = await runAiEdit(field.settings, context, openai);
          } else if (field.edit_type === "dynamic_template") {
            val = applyTemplate(field.settings.template, context);
          }
          if (val) {
            context.gender = val;
          }
        }

        // Copywriting fields
        const handleField = async (fieldConf, current, assign) => {
          if (!fieldConf) return current;
          let val = current;
          if (fieldConf.edit_type === "ai_edit") {
            val = await runAiEdit(fieldConf.settings, context, openai) || current;
          } else if (fieldConf.edit_type === "dynamic_template") {
            val = applyTemplate(fieldConf.settings.template, context) || current;
          }
          if (val !== current) assign(val);
          return val;
        };

        product.title = await handleField(settings.copywriting?.title, product.title, (v) => (product.title = v));
        context.title = product.title;

        product.body_html = await handleField(settings.copywriting?.description, product.body_html, (v) => (product.body_html = v));
        context.description = product.body_html;

        product.seo_title = await handleField(settings.copywriting?.seo_title, product.seo_title, (v) => (product.seo_title = v));
        context.seo_title = product.seo_title;

        product.seo_description = await handleField(settings.copywriting?.seo_description, product.seo_description, (v) => (product.seo_description = v));
        context.seo_description = product.seo_description;

        if (settings.copywriting?.handle && settings.copywriting.handle.edit_type === "dynamic_template") {
          product.handle = applyTemplate(settings.copywriting.handle.settings.template, context);
        }

        // Organization tags via AI
        if (settings.organization?.tags && settings.organization.tags.edit_type === "ai_edit") {
          const aiTags = await runAiEdit(settings.organization.tags.settings, context, openai);
          if (aiTags) {
            product.tags = aiTags;
          }
        }

        // Inventory / pricing
        if (Array.isArray(product.variants)) {
          product.variants.forEach((variant, i) => {
            if (settings.inventoryPrices?.sku?.edit_type === "dynamic_template") {
              variant.sku = applyTemplate(settings.inventoryPrices.sku.settings.template, { ...context, index: i + 1 });
            }
            if (settings.inventoryPrices?.barcode?.edit_type === "dynamic_template") {
              variant.barcode = applyTemplate(settings.inventoryPrices.barcode.settings.template, { ...context, index: i + 1 });
            }
            variant.inventory_policy = settings.inventoryPrices?.variant_inventory_policy || variant.inventory_policy;
            variant.inventory_management = settings.inventoryPrices?.track_quantity ? "shopify" : null;
          });
        }

        // Google settings
        if (settings.google) {
          if (settings.google.product_category?.edit_type === "ai_edit") {
            const cat = await runAiEdit(settings.google.product_category.settings, context, openai);
            if (cat) product.product_category = cat;
          }
          if (settings.google.gender?.edit_type === "ai_edit") {
            const g = await runAiEdit(settings.google.gender.settings, context, openai);
            if (g) product.g_gender = g;
          }
          product.g_condition = settings.google.condition || product.g_condition;
          product.g_age_group = settings.google.ageGroup || product.g_age_group;
          product.g_custom_product = settings.google.customProduct || product.g_custom_product;
          if (Array.isArray(settings.google.custom_labels)) {
            const labels = [];
            for (const lbl of settings.google.custom_labels) {
              if (lbl.edit_type === "ai_edit") {
                const txt = await runAiEdit(lbl.settings, context, openai);
                if (txt) labels.push(txt);
              }
            }
            labels.forEach((l, idx) => {
              product[`g_label${idx}`] = l;
            });
          }
        }

        // Keywords placeholder
        const keywords = await generateKeywords(context, settings.keywords, openai);
        if (keywords.length) {
          product.keywords = keywords;
        }

        raw.product = product;
        await insertSupabaseProductData({
          supabase,
          userId,
          originalProductId: original.id,
          productData: raw,
          sourceType: original.source_type,
          sourcePlatform: original.source_platform,
          sourceCountry: original.source_country,
          importId: settings.bulkeditid,
          storeId: settings.store_id || null,
        });

        createdCount++;
        const shouldUpdate = createdCount - lastReported >= 5;
        const isLast = createdCount === products.length;
        if (shouldUpdate || isLast) {
          const incrementBy = createdCount - lastReported;
          await supabase.rpc("increment_products_processed", {
            uid_input: userId,
            bulkeditid_input: settings.bulkeditid,
            increment_by: incrementBy,
          });
          lastReported = createdCount;
        }
      }

      const { data: historyData } = await supabase
        .from("history_items")
        .select("products_processed, total_products")
        .eq("user_id", userId)
        .eq("bulkeditid", settings.bulkeditid)
        .maybeSingle();
      if (historyData?.products_processed >= historyData?.total_products) {
        await supabase
          .from("history_items")
          .update({ status: "Completed", updated_at: new Date().toISOString() })
          .eq("user_id", userId)
          .eq("bulkeditid", settings.bulkeditid);
      }
      return res.status(200).json({ success: true, message: "Batch processed" });
    } catch (err) {
      console.error("processOptimizeProductsBatchTaskV3 error", err);
      return res.status(500).json({ success: false, message: err.message });
    }
  });
});

