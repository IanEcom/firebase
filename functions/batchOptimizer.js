// Import modelConfig vanuit extern JSON-bestand
const modelConfig = require("./modelConfig.json");


// FIREBASE:
const functions = require("firebase-functions");
const { onRequest } = require("firebase-functions/v2/https");



// GOOGLE:
const { SecretManagerServiceClient } = require("@google-cloud/secret-manager");
// GOOGLE CLOUD TASKS
const { CloudTasksClient } = require("@google-cloud/tasks");
const tasksClient = new CloudTasksClient();
// Secret Manager client
const secretClient = new SecretManagerServiceClient();


// SUPABASE:
const { createClient } = require("@supabase/supabase-js");
let supabaseClient = null;
let supabaseServiceKey = "";
const supabaseUrl = "https://yhrgezxmxjoxpkhpywfw.supabase.co";
/**
* @typedef {import('@supabase/supabase-js').SupabaseClient} SupabaseClient
*/

/**
 * Initialiseert de Supabase-client als deze nog niet bestaat.
 *
 * Haalt de Supabase-service key op uit Google Secret Manager en
 * maakt de Supabase-client aan met die key. Keert terug met
 * de bestaande of nieuw gemaakte client.
 *
 * @return {Promise<SupabaseClient>} De Supabase-client instantie
 */
async function initSupabase() {
  if (!supabaseClient) {
    // Ophalen van de secret
    const [version] = await secretClient.accessSecretVersion({
      name:
        "projects/734399878923/secrets/" +
        "SUPABASE_SERVICE_KEY/versions/latest",
    });
    supabaseServiceKey = version.payload.data.toString();

    // Maak de client met de service key
    supabaseClient = createClient(supabaseUrl, supabaseServiceKey);
    console.log("Supabase client initialized.");
  }
  return supabaseClient;
}

async function createOrUpdateSupabaseHistory(supabaseUserId, bulkeditid, newData) {
  const supabase = await initSupabase();
  const { data: existing, error: selectError } = await supabase
    .from("history_items")
    .select("*")
    .eq("user_id", supabaseUserId)
    .eq("bulkeditid", bulkeditid)
    .single();

  if (selectError && selectError.code !== "PGRST116") {
    console.error("Supabase SELECT error:", selectError);
  }

  if (!existing) {
    const { data: inserted, error: insertError } = await supabase
      .from("history_items")
      .insert([
        {
          user_id: supabaseUserId,
          bulkeditid: bulkeditid,
          ...newData,
        },
      ])
      .select()
      .single();

    if (insertError) {
      console.error("Supabase INSERT error:", insertError);
      return null;
    }
    return inserted;
  } else {
    const { data: updated, error: updateError } = await supabase
      .from("history_items")
      .update({
        ...newData,
        updated_at: new Date().toISOString(),
      })
      .eq("user_id", supabaseUserId)
      .eq("bulkeditid", bulkeditid)
      .select()
      .single();

    if (updateError) {
      console.error("Supabase UPDATE error:", updateError);
      return existing;
    }
    return updated;
  }
}


function getModelAndPrompt(
  fieldName,
  userSelectedModel,
  originalValue,
  settings,
  currentTitle = originalValue,
  currentBody = originalValue,
) {
  const defaultValues = {
    chosenModel: "gpt-3.5-turbo",
    prompt: "",
    systemMessage:
      "Je bent een AI-assistent die helpt bij het optimaliseren " +
      "van productinformatie.",
  };
  let { chosenModel, prompt, systemMessage } = defaultValues;

  if (userSelectedModel && modelConfig[userSelectedModel]) {
    const config = modelConfig[userSelectedModel];
    chosenModel = config.modelId || chosenModel;

    if (config.promptTemplate) {
      prompt = config.promptTemplate
        .replace("{language}", settings.Language)
        .replace("{currentTitle}", currentTitle)
        .replace("{currentBody}", currentBody)
        .replace("{tagsList}", settings.aiTags)
        .replace("{originalValue}", originalValue);
    }

    if (config.systemMessage) {
      systemMessage = config.systemMessage
        .replace("{language}", settings.Language)
        .replace("{currentTitle}", currentTitle)
        .replace("{currentBody}", currentBody);
    }
  }
  console.log(userSelectedModel, chosenModel, prompt, systemMessage);
  return { chosenModel, prompt, systemMessage };
}
const optimizeProductsByIdsBatch = functions.https.onRequest((req, res) => {
  return require("cors")()(req, res, async () => {
    try {
      const { productIds, edits, models, settings } = req.body;

      if (
        !Array.isArray(productIds) ||
        !edits ||
        !models ||
        !settings ||
        !settings.UID
      ) {
        console.warn("❌ Ongeldige input voor optimizeProductsByIdsBatch:", req.body);
        return res.status(400).json({ success: false, message: "Ongeldige input" });
      }

      const userId = settings.UID;
      const bulkeditid = Date.now().toString();
      const batchSize = 10;

      console.log("✅ Ontvangen request voor optimizeProductsByIdsBatch");
      console.log("🧠 Aantal producten:", productIds.length);
      console.log("📝 Edits:", edits);
      console.log("🎯 Models:", models);
      console.log("⚙️ Settings:", settings);

      // voeg batch-tracking info toe
      settings.bulkeditid = bulkeditid;
      settings.startIndex = 0;



      // Maak Supabase history item aan
      await createOrUpdateSupabaseHistory(userId, bulkeditid, {
        status: "Processing",
        type: "AI edit",
        name: settings.name || "AI-edit",
        total_products: productIds.length,
        tokens: 0,
        products_processed: 0,
        output_file: "",
      });

      // Split productIds in batches
      const batches = [];
      for (let i = 0; i < productIds.length; i += batchSize) {
        batches.push(productIds.slice(i, i + batchSize));
      }

      const parent = tasksClient.queuePath("ecomai-3730f", "us-central1", "my-queue");

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        const batchSettings = {
          ...settings,
          startIndex: i * batchSize,
          total_products: productIds.length,
        };

        const taskPayload = {
          productIds: batch,
          edits,
          models,
          settings: batchSettings,
        };

        const task = {
          httpRequest: {
            httpMethod: "POST",
            url: "https://us-central1-ecomai-3730f.cloudfunctions.net/processOptimizeProductsBatchTask",
            headers: { "Content-Type": "application/json" },
            body: Buffer.from(JSON.stringify(taskPayload)).toString("base64"),
          },
        };

        const [cloudTask] = await tasksClient.createTask({ parent, task });
        console.log(`🚀 Batch ${i + 1}/${batches.length} → Task aangemaakt: ${cloudTask.name}`);
      }

      return res.status(200).json({
        success: true,
        message: `✅ ${batches.length} batch-taken aangemaakt.`,
        bulkeditid,
      });
    } catch (err) {
      console.error("❌ Fout in optimizeProductsByIdsBatch:", err);
      return res.status(500).json({ success: false, error: err.message });
    }
  });
});
const processOptimizeProductsBatchTask = onRequest(
  { timeoutSeconds: 300 },
  async (req, res) => {
    const cors = require("cors")({ origin: true });

    return cors(req, res, async () => {
      try {
        const { productIds, edits, models, settings } = req.body;
        const userId = settings.UID;
        const supabase = await initSupabase();

        // OpenAI
        const [version] = await secretClient.accessSecretVersion({
          name: "projects/734399878923/secrets/openAiSecret/versions/latest",
        });
        const openAiKey = version.payload.data.toString();
        const { OpenAI } = require("openai");
        const openai = new OpenAI({ apiKey: openAiKey });

        console.log("⚙️ AI Batch gestart:", { productIds, settings });

        const { data: products, error } = await supabase
          .from("products")
          .select("*")
          .in("id", productIds);

        if (error || !products || products.length === 0) {
          throw new Error("❌ Kan producten niet ophalen uit Supabase.");
        }




        let createdCount = 0;
        let lastReportedCount = 0;


        for (const original of products) {
          let raw = typeof original.product_data === "string"
            ? JSON.parse(original.product_data)
            : original.product_data;

          if (!raw?.product?.title) {
            console.warn("⚠️ Geen geldige product_data:", original.id);
            continue;
          }

          const product = raw.product;

          // Voeg store_id toe aan original product (bewerkte versie bestaat)
          if (settings.store_id) {
            const { error: updateError } = await supabase.rpc("append_store_to_edited_list", {
              original_product_id: original.id,
              store_id_to_add: settings.store_id,
            });

            if (updateError) {
              console.error("❌ Fout bij updaten van edited_versions_for_store_ids:", updateError);
            } else {
              console.log(`📌 Store ${settings.store_id} toegevoegd aan edited_versions_for_store_ids van product ${original.id}`);
            }
          }




          // ✅ Algemene settings
          product.vendor = settings.vendor || product.vendor;
          if (settings.tagAction === "clear") {
            product.tags = "";
          } else if (settings.tagAction === "replace") {
            product.tags = settings.tags || "";
          } else if (settings.tagAction === "add" && settings.tags) {
            const existingTags = (product.tags || "").split(",").map(t => t.trim()).filter(Boolean);
            const newTags = settings.tags.split(",").map(t => t.trim()).filter(Boolean);
            const combinedTags = Array.from(new Set([...existingTags, ...newTags]));
            product.tags = combinedTags.join(", ");
          }

          product.published = settings.published;
          product.status = settings.status || product.status;

          if (Array.isArray(product.variants)) {
            const minQty = parseInt(settings.variant_inventory_qty_min) || 0;
            const maxQty = parseInt(settings.variant_inventory_qty_max) || 0;

            product.variants.forEach((variant) => {
              variant.inventory_policy = settings.variant_inventory_policy || "deny";

              if (maxQty > minQty) {
                const randomQty = Math.floor(Math.random() * (maxQty - minQty + 1)) + minQty;
                variant.inventory_quantity = randomQty;
              } else {
                variant.inventory_quantity = minQty; // fallback
              }

              // ✅ Nieuw: gewichtseenheid
              if (settings.variant_weight_unit) {
                variant.weight_unit = settings.variant_weight_unit;
              }

              // ✅ Nieuw: voorraadbeheer (track_quantity)
              variant.inventory_management = settings.track_quantity ? "shopify" : null;

              // ✅ Nieuw: compare at price berekening
              const price = parseFloat(variant.price || "0");
              const amount = parseFloat(settings.compare_at_amount || "0");

              if (!isNaN(price) && !isNaN(amount)) {
                let comparePrice = null;

                if (settings.compare_at_strategy === "=") {
                  comparePrice = amount;
                } else if (settings.compare_at_strategy === "+") {
                  comparePrice = price + amount;
                } else if (settings.compare_at_strategy === "x") {
                  comparePrice = price * amount;
                }

                if (comparePrice !== null && comparePrice > price) {
                  variant.compare_at_price = comparePrice.toFixed(2);
                }
              }


              // ✅ Prijsvaluta
              if (settings.currency) {
                variant.price_currency = settings.currency;
              }

              // ✅ Compare at valuta (alleen als er een compare_at_price is)
              if (variant.compare_at_price) {
                variant.compare_at_price_currency = settings.currency;
              }


            });
          }





          // ✅ Google Shopping settings
          product.g_category = settings.g_category || product.g_category;
          product.g_gender = settings.g_gender || product.g_gender;
          product.g_age_group = settings.g_age_group || product.g_age_group;
          product.g_condition = settings.g_condition || product.g_condition;
          product.g_custom_product = settings.g_custom_product || product.g_custom_product;
          product.g_label0 = settings.g_label0 || product.g_label0;
          product.g_label1 = settings.g_label1 || product.g_label1;
          product.g_label2 = settings.g_label2 || product.g_label2;
          product.g_label3 = settings.g_label3 || product.g_label3;
          product.g_label4 = settings.g_label4 || product.g_label4;

          // === TITLE ===
          if (edits["ai-edit-title-checkbox"] === "on" && models["ai-title-model-select"]) {
            const { chosenModel, prompt, systemMessage } = getModelAndPrompt(
              "Title",
              models["ai-title-model-select"],
              product.title,
              settings
            );
            if (prompt) {
              try {
                const completion = await openai.chat.completions.create({
                  model: chosenModel,
                  messages: [
                    { role: "system", content: systemMessage },
                    { role: "user", content: prompt },
                  ],
                });
                const newTitle = completion.choices?.[0]?.message?.content?.trim();
                if (newTitle) {
                  console.log("🆕 AI Titel:", newTitle);
                  product.title = newTitle;
                }
              } catch (err) {
                console.error("❌ Titel AI-fout:", err);
              }
            }
          }

          // === BODY_HTML ===
          if (edits["ai-edit-body-checkbox"] === "on" && models["ai-body(html)-model-select"]) {
            const { chosenModel, prompt, systemMessage } = getModelAndPrompt(
              "Body (HTML)",
              models["ai-body(html)-model-select"],
              product.body_html || "",
              settings,
              product.title
            );
            if (prompt) {
              try {
                const completion = await openai.chat.completions.create({
                  model: chosenModel,
                  messages: [
                    { role: "system", content: systemMessage },
                    { role: "user", content: prompt },
                  ],
                });
                const newBody = completion.choices?.[0]?.message?.content?.trim();
                if (newBody) {
                  console.log("🆕 AI Body (HTML):", newBody.slice(0, 100) + "...");
                  product.body_html = newBody;
                }
              } catch (err) {
                console.error("❌ Body HTML AI-fout:", err);
              }
            }
          }

          // === SEO TITLE ===
          if (edits["ai-edit-seo-title-checkbox"] === "on" && models["ai-seotitle-model-select"]) {
            const { chosenModel, prompt, systemMessage } = getModelAndPrompt(
              "SEO Title",
              models["ai-seotitle-model-select"],
              product.title,
              settings,
              product.title
            );
            if (prompt) {
              try {
                const completion = await openai.chat.completions.create({
                  model: chosenModel,
                  messages: [
                    { role: "system", content: systemMessage },
                    { role: "user", content: prompt },
                  ],
                });
                const newSeoTitle = completion.choices?.[0]?.message?.content?.trim();
                if (newSeoTitle) {
                  console.log("🆕 AI SEO Title:", newSeoTitle);
                  product.seo_title = newSeoTitle;
                  if (Array.isArray(product.images)) {
                    product.images.forEach((img) => {
                      img.alt = newSeoTitle;
                    });
                  }
                }
              } catch (err) {
                console.error("❌ SEO Title AI-fout:", err);
              }
            }
          }

          // === SEO DESCRIPTION ===
          if (edits["ai-edit-seo-body-checkbox"] === "on" && models["ai-seodescription-model-select"]) {
            const { chosenModel, prompt, systemMessage } = getModelAndPrompt(
              "SEO Description",
              models["ai-seodescription-model-select"],
              product.body_html || "",
              settings,
              product.title
            );
            if (prompt) {
              try {
                const completion = await openai.chat.completions.create({
                  model: chosenModel,
                  messages: [
                    { role: "system", content: systemMessage },
                    { role: "user", content: prompt },
                  ],
                });
                const newSeoDesc = completion.choices?.[0]?.message?.content?.trim();
                if (newSeoDesc) {
                  console.log("🆕 AI SEO Description:", newSeoDesc.slice(0, 100) + "...");
                  product.seo_description = newSeoDesc;
                }
              } catch (err) {
                console.error("❌ SEO Description AI-fout:", err);
              }
            }
          }

          // === GOOGLE CATEGORY ===
          if (edits["ai-edit-g-category-checkbox"] === "on" && models["ai-g-category-select"]) {
            const { chosenModel, prompt, systemMessage } = getModelAndPrompt(
              "Google Shopping / Google Product Category",
              models["ai-g-category-select"],
              product.title,
              settings
            );
            if (prompt) {
              try {
                const completion = await openai.chat.completions.create({
                  model: chosenModel,
                  messages: [
                    { role: "system", content: systemMessage },
                    { role: "user", content: prompt },
                  ],
                });
                const newCategory = completion.choices?.[0]?.message?.content?.trim();
                if (newCategory) {
                  console.log("🛒 AI Google Category:", newCategory);
                  product.product_category = newCategory;
                }
              } catch (err) {
                console.error("❌ Google Category AI-fout:", err);
              }
            }
          }

          // === GENDER ===
          if (edits["ai-g-gender-checkbox"] === "on") {
            const { chosenModel, prompt, systemMessage } = getModelAndPrompt(
              "Google Shopping / Gender",
              "genderModel1",
              product.title,
              settings
            );
            if (prompt) {
              try {
                const completion = await openai.chat.completions.create({
                  model: chosenModel,
                  messages: [
                    { role: "system", content: systemMessage },
                    { role: "user", content: prompt },
                  ],
                });
                const newGender = completion.choices?.[0]?.message?.content?.trim();
                if (newGender) {
                  console.log("👤 AI Gender:", newGender);
                  product.g_gender = newGender;
                }
              } catch (err) {
                console.error("❌ Gender AI-fout:", err);
              }
            }
          }

          // === TAGS ===
          if (edits["ai-edit-tags-checkbox"] === "on") {
            const tagFieldKeys = ["aiTags", "aiTags1", "aiTags2", "aiTags3"];
            const tagsResults = [];

            for (const key of tagFieldKeys) {
              const tagList = settings[key];
              if (!tagList) continue;

              // Gebruik een gekloonde settings object waarin we aiTags overschrijven
              const modifiedSettings = {
                ...settings,
                aiTags: tagList,
              };

              const { chosenModel, prompt, systemMessage } = getModelAndPrompt(
                "Tags",
                "tagsModel1",
                product.seo_description || "",
                modifiedSettings,
                product.title
              );

              if (prompt) {
                try {
                  const completion = await openai.chat.completions.create({
                    model: chosenModel,
                    messages: [
                      { role: "system", content: systemMessage },
                      { role: "user", content: prompt },
                    ],
                  });

                  const newTags = completion.choices?.[0]?.message?.content?.trim();
                  if (newTags) {
                    tagsResults.push(newTags);
                    console.log(`🏷️ AI Tags (${key}):`, newTags);
                  }
                } catch (err) {
                  console.error(`❌ Tags AI-fout (${key}):`, err);
                }
              }
            }

            if (tagsResults.length > 0) {
              const combined = [product.tags || "", ...tagsResults].filter(Boolean).join(", ");
              product.tags = combined;
              console.log("🏷️ Gecombineerde AI Tags:", combined);
            }
          }



          // === VARIANT OPTION NAME TRANSLATIONS ===
          if (edits["transl-options-checkbox"] === "on") {
            const optionNames = (product.options || []).map(opt => opt.name).filter(Boolean);

            if (optionNames.length === 0) {
              console.log("⚠️ Geen optie-namen om te vertalen.");
            } else {
              const translationPrompt = `Vertaal de volgende variantoptie-namen naar het ${settings.Language}. Geef alleen de vertaalde namen gescheiden door komma's, zonder uitleg. Origineel: ${optionNames.join(", ")}`;

              console.log("🌍 Prompt voor vertaling:", translationPrompt);

              try {
                const completion = await openai.chat.completions.create({
                  model: "gpt-3.5-turbo",
                  messages: [
                    { role: "system", content: "Je bent een vertaal-assistent." },
                    { role: "user", content: translationPrompt },
                  ],
                  temperature: 0.3,
                });

                const translatedText = completion.choices?.[0]?.message?.content?.trim();
                if (!translatedText) {
                  console.warn("⚠️ Geen vertaling ontvangen.");
                } else {
                  const translatedNames = translatedText.split(",").map((t) => t.trim());
                  console.log("🌍 Vertaalde variantoptie-namen:", translatedNames);

                  product.options?.forEach((opt, index) => {
                    if (opt.name === "Title") return; // ⛔ Sla over
                    if (translatedNames[index]) {
                      console.log(`🔠 ${opt.name} → ${translatedNames[index]}`);
                      opt.name = translatedNames[index];
                    }
                  });

                }
              } catch (err) {
                console.error("❌ Fout bij vertaling van optie-namen:", err);
              }
            }
          }

          // === HANDLE ===
          if (edits["ai-edit-seo-title-checkbox"] === "on" || edits["ai-edit-title-checkbox"] === "on") {
            const baseHandle = product.seo_title || product.title;
            if (baseHandle) {
              product.handle = baseHandle
                .toLowerCase()
                .replace(/[^a-z0-9\s-]/g, "")       // verwijder speciale tekens
                .replace(/\s+/g, "-")               // spaties naar streepjes
                .replace(/-+/g, "-")                // dubbele streepjes naar enkel
                .replace(/^-+|-+$/g, "");           // verwijder leidende/afsluitende streepjes
              console.log("🔗 Nieuwe handle:", product.handle);
            }
          }




          // === VARIANT OPTION VALUES VERTALING ===
          if (edits["transl-names-checkbox"] === "on") {
            const optionValuesToTranslate = new Set();

            // Verzamel alle unieke waardes uit de options.values
            product.options?.forEach(opt => {
              (opt.values || []).forEach(value => {
                if (value) optionValuesToTranslate.add(value);
              });
            });

            if (optionValuesToTranslate.size === 0) {
              console.log("⚠️ Geen variant option values gevonden om te vertalen.");
            } else {
              const valuesArray = Array.from(optionValuesToTranslate);
              const prompt = `Vertaal de volgende productvariantwaardes naar het ${settings.Language}. Geef alleen de vertaalde woorden gescheiden door komma's zonder uitleg:\n\n${valuesArray.join(", ")}`;

              console.log("🧠 Prompt voor optie-waardes:", prompt);

              try {
                const completion = await openai.chat.completions.create({
                  model: "gpt-3.5-turbo",
                  messages: [
                    { role: "system", content: "Je bent een vertaalassistent." },
                    { role: "user", content: prompt },
                  ],
                  temperature: 0.3,
                });

                const translatedText = completion.choices?.[0]?.message?.content?.trim() || "";
                const translatedValues = translatedText.split(",").map((val) => val.trim());

                if (translatedValues.length !== valuesArray.length) {
                  console.warn("⚠️ Aantal vertalingen komt niet overeen met originele waardes.");
                } else {
                  const translationMap = {};
                  valuesArray.forEach((original, index) => {
                    translationMap[original] = translatedValues[index];
                  });

                  console.log("🌐 Vertalingen van variantwaardes:", translationMap);

                  // Pas aan in options.values
                  product.options?.forEach((opt) => {
                    if (opt.name === "Title") return; // ⛔ Sla over
                    opt.values = opt.values?.map((v) => translationMap[v] || v);
                  });


                  // Pas aan in elke variant
                  product.variants?.forEach((variant) => {
                    ["option1", "option2", "option3"].forEach((key) => {
                      if (variant[key] && translationMap[variant[key]]) {
                        variant[key] = translationMap[variant[key]];
                      }
                    });
                  });

                }
              } catch (err) {
                console.error("❌ Fout bij vertaling van variantwaardes:", err);
              }
            }
          }


          // ✅ Prijsaanpassing en afronden naar vaste decimalen (zoals 0.95)
          product.variants?.forEach((variant) => {
            let price = parseFloat(variant.price || "0");

            // 1. Prijsaanpassing (positief of negatief)
            if (settings.adjustPrices && settings.adjustmentAmount) {
              const adjust = parseFloat(settings.adjustmentAmount);
              if (!isNaN(adjust)) {
                price += adjust;
              }
            }

            // 2. Afronden naar vaste decimalen (zoals .95)
            if (settings.roundPrices && settings.roundingNumber) {
              const decimalTarget = parseFloat(settings.roundingNumber); // bv. 0.95
              const whole = Math.floor(price); // neem hele euro's
              let rounded = whole + decimalTarget;

              if (rounded < price) {
                // als dat te laag is, ga een euro omhoog
                rounded = whole + 1 + decimalTarget;
              }

              price = rounded;
            }

            variant.price = price.toFixed(2);
          });


          // === SKU GENERATIE ===
          if (settings.vendor) {
            const vendorCode = settings.vendor.substring(0, 2).toUpperCase();
            const now = new Date();
            const dd = String(now.getDate()).padStart(2, "0");
            const mm = String(now.getMonth() + 1).padStart(2, "0");
            const yy = String(now.getFullYear()).slice(-2);
            const hh = String(now.getHours()).padStart(2, "0");
            const min = String(now.getMinutes()).padStart(2, "0");
            const ss = String(now.getSeconds()).padStart(2, "0");
            const rand1 = Math.floor(Math.random() * 90 + 10); // 2 random cijfers
            const rand2 = Math.floor(Math.random() * 90 + 10); // nog 2 random cijfers

            const baseSku = `${vendorCode}-${rand1}${dd}${mm}${yy}-${hh}${min}${ss}-${rand2}`;

            product.variants?.forEach((variant, index) => {
              variant.sku = `${baseSku}-${index + 1}`;
            });

            console.log("🔢 SKU's gegenereerd:", product.variants?.map(v => v.sku));
          }



          raw.product = product;


          // ⬇️ Nieuw product toevoegen
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

          const shouldUpdate = createdCount - lastReportedCount >= 5;
          const isLastProduct = createdCount === products.length;

          if (shouldUpdate || isLastProduct) {
            const incrementBy = createdCount - lastReportedCount;

            try {
              const { error } = await supabase.rpc("increment_products_processed", {
                uid_input: userId,
                bulkeditid_input: settings.bulkeditid,
                increment_by: incrementBy,
              });

              if (error) {
                console.error(`❌ Fout bij RPC update van +${incrementBy}:`, error);
              } else {
                console.log(`📊 Supabase RPC update: +${incrementBy}`);
              }

              lastReportedCount = createdCount;
            } catch (rpcErr) {
              console.error("❌ RPC update failed:", rpcErr);
            }
          }
        }

        // ✅ Finale update van de status indien alles verwerkt is
        const { data: historyData, error: historyError } = await supabase
          .from("history_items")
          .select("products_processed, total_products")
          .eq("user_id", userId)
          .eq("bulkeditid", settings.bulkeditid)
          .maybeSingle();

        if (historyError) {
          console.error("❌ Fout bij ophalen history-item:", historyError);
        } else if (historyData?.products_processed >= historyData?.total_products) {
          console.log("🏁 Alle producten verwerkt → status wordt op 'Completed' gezet");

          const { error: statusError } = await supabase
            .from("history_items")
            .update({
              status: "Completed",
              updated_at: new Date().toISOString(),
            })
            .eq("user_id", userId)
            .eq("bulkeditid", settings.bulkeditid);

          if (statusError) {
            console.error("❌ Fout bij updaten naar status 'Completed':", statusError);
          } else {
            console.log("✅ Status succesvol op 'Completed' gezet");
          }
        }

        return res.status(200).json({ success: true, message: "Batch verwerkt." });

      } catch (err) {
        console.error("❌ Fout in processOptimizeProductsBatchTask:", err);
        return res.status(500).json({ success: false, error: err.message });
      }






    });
  }
);
// insertSupabaseProductData.js
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

  if (error) {
    console.error("❌ insertSupabaseProductData error:", error);
    throw error;
  }

  return data?.[0];
}

module.exports = {
  initSupabase,
  getModelAndPrompt,
  insertSupabaseProductData,
  optimizeProductsByIdsBatch,
  processOptimizeProductsBatchTask,
};

