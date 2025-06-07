const admin = require("firebase-admin");
admin.initializeApp();

const Papa = require("papaparse");
const fetch = require("node-fetch");
const cors = require("cors")({ origin: true });
const cheerio = require("cheerio");
const { v4: uuidv4 } = require("uuid");

const {
  initSupabase,
  getModelAndPrompt,
  optimizeProductsByIdsBatch,
  processOptimizeProductsBatchTask,
} = require("./batchOptimizer");

const {
  optimizeProductsByIdsBatchV3,
  processOptimizeProductsBatchTaskV3,
} = require("./batchOptimizer-v3");

exports.optimizeProductsByIdsBatch = optimizeProductsByIdsBatch;
exports.processOptimizeProductsBatchTask = processOptimizeProductsBatchTask;
exports.optimizeProductsByIdsBatchV3 = optimizeProductsByIdsBatchV3;
exports.processOptimizeProductsBatchTaskV3 = processOptimizeProductsBatchTaskV3;



// FIREBASE:
const functions = require("firebase-functions");
const functionsV2 = require("firebase-functions/v2/https");
const { onSchedule } = require("firebase-functions/v2/scheduler");



// GOOGLE:
const { SecretManagerServiceClient } = require("@google-cloud/secret-manager");
const { Storage } = require("@google-cloud/storage");
// GOOGLE CLOUD TASKS
const { CloudTasksClient } = require("@google-cloud/tasks");
const tasksClient = new CloudTasksClient();
// Google Cloud Storage client
const storage = new Storage();
const bucketName = "ecomai-3730f.firebasestorage.app";
// Secret Manager client
const secretClient = new SecretManagerServiceClient();


// SUPABASE:
const { subDays, formatISO } = require("date-fns");







async function initSupabaseWithRetry(retries = 3, delayMs = 1000) {
  for (let i = 0; i < retries; i++) {
    try {
      return await initSupabase();
    } catch (err) {
      console.warn(`⚠️ Supabase init failed (try ${i + 1}):`, err.message);
      if (i < retries - 1) await new Promise((res) => setTimeout(res, delayMs));
    }
  }
  throw new Error("❌ Supabase init failed after retries");
}


/*
 * [[-- MAIN FUNCTIONS --]]
*/

exports.addHistoryItem = functions.https.onRequest((req, res) => {
  // CORS Headers
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  cors(req, res, async () => {
    try {
      if (req.method !== "POST") {
        return res.status(405).json({ message: "Method Not Allowed" });
      }

      const { csvUrl, settings, productData } = req.body;

      if (
        !csvUrl ||
        !settings ||
        !settings.UID ||
        !settings.name
      ) {
        return res.status(400).json({ message: "Ongeldige gegevens ontvangen" });
      }

      const supabaseUserId = settings.UID || null;
      const name = settings.name || "Naamloos";

      // Genereer bulkeditid met datum
      const bulkeditid = Date.now().toString();

      // Definieer de variabele totalProducts (als je productData verwacht)
      const totalProducts = productData ? productData.length : 0;

      // Definieer de nieuwe data voor history_items
      const newData = {
        type: "Edit",
        status: "Finished",
        tokens: 0, // Pas aan indien nodig
        products_processed: 0, // Pas aan indien nodig
        total_products: totalProducts, // Nu correct gedefinieerd
        output_file: csvUrl,
        name: name,
      };

      // Voeg het history-item toe of werk het bij
      const historyItem = await createOrUpdateSupabaseHistory(
        supabaseUserId,
        bulkeditid,
        newData,
      );

      if (historyItem) {
        return res.status(200).json({
          message: "History item toegevoegd",
          historyItem,
        });
      } else {
        return res.status(500).json({
          message: "Fout bij het toevoegen van history item",
        });
      }
    } catch (error) {
      console.error("Onverwachte fout:", error);
      return res.status(500).json({ message: "Interne serverfout." });
    }
  });
});



exports.handleProducts = functions.https.onRequest((req, res) => {
  // Stel CORS headers in
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  cors(req, res, async () => {
    try {
      console.log("Ontvangen verzoek:", req.body);

      if (req.method !== "POST") {
        console.log("Ongeldige HTTP-methode:", req.method);
        return res.status(405).json({ message: "Method Not Allowed" });
      }

      const products = req.body.products;
      const settings = req.body.settings;

      if (!products || !Array.isArray(products) || !settings || !settings.UID || !settings.name) {
        console.log("Ongeldige gegevens ontvangen:", req.body);
        return res.status(400).json({ message: "Ongeldige gegevens ontvangen" });
      }

      // Genereer een unieke bulkeditid, bijvoorbeeld op basis van timestamp
      const bulkeditid = Date.now().toString();

      // Stel het payload object samen voor de Cloud Task
      const taskPayload = { products, settings, bulkeditid };

      // Maak de Cloud Task aan
      const parent = tasksClient.queuePath("ecomai-3730f", "us-central1", "my-queue");
      // Let op: gebruik hier de juiste URL voor de nieuwe Cloud Task functie
      const processUrl = "https://us-central1-ecomai-3730f.cloudfunctions.net/processHandleProductsTask";
      const task = {
        httpRequest: {
          httpMethod: "POST",
          url: processUrl,
          headers: { "Content-Type": "application/json" },
          body: Buffer.from(JSON.stringify(taskPayload)).toString("base64"),
        },
        retryConfig: {
          maxAttempts: 3,
          maxRetryDuration: { seconds: 600 },
          minBackoff: { seconds: 5 },
          maxBackoff: { seconds: 60 },
        },
      };

      const [cloudTask] = await tasksClient.createTask({ parent, task });
      console.log("Cloud Task created:", cloudTask.name);

      // Geef direct de bulkeditid terug zodat de frontend kan polleren
      return res.status(200).json({ bulkeditid });
    } catch (error) {
      console.error("Onverwachte fout in handleProducts:", error);
      return res.status(500).json({ message: "Interne serverfout." });
    }
  });
});


exports.processHandleProductsTask = functionsV2.onRequest(
  { timeoutSeconds: 300, memory: "712MiB" },
  async (req, res) => {
    return cors(req, res, async () => {
      try {
        if (req.method !== "POST") {
          return res.status(405).json({ message: "Method Not Allowed" });
        }

        const { products, settings, bulkeditid } = req.body;
        if (!products || !settings || !bulkeditid || !settings.UID || !settings.name) {
          return res.status(400).json({
            success: false,
            error: "Missing fields: products, settings.UID, settings.name, bulkeditid",
          });
        }

        const results = [];
        const globalSeenProductIds = new Set();
        let totalProducts = 0;

        // 🔁 Verwerk alle producten/collecties parallel
        await Promise.allSettled(
          products.map(async (input) => {
            const { url, quantity = null, sorting = null } = input;

            if (!url) {
              results.push({ url, status: "Geen URL ingevoerd." });
              return;
            }

            try {
              const productUrls = [];

              if (url.includes("/products/")) {
                const urlObj = new URL(url);
                urlObj.pathname = `${urlObj.pathname}.json`;
                productUrls.push(urlObj.toString());
              } else if (url.includes("/collections/")) {
                const urlObj = new URL(url);
                if (sorting) {
                  urlObj.searchParams.set("sort_by", sorting); // ✅ belangrijke fix
                }

                let currentPage = 1;
                let totalFetched = 0;
                const desiredQuantity = +quantity > 0 ? +quantity : Infinity;

                while (totalFetched < desiredQuantity) {
                  urlObj.searchParams.set("page", currentPage);
                  const response = await fetch(urlObj.toString());
                  const html = await response.text();

                  if (!html || html.trim().length === 0) break;

                  const $ = cheerio.load(html);
                  const productLinks = [];
                  $("a[href*='/products/']").each((_, el) => {
                    let href = $(el).attr("href");
                    if (href) {
                      if (href.startsWith("//")) href = "https:" + href;
                      else if (href.startsWith("/")) href = `${urlObj.origin}${href}`;
                      else if (!href.startsWith("http")) href = `${urlObj.origin}/${href}`;
                      productLinks.push(href);
                    }
                  });

                  const uniqueLinks = [...new Set(productLinks)];
                  if (uniqueLinks.length === 0) break;

                  for (const link of uniqueLinks) {
                    if (totalFetched >= desiredQuantity) break;
                    if (!productUrls.includes(link)) {
                      productUrls.push(link);
                      totalFetched++;
                    }
                  }

                  currentPage++;
                }

                if (productUrls.length === 0) {
                  results.push({ url, status: "Geen producten gevonden in collectie." });
                  return;
                }

                if (quantity && !isNaN(quantity) && quantity > 0) {
                  productUrls.splice(quantity);
                }
              } else {
                results.push({ url, status: "Onbekend URL type." });
                return;
              }

              const fetchProduct = async (productUrl) => {
                try {
                  const htmlRes = await fetch(productUrl);
                  const rawHtml = await htmlRes.text();
                  const langMatch = rawHtml.match(/<html[^>]+lang=["']?([a-zA-Z-]+)["']?/);
                  const sourceCountry = langMatch ? langMatch[1].toLowerCase() : null;

                  const productUrlObj = new URL(productUrl);
                  if (!productUrlObj.pathname.endsWith(".json")) {
                    productUrlObj.pathname += ".json";
                  }

                  const jsonUrl = productUrlObj.toString();
                  const response = await fetch(jsonUrl);
                  if (!response.ok) return null;

                  const data = await response.json();
                  const product = data?.product;
                  if (!product || globalSeenProductIds.has(product.id)) return null;

                  globalSeenProductIds.add(product.id);

                  return {
                    ...product,
                    source_domain: productUrlObj.hostname.replace(/^www\./, ""),
                    source_country: sourceCountry,
                  };
                } catch (err) {
                  console.warn("❌ Fout bij ophalen product:", productUrl, err.message);
                  return null;
                }
              };

              const fetched = await Promise.allSettled(productUrls.map(fetchProduct));
              const successfulProducts = fetched
                .map((r) => (r.status === "fulfilled" ? r.value : null))
                .filter(Boolean);

              if (url.includes("/collections/") && sorting === "best-selling") {
                successfulProducts.forEach((product, i) => {
                  product.ranking = i + 1;
                });
              }

              results.push({
                url,
                status: "Success",
                data: successfulProducts,
              });

              totalProducts += successfulProducts.length;
            } catch (err) {
              console.error(`❌ Fout bij verwerken ${url}:`, err.message);
              results.push({ url, status: `Fout: ${err.message}` });
            }
          })
        );

        const newData = {
          type: "Import (From urls)",
          status: "Finished",
          tokens: 0,
          products_processed: totalProducts,
          total_products: totalProducts,
          output_file: "",
          name: settings.name,
          product_data: results,
        };

        const historyItem = await createOrUpdateSupabaseHistory(settings.UID, bulkeditid, newData);
        if (!historyItem) {
          return res.status(500).json({ message: "Fout bij opslaan history item" });
        }

        console.log(`📦 History item opgeslagen met ${totalProducts} producten`);
        return res.status(200).json({ bulkeditid });
      } catch (err) {
        console.error("❌ Onverwachte fout in processHandleProductsTask:", err);
        return res.status(500).json({ message: "Interne serverfout." });
      }
    });
  }
);






/**
 * KORTE FUNCTIE
 * - Ontvangt request van frontend
 * - Maakt 'Bulk edits' item (Status: Processing)
 * - Maakt Cloud Task aan
 * - Stuurt direct response terug
 */
exports.optimizeShopifyCsv = functions.https.onRequest((req, res) => {
  return cors(req, res, async () => {
    try {
      console.log("Ontvangen verzoek (korte functie):", req.body);

      // Haal csvContent, csvUrl, edits, models en settings op
      let { csvContent, csvUrl, edits, models, settings } = req.body;

      // Als csvContent niet is meegegeven maar wel csvUrl,
      // haal dan de CSV-inhoud op
      if (!csvContent && csvUrl) {
        console.log("csvContent ontbreekt, haal op via csvUrl:", csvUrl);
        const fetchResponse = await fetch(csvUrl);
        if (!fetchResponse.ok) {
          throw new Error("Kon CSV niet ophalen via de meegegeven URL");
        }
        csvContent = await fetchResponse.text();
      }
      // Basisvalidatie
      if (!csvContent || !edits || !models || !settings) {
        console.error("Ontbrekende velden:" +
          "csvContent (of csvUrl), edits, models, settings");
        return res.status(400).json({
          success: false,
          error: "Missing fields:" +
            "csvContent (of csvUrl), edits, models, settings",
        });
      }

      // Supabase user ID
      const supabaseUserId = settings.UID || null;

      const vendor = settings.vendor || "naamloos";
      const bulkeditid = Date.now().toString();
      const name = settings.name || vendor;


      // Stop de bulkeditid in settings (zodat de async functie hem ook weet)
      settings.bulkeditid = bulkeditid;

      // Nieuw: startIndex toevoegen voor batchverwerking
      settings.startIndex = 0;


      // -- NIEUW -- ook in Supabase wegschrijven
      if (supabaseUserId) {
        await createOrUpdateSupabaseHistory(supabaseUserId, bulkeditid, {
          status: "Processing",
          type: "AI edit",
          tokens: 0,
          products_processed: 0,
          total_products: 0,
          output_file: "",
          name: name,
        });
      }

      // CLOUDTASKS AANMAKEN
      const parent = tasksClient.queuePath(
        "ecomai-3730f",
        "us-central1",
        "my-queue",
      );

      const taskPayload = {
        csvContent,
        edits,
        models,
        settings,
      };

      // URL van de asynchrone functie
      const processOptimizeUrl =
        "https://us-central1-ecomai-3730f.cloudfunctions.net/processOptimizeShopifyCsvTask";

      const task = {
        httpRequest: {
          httpMethod: "POST",
          url: processOptimizeUrl,
          headers: { "Content-Type": "application/json" },
          body: Buffer.from(JSON.stringify(taskPayload)).toString("base64"),
        },
        retryConfig: {
          maxAttempts: 5,
          maxRetryDuration: { seconds: 3600 },
          minBackoff: { seconds: 5 },
          maxBackoff: { seconds: 60 },
        },
      };

      // Task aanmaken
      const [cloudTask] = await tasksClient.createTask({ parent, task });
      console.log("Cloud Task created:", cloudTask.name);

      // Meteen response terug naar frontend
      return res.status(200).json({
        success: true,
        message: "Task is aangemaakt en wordt asynchroon verwerkt.",
        bulkeditid: bulkeditid,
      });
    } catch (err) {
      console.error("Fout in optimizeShopifyCsv (korte functie):", err);
      return res.status(500).json({
        success: false,
        error: err.toString(),
      });
    }
  });
});

/**
 * ASYNCHRONE FUNCTIE
 * - Wordt door Cloud Tasks aangeroepen
 * - Bevat alle “zware” CSV/AI/Memberstack-logica
 * - Updated tussentijds elke 5 producten "ProductsProcessed" en "Tokens"
 */
const { onRequest } = require("firebase-functions/v2/https");

exports.processOptimizeShopifyCsvTask = onRequest(
  { timeoutSeconds: 300 },
  async (req, res) => {
    return cors(req, res, async () => {
      try {
        console.log("Ontvangen verzoek (async functie):", req.body);

        const { csvContent, edits, models, settings } = req.body;
        console.log("Settings ontvangen in async functie:", settings);
        if (!csvContent || !edits || !models || !settings) {
          console.error("Missing fields in the request body.");
          return res.status(400).json({
            success: false,
            error: "Missing fields: csvContent, edits, models, settings",
          });
        }

        // Pak user- en tokengegevens uit settings
        const supabaseUserId = settings.UID || null;
        const tokensPerProduct = settings.tokensPerProduct || 0;
        const bulkeditid = settings.bulkeditid || Date.now().toString();
        const sourceType = settings.source_type || "other";
        const sourceCountry = settings.source_country || null;
        const editType = "ai-edit";



        // Gebruik cumulatieve waarden als deze al aanwezig zijn, anders starten we op 0.
        let productsProcessedSoFar = settings.productsProcessedSoFar ? parseInt(settings.productsProcessedSoFar, 10) : 0;
        let tokensUsedSoFar = settings.tokensUsedSoFar ? parseInt(settings.tokensUsedSoFar, 10) : 0;


        console.log("Supabase userId:", supabaseUserId);

        // 1. Haal de OpenAI API-key op uit Secret Manager
        const [version] = await secretClient.accessSecretVersion({
          name: "projects/734399878923/secrets/openAiSecret/versions/latest",
        });
        const openAiKey = version.payload.data.toString();

        // 2. Initialiseer OpenAI
        const { OpenAI } = require("openai");
        const openai = new OpenAI({ apiKey: openAiKey });


        // 3. Parseer CSV
        const parsedCsv = Papa.parse(csvContent, { header: true });
        const rows = parsedCsv.data;
        console.log(`CSV succesvol geparsed met ${rows.length} rijen.`);

        // 4. Groepeer rijen per originele Handle
        const productsMap = {};
        rows.forEach((row) => {
          const handle = row["Handle"];
          if (!productsMap[handle]) {
            productsMap[handle] = {
              productRows: [],
              variantRows: [],
              otherRows: [],
            };
          }
          if (row["Title"] && row["Title"].trim() !== "") {
            productsMap[handle].productRows.push(row);
          } else if (row["Option1 Value"] && row["Option1 Value"].trim() !== "") {
            productsMap[handle].variantRows.push(row);
          } else {
            productsMap[handle].otherRows.push(row);
          }
        });

        // 5. Bepaal welke velden te optimaliseren
        const productFieldsToOptimize = [];
        if (edits["ai-edit-title-checkbox"] === "on") {
          productFieldsToOptimize.push("Title");
        }
        if (edits["ai-edit-body-checkbox"] === "on") {
          productFieldsToOptimize.push("Body (HTML)");
        }
        if (edits["ai-edit-seo-title-checkbox"] === "on") {
          productFieldsToOptimize.push("SEO Title");
        }
        if (edits["ai-edit-seo-body-checkbox"] === "on") {
          productFieldsToOptimize.push("SEO Description");
        }
        if (edits["ai-edit-g-category-checkbox"] === "on") {
          productFieldsToOptimize.push("Google Shopping / Google Product Category");
        }
        if (edits["ai-g-gender-checkbox"] === "on") {
          productFieldsToOptimize.push("Google Shopping / Gender");
        }
        if (edits["ai-edit-tags-checkbox"] === "on") {
          productFieldsToOptimize.push("Tags");
        }
        if (edits["transl-names-checkbox"] === "on") {
          productFieldsToOptimize.push("Option1 Name", "Option2 Name", "Option3 Name");
        }
        console.log("Velden te optimaliseren:", productFieldsToOptimize);

        // Verkrijg de lijst met alle handles en bepaal totaal aantal producten
        const allHandles = Object.keys(productsMap);
        const totalProducts = allHandles.length;

        // Werk de totale aantal producten bij in de databronnen (alleen bij de eerste batch)
        if (supabaseUserId && productsProcessedSoFar === 0) {
          await createOrUpdateSupabaseHistory(supabaseUserId, bulkeditid, {
            total_products: totalProducts,
          });
        }

        // Bepaal de batchgrenzen
        const batchSize = 10; // Pas dit aan op basis van de verwachte verwerkingstijd per product
        const startIndex = settings.startIndex ? parseInt(settings.startIndex, 10) : 0;
        const endIndex = Math.min(startIndex + batchSize, totalProducts);
        console.log(`Verwerk producten van index ${startIndex} tot ${endIndex - 1} van ${totalProducts}`);

        // Houd een set bij voor reeds verwerkte (nieuwe) handles binnen deze batch
        const processedUpdatedHandles = new Set();

        // 6. Verwerk de productgroepen in de huidige batch
        for (let i = startIndex; i < endIndex; i++) {
          const handle = allHandles[i];
          if (!Object.prototype.hasOwnProperty.call(productsMap, handle)) continue;
          const { productRows, variantRows, otherRows } = productsMap[handle];
          if (productRows.length === 0) {
            console.warn(`Geen productrijen voor handle ${handle}.`);
            continue;
          }

          const productRow = productRows[0];
          console.log(`Verwerken product voor handle ${handle}:`, productRow);

          let newTitle = productRow["Title"];
          let newHandle = null;
          let newBody = productRow["Body (HTML)"];

          // 6.a. Bewerk productvelden met OpenAI
          for (const fieldName of productFieldsToOptimize) {
            if (
              [
                "Title",
                "Body (HTML)",
                "SEO Title",
                "SEO Description",
                "Google Shopping / Google Product Category",
                "Google Shopping / Gender",
                "Tags",
                "Option1 Name",
                "Option2 Name",
                "Option3 Name",
              ].includes(fieldName)
            ) {
              const originalValue = productRow[fieldName];
              if (!originalValue) {
                console.log(`Veld ${fieldName} is leeg. Overslaan.`);
                continue;
              }

              const modelKey =
                "ai-" +
                fieldName.toLowerCase().replace(/[\s/]+/g, "") +
                "-model-select";
              let userSelectedModel;
              if (fieldName === "Google Shopping / Gender") {
                userSelectedModel = "genderModel1";
              } else if (fieldName === "Google Shopping / Google Product Category") {
                userSelectedModel = "gCatModel1";
              } else if (fieldName === "Tags") {
                userSelectedModel = "tagsModel1";
              } else if (["Option1 Name", "Option2 Name", "Option3 Name"].includes(fieldName)) {
                userSelectedModel = "variantOptionNamesModel1";
              } else {
                userSelectedModel = models[modelKey];
                if (!userSelectedModel) {
                  console.warn(`Model key ${modelKey} not found.`);
                  continue;
                }
              }

              const currentTitle = newTitle;
              let currentBody = newBody;
              if (fieldName === "Body (HTML)") {
                currentBody = originalValue;
              }

              const modelAndPrompt = getModelAndPrompt(
                fieldName,
                userSelectedModel,
                originalValue,
                settings,
                currentTitle,
                currentBody
              );
              const { chosenModel, prompt, systemMessage } = modelAndPrompt;
              if (prompt) {
                try {
                  const completion = await openai.chat.completions.create({
                    model: chosenModel,
                    messages: [
                      { role: "system", content: systemMessage },
                      { role: "user", content: prompt },
                    ],
                    temperature: 0.7,
                  });
                  const newValue = completion.choices?.[0]?.message?.content.trim() || originalValue;
                  console.log(
                    "Model voor " +
                    fieldName +
                    ": " +
                    chosenModel +
                    " - Prompt: " +
                    prompt +
                    " - SystemMessage: " +
                    systemMessage +
                    " - Ontvangen waarde: " +
                    newValue
                  );
                  if (fieldName === "Tags") {
                    productRow[fieldName] = originalValue + ", " + newValue;
                  } else {
                    productRow[fieldName] = newValue;
                  }
                  if (fieldName === "Title") {
                    newTitle = newValue;
                  }

                  if (fieldName === "SEO Title") {
                    // Geef SEO Title voorrang boven Title voor de nieuwe handle
                    const baseForHandle = newValue || newTitle;
                    if (baseForHandle) {
                      newHandle = baseForHandle
                        .toLowerCase()
                        .replace(/[^\w\s-]/g, "") // Verwijder speciale tekens
                        .trim()
                        .replace(/\s+/g, "-");
                    }

                  }

                  if (fieldName === "Body (HTML)") {
                    newBody = newValue;
                  }
                } catch (error) {
                  console.error(`OpenAI fout bij ${fieldName}:`, error);
                  productRow[fieldName] = originalValue;
                }
              }
            }
          }

          // 6.a.1. Genereer nieuwe handle als deze nog niet gezet is
          if (!newHandle) {
            const seoTitle = productRow["SEO Title"];
            const title = productRow["Title"];
            const baseForHandle = seoTitle || title || handle;
            if (baseForHandle) {
              newHandle = baseForHandle
                .toLowerCase()
                .replace(/[^\w\s-]/g, "") // verwijder speciale tekens
                .trim()
                .replace(/\s+/g, "-");
              console.log(`Nieuwe handle gegenereerd uit "${baseForHandle}": ${newHandle}`);
            }
          }


          // 6.b. Vertaal optie-waarden in product- en variantrijen
          if (edits["transl-options-checkbox"] === "on") {
            const optionValuesToTranslate = new Set();
            productRows.forEach((pRow) => {
              if (pRow["Option1 Value"]) {
                optionValuesToTranslate.add(pRow["Option1 Value"]);
              }
              if (pRow["Option2 Value"]) {
                optionValuesToTranslate.add(pRow["Option2 Value"]);
              }
              if (pRow["Option3 Value"]) {
                optionValuesToTranslate.add(pRow["Option3 Value"]);
              }
            });
            variantRows.forEach((vRow) => {
              if (vRow["Option1 Value"]) {
                optionValuesToTranslate.add(vRow["Option1 Value"]);
              }
              if (vRow["Option2 Value"]) {
                optionValuesToTranslate.add(vRow["Option2 Value"]);
              }
              if (vRow["Option3 Value"]) {
                optionValuesToTranslate.add(vRow["Option3 Value"]);
              }
            });
            if (optionValuesToTranslate.size > 0) {
              const namesArray = Array.from(optionValuesToTranslate);
              const variantPrompt =
                "Instruction: Je bent een vertaalassistent gespecialiseerd in het vertalen van productvariantnamen. Wanneer je een lijst met variantnamen ontvangt, vertaal deze dan naar het " +
                settings.Language +
                ". Geef uitsluitend de vertaalde namen terug, als een lijst gescheiden door komma's, zonder extra uitleg of opmaak. :\n\n" +
                namesArray.join(", ") +
                "\n\n" +
                "Geef alleen de vertalingen in een lijst gescheiden door komma's.";
              console.log("Verzenden model voor variantnamen vertaling:", variantPrompt);
              const translatedVariantNames = {};
              try {
                const completion = await openai.chat.completions.create({
                  model: "gpt-3.5-turbo",
                  messages: [
                    { role: "system", content: "Je bent een AI-assistent." },
                    { role: "user", content: variantPrompt },
                  ],
                  temperature: 0.3,
                });
                const translatedText = (
                  completion.choices?.[0]?.message?.content || ""
                ).trim();
                const translatedNames = translatedText.split(",").map((name) => name.trim());
                if (translatedNames.length === namesArray.length) {
                  namesArray.forEach((original, index) => {
                    translatedVariantNames[original] = translatedNames[index];
                  });
                  console.log("Vertaalde variantnamen:", translatedVariantNames);
                } else {
                  console.warn(
                    "Aantal vertalingen komt niet overeen met " +
                    "aantal originele namen.",
                    translatedVariantNames
                  );
                }
              } catch (error) {
                console.error("OpenAI fout bij variantnamen:", error);
              }
              productRows.forEach((pRow) => {
                if (pRow["Option1 Value"] && translatedVariantNames[pRow["Option1 Value"]]) {
                  pRow["Option1 Value"] = translatedVariantNames[pRow["Option1 Value"]];
                }
                if (pRow["Option2 Value"] && translatedVariantNames[pRow["Option2 Value"]]) {
                  pRow["Option2 Value"] = translatedVariantNames[pRow["Option2 Value"]];
                }
                if (pRow["Option3 Value"] && translatedVariantNames[pRow["Option3 Value"]]) {
                  pRow["Option3 Value"] = translatedVariantNames[pRow["Option3 Value"]];
                }
              });
              variantRows.forEach((vRow) => {
                if (vRow["Option1 Value"] && translatedVariantNames[vRow["Option1 Value"]]) {
                  vRow["Option1 Value"] = translatedVariantNames[vRow["Option1 Value"]];
                }
                if (vRow["Option2 Value"] && translatedVariantNames[vRow["Option2 Value"]]) {
                  vRow["Option2 Value"] = translatedVariantNames[vRow["Option2 Value"]];
                }
                if (vRow["Option3 Value"] && translatedVariantNames[vRow["Option3 Value"]]) {
                  vRow["Option3 Value"] = translatedVariantNames[vRow["Option3 Value"]];
                }
              });
            }
          }

          // 6.c. Update Handle in alle rijen
          const updatedHandle = newHandle;
          if (processedUpdatedHandles.has(updatedHandle)) {
            console.log(`Handle ${updatedHandle} is al verwerkt, overslaan.`);
            continue;
          }
          processedUpdatedHandles.add(updatedHandle);
          productRows.forEach((pRow) => {
            pRow["Handle"] = updatedHandle;
          });
          variantRows.forEach((vRow) => {
            vRow["Handle"] = updatedHandle;
          });
          otherRows.forEach((oRow) => {
            oRow["Handle"] = updatedHandle;
          });

          productsProcessedSoFar += 1;
          tokensUsedSoFar = productsProcessedSoFar * tokensPerProduct;


          if (supabaseUserId && productsProcessedSoFar % 5 === 0) {
            await createOrUpdateSupabaseHistory(supabaseUserId, bulkeditid, {
              tokens: tokensUsedSoFar,
              products_processed: productsProcessedSoFar,
            });
          }
          await new Promise((resolve) => {
            setTimeout(resolve, 500);
          });
        } // Einde batch-verwerking

        // Bouw de (bijgewerkte) rijen opnieuw samen uit het volledige productsMap
        const updatedRows = [];
        for (const handle in productsMap) {
          if (Object.prototype.hasOwnProperty.call(productsMap, handle)) {
            const { productRows, variantRows, otherRows } = productsMap[handle];
            updatedRows.push(...productRows, ...variantRows, ...otherRows);
          }
        }
        const updatedCsv = Papa.unparse(updatedRows);

        // Als er nog meer producten over zijn, plan dan een nieuwe Cloud Task in
        if (endIndex < totalProducts) {
          settings.startIndex = endIndex;
          settings.productsProcessedSoFar = productsProcessedSoFar;
          settings.tokensUsedSoFar = tokensUsedSoFar;

          const parent = tasksClient.queuePath(
            "ecomai-3730f",
            "us-central1",
            "my-queue"
          );
          const taskPayload = {
            csvContent: updatedCsv,
            edits,
            models,
            settings,
          };
          const processOptimizeUrl =
            "https://us-central1-ecomai-3730f.cloudfunctions.net/processOptimizeShopifyCsvTask";
          const task = {
            httpRequest: {
              httpMethod: "POST",
              url: processOptimizeUrl,
              headers: { "Content-Type": "application/json" },
              body: Buffer.from(JSON.stringify(taskPayload)).toString("base64"),
            },
          };
          const [cloudTask] = await tasksClient.createTask({ parent, task });
          console.log("Nieuwe Cloud Task aangemaakt voor volgende batch:", cloudTask.name);
          return res.status(200).json({
            success: true,
            message: `Batch van index ${startIndex} tot ${endIndex - 1} verwerkt. Nieuwe taak aangemaakt voor de resterende producten.`,
            bulkeditid: bulkeditid,
          });
        }

        // 7. Als alle producten verwerkt zijn, upload de finale CSV en maak een signed URL
        const type = settings.Type || "bulk edit";
        console.log("Bijgewerkte CSV:", updatedCsv.substring(0, 100) + "...");
        const fileName = `optimized-csv-${Date.now()}.csv`;
        const file = storage.bucket(bucketName).file(fileName);
        console.log(`Upload CSV naar Cloud Storage als ${fileName}...`);
        await file.save(updatedCsv, {
          metadata: { contentType: "text/csv" },
          resumable: false,
        });
        console.log("CSV-bestand succesvol geüpload.");

        const [signedUrl] = await file.getSignedUrl({
          action: "read",
          expires: Date.now() + 7 * 24 * 60 * 60 * 1000,
        });
        console.log("Signed URL:", signedUrl);


        if (supabaseUserId) {
          await createOrUpdateSupabaseHistory(supabaseUserId, bulkeditid, {
            status: "Finished",
            output_file: signedUrl,
            tokens: tokensUsedSoFar,
            products_processed: productsProcessedSoFar,
          });
          await addSupabaseUsageItem(supabaseUserId, bulkeditid, type, -tokensUsedSoFar);
        }

        // Groepeer alle varianten per unieke Handle
        const groupedByHandle = {};
        for (const row of updatedRows) {
          const handleVal = row["Handle"]?.trim();
          if (!handleVal) continue;
          if (!groupedByHandle[handleVal]) groupedByHandle[handleVal] = [];
          groupedByHandle[handleVal].push(row);
        }


        if (supabaseUserId && updatedRows.length > 0) {
          try {
            const supabase = await initSupabase();
            const inserted = await addProducts(
              supabaseUserId,
              groupedByHandle,
              supabase,
              editType,
              sourceType,
              "Shopify",     // ← sourcePlatform
              sourceCountry, // ← sourceCountry
              bulkeditid     // ← import_id ✅
            );



            if (inserted && inserted.length > 0) {
              console.log(`✅ ${inserted.length} AI-bewerkte producten toegevoegd aan Supabase products-tabel`);
            } else {
              console.warn("⚠️ Geen producten toegevoegd of insert gaf null terug.");
            }
          } catch (err) {
            console.error("❌ Fout bij toevoegen van producten:", err);
          }
        }

        return res.status(200).json({ success: true, downloadLink: signedUrl });
      } catch (err) {
        console.error("Fout in processOptimizeShopifyCsvTask:", err);
        return res.status(500).json({ success: false, error: err.toString() });
      }
    });
  }
);




/**
 * Firebase Cloud Function: uploadProducts
 *
 * Deze functie verwerkt een CSV-upload.
 * Verwacht in de request-body:
 * - csvUrl: URL van het CSV-bestand.
 * - settings: { UID, name, totalProducts }
 *
 * Er wordt een history item met type "Upload" in Supabase opgeslagen en de functie retourneert een bulkeditid.
 */
exports.uploadProducts = functions.https.onRequest((req, res) => {
  // Stel CORS-headers in
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  cors(req, res, async () => {
    try {
      if (req.method !== "POST") {
        return res.status(405).json({ message: "Method Not Allowed" });
      }

      const { csvUrl, settings } = req.body;
      if (!csvUrl || !settings || !settings.UID || !settings.name) {
        return res.status(400).json({ message: "Invalid data received" });
      }

      // Gebruik totalProducts uit settings; als deze niet aanwezig is, default naar 0.
      const totalProducts = settings.totalProducts || 0;
      const supabaseUserId = settings.UID;
      const name = settings.name;
      // Genereer een unieke bulkeditid
      const bulkeditid = Date.now().toString();

      // Bouw het nieuwe data-object voor Supabase met type "Upload"
      const newData = {
        type: "Upload",
        status: "Finished",
        tokens: 0,
        products_processed: totalProducts,
        total_products: totalProducts,
        output_file: csvUrl,
        name: name,
        // Optioneel: je kunt ook de CSV-content of andere data opslaan
      };

      const historyItem = await createOrUpdateSupabaseHistory(supabaseUserId, bulkeditid, newData);
      if (!historyItem) {
        return res.status(500).json({ message: "Failed to create history item in Supabase." });
      }

      console.log("History item successfully created:", historyItem);
      return res.status(200).json({
        message: "Upload history item created successfully",
        bulkeditid,
      });
    } catch (error) {
      console.error("Error in uploadProducts function:", error);
      return res.status(500).json({ message: "Internal server error." });
    }
  });
});


/**
 * Creëert of update een row in de "history_items" tabel in Supabase.
 *
 * @param {string} supabaseUserId - De Supabase user ID.
 * @param {string} bulkeditid - De unieke bulkedit ID.
 * @param {object} newData - De data om op te slaan.
 * @return {Promise<object|null>} Het aangemaakte of geüpdatete record.
 */
async function createOrUpdateSupabaseHistory(supabaseUserId, bulkeditid, newData) {
  const supabase = await initSupabase();
  // Controleer of er al een record bestaat
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










// 1. Haalt periodiek de ranking data op

exports.triggerUpdateTrackedDataScheduled = onSchedule(
  {
    schedule: "15 1 * * *",       // dagelijks om 21:30
    timeZone: "Europe/Amsterdam",  // jouw lokale tijd
    region: "us-central1",       // regio van je queue
  },
  async () => {
    const supabase = await initSupabase();

    try {
      const { data: stores, error } = await supabase
        .from("tracked_stores")
        .select("*")
        .eq("status", "active");

      if (error || !Array.isArray(stores) || stores.length === 0) {
        console.warn("📭 Geen actieve stores gevonden.");
        return;
      }

      const snapshotId = Date.now().toString();
      const parent = tasksClient.queuePath(
        "ecomai-3730f",
        "us-central1",
        "my-queue"
      );
      const processUrl =
        "https://us-central1-ecomai-3730f.cloudfunctions.net/processUpdateTrackedDataTask";

      for (const store of stores) {
        const taskPayload = {
          store_id: store.id,
          user_id: store.user_id,
          store_url: store.store_url,
          products_to_track: store.products_to_track,
          snapshot_id: snapshotId,
        };

        const task = {
          httpRequest: {
            httpMethod: "POST",
            url: processUrl,
            headers: { "Content-Type": "application/json" },
            body: Buffer.from(JSON.stringify(taskPayload)).toString("base64"),
          },
          retryConfig: {
            maxAttempts: 3,
            minBackoff: { seconds: 10 },
            maxBackoff: { seconds: 60 },
            maxRetryDuration: { seconds: 3600 },
          },
        };

        await tasksClient.createTask({ parent, task });
        console.log(`✅ Task aangemaakt voor store: ${store.store_url}`);
      }

      console.log(`✅ ${stores.length} taken aangemaakt.`);
    } catch (err) {
      console.error("❌ Fout in triggerUpdateTrackedData:", err);
    }
  }
);





exports.processUpdateTrackedDataTask = onRequest({
  timeoutSeconds: 300,
  memory: "712MiB",
}, async (req, res) => {
  const supabase = await initSupabase();

  try {
    const { store_id, user_id, store_url, products_to_track, snapshot_id } = req.body;

    if (!store_id || !user_id || !store_url || !snapshot_id) {
      return res.status(400).send("Missing fields");
    }

    let fixedUrl = store_url;
    if (!fixedUrl.startsWith("http")) {
      fixedUrl = "https://" + fixedUrl;
    }

    const url = new URL(fixedUrl);
    url.pathname = "/collections/all";
    url.searchParams.set("sort_by", "best-selling");

    const response = await fetch(url.toString());
    const html = await response.text();
    const $ = cheerio.load(html);

    const productLinks = new Set();
    $("a[href*='/products/']").each((_, el) => {
      let href = $(el).attr("href");
      if (href.startsWith("//")) href = "https:" + href;
      else if (href.startsWith("/")) href = `${url.origin}${href}`;
      else if (!href.startsWith("http")) href = `${url.origin}/${href}`;
      productLinks.add(href);
    });

    const limitedLinks = Array.from(productLinks).slice(0, products_to_track || 50);
    const trackedData = [];
    let rank = 1;

    for (const productUrl of limitedLinks) {
      try {
        const productJsonUrl = new URL(productUrl);
        productJsonUrl.pathname += ".json";

        const productRes = await fetch(productJsonUrl.toString());
        if (!productRes.ok) throw new Error(`Bad response: ${productRes.status}`);

        const productJson = await productRes.json();
        const product = productJson?.product;
        if (!product) continue;

        const sourceDomain = productJsonUrl.hostname.replace(/^www\./, "");
        const langMatch = html.match(/<html[^>]+lang=["']?([a-zA-Z-]+)["']?/);
        const sourceLang = langMatch ? langMatch[1].toLowerCase() : null;

        trackedData.push({
          user_id,
          store_id,
          snapshot_id,
          timestamp: new Date().toISOString(),
          handle: product.handle,
          title: product.title,
          product_id: product.id,
          current_rank: rank,
          status: "active",
          source_domain: sourceDomain,
          source_country: sourceLang,
        });

        rank++;
      } catch (err) {
        console.warn(`❌ Fout bij ophalen van ${productUrl}:`, err.message);
      }
    }

    if (trackedData.length > 0) {
      const { error: insertError } = await supabase
        .from("tracked_data")
        .insert(trackedData);

      if (insertError) {
        console.error("❌ Fout bij insert:", insertError);
        return res.status(500).send("Insert error");
      }

      console.log(`✅ ${trackedData.length} producten opgeslagen voor ${store_url}`);

      // Reset warning als het succesvol is
      await supabase
        .from("tracked_stores")
        .update({
          warnings: "",
          updated_at: new Date().toISOString(),
        })
        .eq("id", store_id);
    } else {
      console.warn(`⚠️ Geen producten gevonden voor ${store_url}`);

      // Check huidige warning level
      const { data: currentStore } = await supabase
        .from("tracked_stores")
        .select("warnings")
        .eq("id", store_id)
        .single();

      const prevWarning = currentStore?.warnings || "";
      let newWarning = "";
      let newStatus = "active";

      if (prevWarning === "Failed_1") {
        newWarning = "Failed_2";
      } else if (prevWarning === "Failed_2") {
        newWarning = "Failed_3";
        newStatus = "inactive";
      } else {
        newWarning = "Failed_1";
      }

      const { error: updateError } = await supabase
        .from("tracked_stores")
        .update({
          warnings: newWarning,
          status: newStatus,
          updated_at: new Date().toISOString(),
        })
        .eq("id", store_id);

      if (updateError) {
        console.error("❌ Fout bij bijwerken failstatus:", updateError);
      } else {
        console.log(`📛 Store gemarkeerd als: ${newWarning} (status: ${newStatus})`);
      }
    }

    return res.status(200).send("Store verwerkt.");
  } catch (err) {
    console.error("❌ Fout in processUpdateTrackedDataTask:", err.message);
    return res.status(500).send("Er ging iets mis.");
  }
});



// 2. Berekend winners in de ranking data en plaatst deze in de tracked_winners

exports.triggerCalculateRankRisersScheduled = onSchedule(
  {
    schedule: "20 1 * * *",          // elke dag om 22:00
    timeZone: "Europe/Amsterdam",
    region: "us-central1",
  },
  async () => {
    const supabase = await initSupabase();

    try {
      // 1) Haal alle actieve stores op
      const { data: stores, error } = await supabase
        .from("tracked_stores")
        .select("id, user_id, store_url")
        .eq("status", "active");

      if (error || !Array.isArray(stores) || stores.length === 0) {
        console.warn("📭 Geen actieve stores gevonden.");
        return;
      }

      // 2) Stel je task‐queue en process‐URL in
      const parent = tasksClient.queuePath(
        "ecomai-3730f",
        "us-central1",
        "my-queue"
      );
      const processUrl =
        "https://us-central1-ecomai-3730f.cloudfunctions.net/processCalculateRankRisersTask";

      // 3) Voor elke store maak je een Cloud Tasks‐taak
      for (const store of stores) {
        const taskPayload = {
          store_id: store.id,
          user_id: store.user_id,
          store_url: store.store_url,
        };

        const task = {
          httpRequest: {
            httpMethod: "POST",
            url: processUrl,
            headers: { "Content-Type": "application/json" },
            body: Buffer.from(JSON.stringify(taskPayload)).toString("base64"),
          },
          retryConfig: {
            maxAttempts: 3,
            minBackoff: { seconds: 10 },
            maxBackoff: { seconds: 60 },
            maxRetryDuration: { seconds: 3600 },
          },
        };

        await tasksClient.createTask({ parent, task });
        console.log(`✅ Task aangemaakt voor store: ${store.store_url}`);
      }

      console.log(`✅ ${stores.length} rankriser-taken aangemaakt.`);
    } catch (err) {
      console.error("❌ Fout in triggerCalculateRankRisers:", err);
    }
  }
);



exports.processCalculateRankRisersTask = onRequest({
  timeoutSeconds: 300,
  memory: "712MiB",
}, async (req, res) => {
  const supabase = await initSupabase();

  try {
    const { store_id, user_id, store_url } = req.body;

    if (!store_id || !user_id) {
      return res.status(400).send("Missing store_id or user_id");
    }

    // 🧠 Snapshots ophalen
    const { data: snapshots, error: snapshotError } = await supabase
      .from("tracked_data")
      .select("snapshot_id, timestamp")
      .eq("store_id", store_id)
      .not("snapshot_id", "is", null)
      .order("timestamp", { ascending: false })
      .limit(20);

    if (snapshotError) {
      console.error("❌ Fout bij ophalen snapshots:", snapshotError);
      return res.status(500).send("Snapshot fetch error");
    }

    const uniqueSnapshots = Array.from(
      new Map(snapshots.map((s) => [s.snapshot_id, s.timestamp]))
    );

    if (uniqueSnapshots.length < 2) {
      console.warn(`⚠️ Niet genoeg snapshots voor ${store_url}`);
      return res.status(200).send("Te weinig snapshots.");
    }

    const [latestEntry, previousEntry] = uniqueSnapshots;
    const latestId = latestEntry[0];
    const previousId = previousEntry[0];

    // 🔄 Data ophalen
    const { data: latestData } = await supabase
      .from("tracked_data")
      .select("*")
      .eq("store_id", store_id)
      .eq("snapshot_id", latestId);

    const { data: previousData } = await supabase
      .from("tracked_data")
      .select("*")
      .eq("store_id", store_id)
      .eq("snapshot_id", previousId);

    const previousMap = new Map();
    for (const item of previousData) {
      previousMap.set(item.handle, item);
    }

    const winners = [];

    for (const latestItem of latestData) {
      const prev = previousMap.get(latestItem.handle);
      if (!prev) continue;

      const oldRank = prev.current_rank;
      const newRank = latestItem.current_rank;

      if (
        typeof oldRank === "number" &&
        typeof newRank === "number" &&
        newRank < oldRank
      ) {
        // 🔒 Check op bestaande entry
        const { data: exists } = await supabase
          .from("tracked_winners")
          .select("id")
          .eq("store_id", store_id)
          .eq("handle", latestItem.handle)
          .limit(1);

        if (exists && exists.length > 0) {
          console.log(`⏭️ ${latestItem.handle} al in winners`);
          continue;
        }

        winners.push({
          user_id,
          store_id,
          handle: latestItem.handle,
          title: latestItem.title,
          product_id: latestItem.product_id,
          unique_id: latestItem.unique_id || null,
          start_rank: oldRank,
          current_rank: newRank,
          source_domain: latestItem.source_domain,
          source_country: latestItem.source_country,
          status: "winner",
          processed: false,
          timestamp: latestItem.timestamp,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        });
      }
    }

    if (winners.length > 0) {
      const { error: insertError } = await supabase
        .from("tracked_winners")
        .insert(winners);

      if (insertError) {
        console.error(`❌ Insert error voor ${store_url}:`, insertError);
        return res.status(500).send("Insert error");
      }

      console.log(`✅ ${winners.length} winners toegevoegd voor ${store_url}`);
    } else {
      console.log(`📉 Geen nieuwe winners voor ${store_url}`);
    }

    return res.status(200).send("Rankriser-verwerking voltooid.");
  } catch (err) {
    console.error("❌ processCalculateRankRisersTask error:", err.message);
    return res.status(500).send("Er ging iets mis.");
  }
});



// 3. Importeert tracked_winners als products

exports.triggerProcessTrackedWinnersScheduled = onSchedule(
  {
    schedule: "50 5 * * *",       // elke dag om 22:30
    timeZone: "Europe/Amsterdam",
    region: "us-central1",
  },
  async () => {
    const supabase = await initSupabase();

    try {
      // 1) Haal alle nog niet verwerkte winners
      const { data: winners, error } = await supabase
        .from("tracked_winners")
        .select("*")
        .eq("processed", false);

      if (error || !Array.isArray(winners) || winners.length === 0) {
        console.warn("📭 Geen unprocessed winners.");
        return;
      }

      // 2) Splits in batches van 25
      const batchSize = 25;
      const batches = Array.from(
        { length: Math.ceil(winners.length / batchSize) },
        (_, i) => winners.slice(i * batchSize, i * batchSize + batchSize)
      );

      // 3) Voor elke batch een task in de queue
      const parent = tasksClient.queuePath(
        "ecomai-3730f",
        "us-central1",
        "my-queue"
      );
      const processUrl =
        "https://us-central1-ecomai-3730f.cloudfunctions.net/processTrackedWinnersBatchTask";

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        const taskPayload = { winners: batch };

        const task = {
          httpRequest: {
            httpMethod: "POST",
            url: processUrl,
            headers: { "Content-Type": "application/json" },
            body: Buffer.from(JSON.stringify(taskPayload)).toString("base64"),
          },
          retryConfig: {
            maxAttempts: 3,
            minBackoff: { seconds: 10 },
            maxBackoff: { seconds: 60 },
            maxRetryDuration: { seconds: 3600 },
          },
        };

        await tasksClient.createTask({ parent, task });
        console.log(
          `✅ Batch ${i + 1}/${batches.length} task aangemaakt (${batch.length} winners)`
        );
      }

      console.log(
        `✅ ${batches.length} taken aangemaakt (${winners.length} winners totaal)`
      );
    } catch (err) {
      console.error("❌ Fout in triggerProcessTrackedWinners:", err);
    }
  }
);


exports.processTrackedWinnersBatchTask = onRequest({
  timeoutSeconds: 300,
  memory: "712MiB",
}, async (req, res) => {
  const supabase = await initSupabase();

  // 1️⃣ genereer één bulkeditid per run, vóór try/catch
  const bulkeditid = Date.now().toString();  // bv. "1744822500926"

  try {
    const { winners } = req.body;
    if (!winners || !Array.isArray(winners) || winners.length === 0) {
      return res.status(400).send("Geen winners ontvangen.");
    }

    // user_id voor history
    const userId = winners[0].user_id;

    // History aanmaken (één keer) met status 'started'
    await createOrUpdateSupabaseHistory(userId, bulkeditid, {
      name: "Tracked Winners",
      type: "Import (Winners)",
      status: "started",
      total_products: winners.length,
    });

    const processedWinnerIds = [];
    const transformedProducts = [];

    // Batch-verwerking
    for (const winner of winners) {
      try {
        const baseUrl = winner.source_domain?.startsWith("http")
          ? new URL(winner.source_domain).origin
          : `https://${winner.source_domain}`;
        const handle = encodeURIComponent(winner.handle);
        const productUrl = `${baseUrl}/products/${handle}.json`;

        console.log(`🔍 Ophalen van product: ${productUrl}`);
        const response = await fetch(productUrl);
        if (!response.ok) {
          console.warn(`❌ Fout bij ophalen van ${productUrl}: HTTP ${response.status}`);
          // fallback naar HTML
          const fallbackHtmlUrl = `${baseUrl}/products/${handle}`;
          const htmlResponse = await fetch(fallbackHtmlUrl);
          const html = await htmlResponse.text();
          if (html.includes("product")) {
            console.log(`⚠️ ${fallbackHtmlUrl} bevat waarschijnlijk product-info`);
          }
          continue;
        }

        const data = await response.json();
        const product = data?.product;
        if (!product) {
          console.warn(`⚠️ Geen productdata in JSON voor ${winner.handle}`);
          continue;
        }

        // Duplicate-handle check
        let finalHandle = product.handle;
        const { data: exists } = await supabase
          .from("products")
          .select("id")
          .eq("userid", winner.user_id)
          .eq("product_data->product->>handle", finalHandle)
          .limit(1);
        if (exists?.length) {
          const rand = Math.floor(100 + Math.random() * 900);
          finalHandle = `${product.handle}-${rand}`;
          product.handle = finalHandle;
          console.log(`⚠️ Handle aangepast naar ${finalHandle}`);
        }

        const timestamp = new Date().toISOString();
        product.created_at = timestamp;
        product.updated_at = timestamp;

        // Voeg import_id = bulkeditid toe
        transformedProducts.push({
          userid: winner.user_id,
          title: product.title,
          price: product.variants?.[0]?.price ?? null,
          image: product.image?.src ?? null,
          in_app_tags: [],
          source_type: "Tracked",
          source_domain: winner.source_domain || null,
          source_country: winner.source_country || null,
          edit_type: "Original",
          ranking: winner.current_rank,
          import_id: bulkeditid,       // ← hier
          product_data: { product },
        });

        processedWinnerIds.push(winner.id);
      } catch (innerErr) {
        console.warn(`⚠️ Fout bij verwerken ${winner.handle}:`, innerErr.message);
      }
    }

    // Insert en winners updaten
    if (transformedProducts.length > 0) {
      const { error: insertError } = await supabase
        .from("products")
        .insert(transformedProducts);

      if (insertError) {
        console.error("❌ Fout bij insert van producten:", insertError);
        // History naar 'failed'
        await createOrUpdateSupabaseHistory(userId, bulkeditid, {
          status: "failed",
          products_processed: 0,
        });
        return res.status(500).send("Insert error");
      }

      const { error: updateWinnersError } = await supabase
        .from("tracked_winners")
        .update({ processed: true, updated_at: new Date().toISOString() })
        .in("id", processedWinnerIds);

      if (updateWinnersError) {
        console.warn("⚠️ Fout bij updaten winners:", updateWinnersError);
      } else {
        console.log(`✅ ${transformedProducts.length} producten toegevoegd`);
        console.log(`✅ ${processedWinnerIds.length} winners gemarkeerd als processed`);
      }

      // History afronden op 'Finished'
      await createOrUpdateSupabaseHistory(userId, bulkeditid, {
        status: "Finished",
        products_processed: ""
      });
    } else {
      console.log("📭 Geen geldige producten in deze batch");
      await createOrUpdateSupabaseHistory(userId, bulkeditid, {
        status: "Finished",
        products_processed: 0,
      });
    }

    return res.status(200).send("Batch verwerkt.");
  } catch (err) {
    console.error("❌ Fout in processTrackedWinnersBatchTask:", err.message);
    // History naar 'failed' indien bulkeditid bestaat
    if (req.body?.winners?.length) {
      const userId = req.body.winners[0].user_id;
      await createOrUpdateSupabaseHistory(userId, bulkeditid, {
        status: "failed",
      });
    }
    return res.status(500).send("Er ging iets mis.");
  }
});







/*
 * [[ HELPERFUNCTIES ]]
*/


/**
 * Bepaalt op basis van veldnaam, userSelectedModel en instellingen
 * welk model en prompt gebruikt wordt.
 *
 * @param {string} fieldName - Naam van het veld dat verwerkt wordt
 * @param {string} userSelectedModel - Key van het door de gebruiker
 * gekozen model
 * @param {string} originalValue - De originele waarde van het veld
 * @param {Object} settings - Instellingen (taal, tags, enz.)
 * @param {string} [currentTitle] - Optioneel, huidige Title
 * @param {string} [currentBody] - Optioneel, huidige Body
 * @return {{chosenModel: string, prompt: string, systemMessage: string}}
 * Een object met modelinfo, prompt en system-message.
*/








/*
 * addProducts helperfunctie
 */

async function addProducts(
  userId,
  groupedByHandle,
  supabase,
  editType = "ai edit",
  sourceType = "Import",
  sourcePlatform,
  sourceCountry,
  importId = null
) {
  if (!userId || typeof groupedByHandle !== "object" || Object.keys(groupedByHandle).length === 0) {
    console.warn("⚠️ Ongeldige input voor addProducts", {
      userId,
      groupedByHandle,
    });
    return null;
  }

  const productsToInsert = Object.entries(groupedByHandle).map(([, rows]) => {
    const base = rows.find((r) => r.Title && r["Variant Price"]) || rows[0];

    // ✅ Stap 1: Groepeer unieke images en combineer variant_ids
    const imageMap = rows.reduce((acc, r, i) => {
      const src = r["Image Src"];
      if (!src) return acc;

      if (!acc[src]) {
        acc[src] = {
          id: null,
          product_id: null,
          src,
          alt: r["Image Alt Text"] || null,
          width: r["Image Width"] || null,
          height: r["Image Height"] || null,
          position: r["Image Position"] || i + 1,
          created_at: r["Image Created At"] || new Date().toISOString(),
          updated_at: r["Image Updated At"] || new Date().toISOString(),
          variant_ids: new Set(),
        };
      }

      const raw = r["Image Variant IDs"];
      let ids = [];

      try {
        if (typeof raw === "string") {
          const parsed = JSON.parse(raw);
          if (Array.isArray(parsed)) ids = parsed;
        } else if (Array.isArray(raw)) {
          ids = raw;
        }
      } catch (err) {
        console.warn("❌ Fout bij parsen van Image Variant IDs:", raw);
      }

      ids.forEach((id) => acc[src].variant_ids.add(String(id)));

      return acc;
    }, {});

    const images = Object.values(imageMap).map((img) => ({
      ...img,
      variant_ids: Array.from(img.variant_ids),
    }));

    // ✅ Stap 2: Stel productJson samen
    // 🔁 Bouw mapping: variantId => image.src
    const variantIdToImageMap = {};
    images.forEach((img) => {
      (img.variant_ids || []).forEach((variantId) => {
        variantIdToImageMap[variantId] = img.src;
      });
    });

    const productJson = {
      product: {
        id: null,
        title: base.Title,
        body_html: base["Body (HTML)"] || "",
        vendor: base["Vendor"] || "",
        product_type: base["Type"] || "",
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        published_at: new Date().toISOString(),
        published_scope: "global",
        template_suffix: null,
        handle: base.Handle,
        tags: base["Tags"] || "",

        variants: rows
          .filter((r) => r["Variant Shopify ID"] && r["Variant Price"])
          .map((r, i) => {
            const variantId = r["Variant Shopify ID"] || `${base.Handle}-${i}`;
            const imageSrc = variantIdToImageMap[variantId] || null;

            return {
              id: variantId,
              product_id: null,
              title: `${r["Option1 Value"] || ""} / ${r["Option2 Value"] || ""}`.trim(),
              price: r["Variant Price"] || null,
              sku: r["Variant SKU"] || null,
              position: i + 1,
              compare_at_price: r["Variant Compare At Price"] || null,
              fulfillment_service: r["Variant Fulfillment Service"] || "manual",
              inventory_management: r["Variant Inventory Tracker"] || null,
              option1: r["Option1 Value"] || null,
              option2: r["Option2 Value"] || null,
              option3: r["Option3 Value"] || null,
              created_at: new Date().toISOString(),
              updated_at: new Date().toISOString(),
              taxable: r["Variant Taxable"] === "TRUE",
              barcode: r["Variant Barcode"] || null,
              grams: parseInt(r["Variant Grams"] || "0"),
              image_id: imageSrc,
              weight: 0,
              weight_unit: r["Variant Weight Unit"] || "kg",
              requires_shipping: r["Variant Requires Shipping"] === "TRUE",
              price_currency: "EUR",
              compare_at_price_currency: "EUR",
            };
          }),


        options: [
          {
            name: base["Option1 Name"] || "Optie 1",
            position: 1,
            values: [...new Set(rows.map((r) => r["Option1 Value"]).filter(Boolean))],
          },
          {
            name: base["Option2 Name"] || "Optie 2",
            position: 2,
            values: [...new Set(rows.map((r) => r["Option2 Value"]).filter(Boolean))],
          },
          {
            name: base["Option3 Name"] || "Optie 3",
            position: 3,
            values: [...new Set(rows.map((r) => r["Option3 Value"]).filter(Boolean))],
          },
        ].filter((opt) => opt.values.length > 0),

        images: images,

        image: {
          id: null,
          product_id: null,
          position: 1,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          alt: base["Image Alt Text"] || null,
          width: base["Image Width"] || null,
          height: base["Image Height"] || null,
          src: base["Image Src"] || null,
          variant_ids: [],
        },
      },
    };


    return {
      userid: userId,
      source_country: sourceCountry || null,
      import_id: importId,
      title: productJson.product.title,
      price: productJson.product.variants?.[0]?.price || null,
      image: productJson.product.image?.src || null,
      in_app_tags: [],
      source_type: sourceType,
      source_domain: base["Source Domain"] || null,
      edit_type: editType,
      source_platform: sourcePlatform,
      ranking: base["ranking"] || base["Ranking"] || null,
      product_data: productJson,
    };
  });

  console.log("📦 Producten die naar Supabase gaan:", productsToInsert);

  const { data, error } = await supabase
    .from("products")
    .insert(productsToInsert)
    .select();

  if (error) {
    console.error("❌ Supabase INSERT error:", error);
    return null;
  }

  return data;
}






exports.setStoreLangFromHTML = functions.https.onRequest(async (req, res) => {
  // ✅ Handmatige CORS headers
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.status(204).send("");
  }

  const supabase = await initSupabase();

  function calculateProductsToTrack(reviewsCount, totalProducts) {
    if (totalProducts <= 100) {
      if (reviewsCount === 0) return 0;
      if (reviewsCount <= 5) return 5;
      if (reviewsCount <= 10) return 7;
      if (reviewsCount <= 25) return 10;
      if (reviewsCount <= 50) return 15;
      if (reviewsCount <= 100) return 25;
      if (reviewsCount <= 250) return 35;
      if (reviewsCount <= 500) return 50;
      if (reviewsCount <= 1000) return 65;
      return 75; // voor 1000+ reviews
    }
    if (totalProducts <= 500) {
      if (reviewsCount === 0) return 0;
      if (reviewsCount <= 5) return 5;
      if (reviewsCount <= 10) return 10;
      if (reviewsCount <= 25) return 15;
      if (reviewsCount <= 50) return 20;
      if (reviewsCount <= 100) return 30;
      if (reviewsCount <= 250) return 40;
      if (reviewsCount <= 500) return 50;
      if (reviewsCount <= 1000) return 75;
      return 100; // voor 1000+ reviews
    }
    if (totalProducts <= 1000) {
      if (reviewsCount === 0) return 0;
      if (reviewsCount <= 5) return 5;
      if (reviewsCount <= 10) return 10;
      if (reviewsCount <= 25) return 15;
      if (reviewsCount <= 50) return 20;
      if (reviewsCount <= 100) return 30;
      if (reviewsCount <= 250) return 40;
      if (reviewsCount <= 500) return 65;
      if (reviewsCount <= 1000) return 75;
      return 100; // voor 1000+ reviews
    }
    if (totalProducts <= 2000) {
      if (reviewsCount === 0) return 0;
      if (reviewsCount <= 5) return 5;
      if (reviewsCount <= 10) return 10;
      if (reviewsCount <= 25) return 15;
      if (reviewsCount <= 50) return 20;
      if (reviewsCount <= 100) return 35;
      if (reviewsCount <= 250) return 50;
      if (reviewsCount <= 500) return 70;
      if (reviewsCount <= 1000) return 75;
      return 100; // voor 1000+ reviews
    }
    // 2000+ products
    if (reviewsCount === 0) return 0;
    if (reviewsCount <= 5) return 5;
    if (reviewsCount <= 10) return 10;
    if (reviewsCount <= 25) return 15;
    if (reviewsCount <= 50) return 20;
    if (reviewsCount <= 100) return 45;
    if (reviewsCount <= 250) return 65;
    if (reviewsCount <= 500) return 75;
    if (reviewsCount <= 1000) return 85;
    return 100; // voor 1000+ reviews
  }


  async function fetchTotalProducts(shopUrl) {
    let page = 1;
    let total = 0;
    let keepGoing = true;

    while (keepGoing && page < 20) { // cap bij 20 pagina's voor veiligheid
      try {
        const res = await fetch(`${shopUrl}/products.json?limit=250&page=${page}`);
        if (!res.ok) break;

        const json = await res.json();
        const products = json.products || [];

        total += products.length;
        if (products.length < 250) {
          keepGoing = false;
        } else {
          page++;
        }
      } catch (err) {
        console.warn("⚠️ Fout bij ophalen van producten:", err.message);
        break;
      }
    }

    return total;
  }



  try {
    const { store_id, store_url } = req.body;

    if (!store_id || !store_url) {
      return res.status(400).send("store_id en store_url zijn verplicht.");
    }

    // Normaliseer URL naar domein zonder pad
    let inputUrl = store_url.trim();
    if (!inputUrl.startsWith("http")) {
      inputUrl = "https://" + inputUrl;
    }

    let url;
    try {
      url = new URL(inputUrl);
    } catch (err) {
      return res.status(400).send("Ongeldige store_url.");
    }

    // Gebruik alleen domeinnaam (geen pad/query)
    const normalizedStoreUrl = url.hostname.replace(/^www\./, "");


    url.pathname = "/collections/all";

    const response = await fetch(url.toString());
    const html = await response.text();

    const langMatch = html.match(/<html[^>]+lang=["']?([a-zA-Z-]+)["']?/);
    const lang = langMatch ? langMatch[1].toLowerCase() : null;

    const productCount = await fetchTotalProducts(`https://${url.hostname}`);


    // Trustpilot: aantal reviews ophalen
    // Trustpilot: aantal reviews ophalen
    let reviewsCount = 0;
    try {
      const trustpilotDomain = url.hostname.replace(/^www\./, "");
      const trustpilotUrl = `https://www.trustpilot.com/review/${trustpilotDomain}`;
      const trustRes = await fetch(trustpilotUrl);
      const trustHtml = await trustRes.text();

      const $ = cheerio.load(trustHtml);

      // Zoek binnen de container met class "styles_businessInfoColumnTop__..."
      const container = $('[class^="styles_businessInfoColumnTop__"]');

      const reviewText = container.find('h1 span:contains("Reviews")').text(); // bijv. "Reviews 283"
      const match = reviewText.match(/Reviews\s+([\d,.]+)/i);
      if (match && match[1]) {
        reviewsCount = parseInt(match[1].replace(/[.,]/g, ""));
      }
    } catch (err) {
      console.warn(`⚠️ Kon Trustpilot reviews niet goed parsen voor ${url.hostname}:`, err.message);
    }





    const faviconMatch = html.match(/<link[^>]+rel=["']?(icon|shortcut icon|apple-touch-icon)["']?[^>]*href=["']?([^"'>\s]+)/i);
    let faviconUrl = faviconMatch ? faviconMatch[2] : null;

    if (faviconUrl) {
      const base = `${url.protocol}//${url.hostname}`;

      // Als favicon al absoluut is: niets doen
      if (faviconUrl.startsWith("http")) {
        // ok
      } else if (faviconUrl.startsWith("//")) {
        faviconUrl = `${url.protocol}${faviconUrl}`;// e.g. //cdn.shopify.com → https://cdn.shopify.com
      } else if (faviconUrl.startsWith("/")) {
        faviconUrl = `${base}${faviconUrl}`; // e.g. /cdn/shop/favicon.ico
      } else {
        faviconUrl = `${base}/${faviconUrl}`; // relative path, no slash
      }
    }


    const { error: updateError } = await supabase
      .from("tracked_stores")
      .update({
        store_url: normalizedStoreUrl,
        lang,
        favicon_url: faviconUrl,
        products_to_track: calculateProductsToTrack(reviewsCount, productCount),
        updated_at: new Date().toISOString(),
        total_products: productCount,
        number_tp_reviews: reviewsCount,

      })
      .eq("id", store_id);

    if (updateError) {
      console.error("❌ Fout bij updaten lang/fav:", updateError);
      return res.status(500).send("Fout bij opslaan.");
    }

    console.log(`✅ Lang (${lang}) en ${reviewsCount} en favicon toegevoegd voor ${normalizedStoreUrl}`);
    return res.status(200).send({ lang, favicon_url: faviconUrl });
  } catch (err) {
    console.error("❌ Fout in setStoreLangFromHTML:", err.message);
    return res.status(500).send("Er ging iets mis.");
  }
});



exports.addImportHistoryItem = functions.https.onRequest((req, res) => {
  // CORS Headers instellen
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  cors(req, res, async () => {
    try {
      if (req.method !== "POST") {
        return res.status(405).json({ message: "Method Not Allowed" });
      }

      const { csvUrl, settings, productData, tokens, products_processed, total_products } = req.body;

      // Valideer de vereiste gegevens
      if (
        !csvUrl ||
        !settings ||
        !settings.UID ||
        !settings.name ||
        !productData
      ) {
        return res.status(400).json({ message: "Ongeldige gegevens ontvangen" });
      }

      const supabaseUserId = settings.UID;
      const name = settings.name;

      // Genereer een unieke bulkeditid, bijvoorbeeld met de huidige timestamp
      const bulkeditid = Date.now().toString();

      // Bouw het newData object, waarin we alle relevante info opslaan
      const newData = {
        type: "Import (From urls)",
        status: "Finished",
        tokens: tokens || 0,
        products_processed: products_processed || 0,
        total_products: total_products || (Array.isArray(productData) ? productData.length : 0),
        output_file: csvUrl,
        name: name,
        product_data: productData, // Hier slaan we de volledige productdata op
      };

      // Voeg het history-item toe of update het, indien al aanwezig
      const historyItem = await createOrUpdateSupabaseHistory(
        supabaseUserId,
        bulkeditid,
        newData
      );

      if (historyItem) {
        return res.status(200).json({
          message: "History item toegevoegd",
          historyItem,
        });
      } else {
        return res.status(500).json({
          message: "Fout bij het toevoegen van history item",
        });
      }
    } catch (error) {
      console.error("Onverwachte fout:", error);
      return res.status(500).json({ message: "Interne serverfout." });
    }
  });
});








/**
 * Hulpfunctie voegt een usage item toe aan de `usage` table in Supabase.
 * Voeg een usage-item toe in de `usage`-tabel van Supabase.
 *
 * @param {string} supabaseUserId - De gebruiker in Supabase.
 * @param {string} bulkeditid - De bijbehorende bulkedit ID.
 * @param {string} type - Het type usage (bv. "AI edit").
 * @param {number} tokensUsed - Aantal gebruikte tokens (kan negatief zijn).
 * @return {Promise<object|null>} De aangemaakte usage-row of null bij error.
 */
async function addSupabaseUsageItem(
  supabaseUserId,
  bulkeditid,
  type,
  tokensUsed, // mag negatief zijn
) {
  const supabase = await initSupabase();

  const { data, error } = await supabase.from("usage").insert([
    {
      user_id: supabaseUserId,
      bulkeditid: bulkeditid,
      type: type,
      tokens: tokensUsed,
      // date wordt automatisch default
    },
  ]);

  if (error) {
    console.error("Supabase USAGE insert error:", error);
    return null;
  }

  return data;
}






/**
 * Firebase Cloud Function: importProducts
 *
 * Deze functie ontvangt een POST-request met:
 *   - csvUrl: De URL naar het CSV-bestand met de productdata.
 *   - settings: Een object met minimaal UID en name.
 *
 * De functie slaat een history item op in Supabase met type "Import".
 * Dit record blijft gescheiden van bewerkingen of AI-edits.
 */
exports.importProducts = functions.https.onRequest((req, res) => {
  // Stel de CORS-headers in
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  cors(req, res, async () => {
    try {
      if (req.method !== "POST") {
        return res.status(405).json({ message: "Method Not Allowed" });
      }

      const { csvUrl, settings } = req.body;
      if (!csvUrl || !settings || !settings.UID || !settings.name) {
        return res.status(400).json({ message: "Invalid data received" });
      }

      // Definieer totalProducts voordat we deze gebruiken
      const totalProducts = settings.totalProducts || 0;

      const supabaseUserId = settings.UID;
      const name = settings.name;
      const bulkeditid = Date.now().toString();

      const newData = {
        type: "Import (From urls)",
        status: "Finished",
        tokens: 0,
        products_processed: totalProducts,
        total_products: totalProducts,
        output_file: csvUrl,
        name: name,
      };

      const historyItem = await createOrUpdateSupabaseHistory(supabaseUserId, bulkeditid, newData);
      if (historyItem) {
        return res.status(200).json({
          message: "Import history item created successfully",
          historyItem: historyItem,
        });
      } else {
        return res.status(500).json({ message: "Failed to create history item" });
      }
    } catch (error) {
      console.error("Error in importProducts function:", error);
      return res.status(500).json({ message: "Internal server error" });
    }
  });
});



// handmatige testfuncties


exports.updateTrackedDataManual = functions.https.onRequest(async (req, res) => {
  const supabase = await initSupabase();

  try {
    const { data: stores, error } = await supabase
      .from("tracked_stores")
      .select("*")
      .eq("status", "active");

    if (error) {
      console.error("❌ Fout bij ophalen van tracked_stores:", error);
      return res.status(500).send("Fout bij ophalen van stores.");
    }

    let totaalAantal = 0;

    for (const store of stores) {
      const {
        id: store_id,
        user_id,
        store_url,
        products_to_track = 50,
      } = store;

      const snapshotId = uuidv4(); // ✅ unieke ID per store-run

      let fixedUrl = store_url;
      if (!fixedUrl.startsWith("http")) {
        fixedUrl = "https://" + fixedUrl;
      }

      let collectionUrl;
      try {
        collectionUrl = new URL(fixedUrl);
      } catch (urlError) {
        console.warn(`⚠️ Ongeldige store_url overgeslagen: ${store_url}`);
        continue; // Skip deze store
      }

      collectionUrl.pathname = "/collections/all";
      collectionUrl.searchParams.set("sort_by", "best-selling");

      try {
        const response = await fetch(collectionUrl.toString());
        const html = await response.text();
        const $ = cheerio.load(html);

        const productLinks = new Set();
        $("a[href*='/products/']").each((i, el) => {
          let href = $(el).attr("href");
          if (href.startsWith("//")) href = "https:" + href;
          else if (href.startsWith("/")) href = `${collectionUrl.origin}${href}`;
          else if (!href.startsWith("http")) href = `${collectionUrl.origin}/${href}`;
          productLinks.add(href);
        });

        const limitedProductLinks = Array.from(productLinks).slice(0, products_to_track);

        const trackedData = [];
        let rank = 1;
        const timestamp = new Date().toISOString();

        for (const productUrl of limitedProductLinks) {
          try {
            const jsonUrl = new URL(productUrl);
            jsonUrl.pathname += ".json";

            const productRes = await fetch(jsonUrl.toString());
            if (!productRes.ok) throw new Error(`Bad response: ${productRes.status}`);
            const productJson = await productRes.json();

            const product = productJson?.product;
            if (!product) continue;

            trackedData.push({
              user_id,
              store_id,
              handle: product.handle,
              title: product.title,
              product_id: product.id,
              current_rank: rank,
              source_domain: jsonUrl.hostname.replace(/^www\./, ""),
              source_country: null,
              status: "active",
              timestamp,
              snapshot_id: snapshotId, // ✅ toegevoegd
            });

            rank++;
          } catch (err) {
            console.warn(`⚠️ Fout bij ophalen product JSON: ${productUrl}`, err.message);
          }
        }

        if (trackedData.length > 0) {
          const { error: insertError } = await supabase
            .from("tracked_data")
            .insert(trackedData);

          if (insertError) {
            console.error(`❌ Fout bij opslaan data voor ${store_url}:`, insertError);
          } else {
            console.log(`✅ ${trackedData.length} producten opgeslagen voor ${store_url} (snapshot: ${snapshotId})`);
            totaalAantal += trackedData.length;
          }
        } else {
          console.warn(`⚠️ Geen producten gevonden of ingelezen bij: ${store_url}`);
        }
      } catch (scrapeErr) {
        console.error(`❌ Fout bij verwerken store ${store_url}:`, scrapeErr.message);
      }
    }

    return res.status(200).send(`Handmatig uitgevoerd. Totaal opgeslagen: ${totaalAantal} producten.`);
  } catch (err) {
    console.error("❌ Fout in updateTrackedDataManual (algemeen):", err.message);
    return res.status(500).send("Er ging iets mis.");
  }
});


exports.calculateRankRisersManual = functions.https.onRequest(async (req, res) => {
  const supabase = await initSupabase();

  try {
    const { data: stores, error } = await supabase
      .from("tracked_stores")
      .select("*")
      .eq("status", "active");

    if (error) {
      console.error("❌ Fout bij ophalen stores:", error);
      return res.status(500).send("Fout bij ophalen stores.");
    }

    let totaalWinners = 0;

    for (const store of stores) {
      const { id: store_id, user_id, store_url } = store;

      const { data: snapshots, error: snapErr } = await supabase
        .from("tracked_data")
        .select("snapshot_id, timestamp")
        .eq("store_id", store_id)
        .not("snapshot_id", "is", null)
        .order("timestamp", { ascending: false })
        .limit(20);

      if (snapErr || !snapshots || snapshots.length < 2) {
        console.warn(`⚠️ Geen snapshots gevonden voor ${store_url}`);
        continue;
      }

      const uniqueSnapshots = Array.from(
        new Map(snapshots.map((s) => [s.snapshot_id, s.timestamp]))
      );

      if (uniqueSnapshots.length < 2) {
        console.warn(`⚠️ Niet genoeg unieke snapshots voor ${store_url}`);
        continue;
      }

      const [latestEntry, previousEntry] = uniqueSnapshots;
      const latestId = latestEntry[0];
      const previousId = previousEntry[0];

      console.log(`🔄 Vergelijken snapshots voor ${store_url}`);
      console.log(`➡️ Nieuwste snapshot: ${latestId}`);
      console.log(`⬅️ Vorige  snapshot: ${previousId}`);

      const { data: latestData } = await supabase
        .from("tracked_data")
        .select("*")
        .eq("store_id", store_id)
        .eq("snapshot_id", latestId);

      const { data: previousData } = await supabase
        .from("tracked_data")
        .select("*")
        .eq("store_id", store_id)
        .eq("snapshot_id", previousId);

      const previousMap = new Map();
      for (const item of previousData) {
        previousMap.set(item.handle, item);
      }

      const winners = [];

      for (const latestItem of latestData) {
        const prev = previousMap.get(latestItem.handle);
        if (!prev) continue;

        const oldRank = prev.current_rank;
        const newRank = latestItem.current_rank;

        const alreadyExists = await supabase
          .from("tracked_winners")
          .select("id")
          .eq("store_id", store_id)
          .eq("handle", latestItem.handle)
          .limit(1);

        if (alreadyExists.data && alreadyExists.data.length > 0) {
          console.log(`⏭️ Winner ${latestItem.handle} al aanwezig, skip`);
          continue;
        }

        if (
          typeof oldRank === "number" &&
          typeof newRank === "number" &&
          newRank < oldRank
        ) {
          console.log(`🏆 RANKRISER: ${latestItem.handle} van ${oldRank} → ${newRank}`);

          winners.push({
            user_id,
            store_id,
            handle: latestItem.handle,
            title: latestItem.title,
            product_id: latestItem.product_id,
            unique_id: latestItem.unique_id || null,
            start_rank: oldRank,
            current_rank: newRank,
            source_domain: latestItem.source_domain,
            source_country: latestItem.source_country,
            status: "winner",
            processed: false,
            timestamp: latestItem.timestamp,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
          });
        }
      }

      if (winners.length > 0) {
        const { error: insertError } = await supabase
          .from("tracked_winners")
          .insert(winners);

        if (insertError) {
          console.error(`❌ Fout bij insert winners voor ${store_url}:`, insertError);
        } else {
          console.log(`✅ ${winners.length} winners toegevoegd voor ${store_url}`);
          totaalWinners += winners.length;
        }
      } else {
        console.log(`📉 Geen nieuwe stijgers bij ${store_url}`);
      }
    }

    return res.status(200).send(`Rankrisers scan afgerond. Totaal toegevoegd: ${totaalWinners}`);
  } catch (err) {
    console.error("❌ Fout in calculateRankRisersManual:", err.message);
    return res.status(500).send("Er ging iets mis.");
  }
});


exports.processTrackedWinnersManual = functions.https.onRequest(async (req, res) => {
  const supabase = await initSupabase();

  try {
    const { data: winners, error } = await supabase
      .from("tracked_winners")
      .select("*")
      .eq("processed", false);

    if (error) {
      console.error("❌ Fout bij ophalen winners:", error);
      return res.status(500).send("Fout bij ophalen winners.");
    }

    if (!winners || winners.length === 0) {
      console.log("📭 Geen nieuwe winners om te verwerken.");
      return res.status(200).send("Geen nieuwe winners gevonden.");
    }

    const winnersByUser = {};

    for (const winner of winners) {
      if (!winnersByUser[winner.user_id]) {
        winnersByUser[winner.user_id] = [];
      }
      winnersByUser[winner.user_id].push(winner);
    }

    let totaalToegevoegd = 0;

    for (const userId in winnersByUser) {
      const userWinners = winnersByUser[userId];
      const transformedProducts = [];
      const processedWinnerIds = [];

      for (const winner of userWinners) {
        const baseUrl = winner.source_domain?.startsWith("http")
          ? winner.source_domain
          : `https://${winner.source_domain}`;
        const productUrl = `${baseUrl}/products/${winner.handle}.json`;

        try {
          // ✅ Als de lang nog leeg is voor deze store, haal hem op
          const { data: storeData } = await supabase
            .from("tracked_stores")
            .select("*")
            .eq("id", winner.store_id)
            .single();

          if (storeData && !storeData.lang) {
            try {
              const htmlRes = await fetch(`${baseUrl}`);
              const rawHtml = await htmlRes.text();
              const match = rawHtml.match(/<html[^>]+lang=["']?([a-zA-Z-]+)["']?/i);
              const langValue = match ? match[1].toLowerCase() : null;

              if (langValue) {
                await supabase
                  .from("tracked_stores")
                  .update({ lang: langValue, updated_at: new Date().toISOString() })
                  .eq("id", storeData.id);
                console.log(`🌍 Lang opgehaald voor store ${baseUrl}: ${langValue}`);
              } else {
                console.warn(`⚠️ Kon lang niet vinden voor store: ${baseUrl}`);
              }
            } catch (err) {
              console.warn(`⚠️ Fout bij ophalen van lang attribuut voor ${baseUrl}:`, err.message);
            }
          }

          // ✅ Haal product.json op
          const response = await fetch(productUrl);
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }

          const data = await response.json();
          const product = data?.product;

          if (!product) {
            console.warn(`⚠️ Geen productdata gevonden voor ${winner.handle}`);
            continue;
          }

          let finalHandle = product.handle;

          const { data: exists } = await supabase
            .from("products")
            .select("id")
            .eq("userid", userId)
            .eq("product_data->product->>handle", finalHandle)
            .limit(1);

          if (exists && exists.length > 0) {
            const random = Math.floor(100 + Math.random() * 900);
            finalHandle = `${product.handle}-${random}`;
            product.handle = finalHandle;
            console.log(`⚠️ Handle aangepast naar: ${finalHandle}`);
          }

          const timestamp = new Date().toISOString();
          product.created_at = timestamp;
          product.updated_at = timestamp;

          transformedProducts.push({
            userid: userId,
            title: product.title,
            price: product.variants?.[0]?.price ?? null,
            image: product.image?.src ?? null,
            in_app_tags: [],
            source_type: "Tracked",
            source_domain: winner.source_domain || null,
            source_country: storeData?.lang || null,
            edit_type: "Original",
            ranking: winner.current_rank,
            product_data: { product },
          });

          processedWinnerIds.push(winner.id);
        } catch (err) {
          console.warn(`❌ Fout bij ophalen van ${productUrl}:`, err.message);
        }
      }

      if (transformedProducts.length > 0) {
        const { data: inserted, error: insertError } = await supabase
          .from("products")
          .insert(transformedProducts)
          .select();

        if (insertError) {
          console.error("❌ Fout bij toevoegen van producten:", insertError);
        } else {
          const { error: updateError } = await supabase
            .from("tracked_winners")
            .update({
              processed: true,
              updated_at: new Date().toISOString(),
            })
            .in("id", processedWinnerIds);

          if (updateError) {
            console.warn("⚠️ Fout bij updaten winners:", updateError);
          }

          console.log(`✅ ${inserted.length} producten toegevoegd voor user ${userId}`);
          totaalToegevoegd += inserted.length;
        }
      }
    }

    return res.status(200).send(`Klaar. Totaal producten toegevoegd: ${totaalToegevoegd}`);
  } catch (err) {
    console.error("❌ Fout in processTrackedWinnersManual:", err.message);
    return res.status(500).send("Er ging iets mis.");
  }
});


exports.refreshStoreMetadataManual = functions.https.onRequest(async (req, res) => {
  // CORS headers
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.status(204).send("");
  }

  const supabase = await initSupabase();

  try {
    const { data: stores, error } = await supabase
      .from("tracked_stores")
      .select("id, store_url");

    if (error || !stores || stores.length === 0) {
      console.error("❌ Kan stores niet ophalen:", error);
      return res.status(500).send("Geen stores gevonden.");
    }

    let updatedCount = 0;

    for (const store of stores) {
      try {
        let fixedUrl = store.store_url;
        if (!fixedUrl.startsWith("http")) {
          fixedUrl = "https://" + fixedUrl;
        }

        const url = new URL(fixedUrl);
        url.pathname = "/collections/all";

        const response = await fetch(url.toString());
        const html = await response.text();

        const langMatch = html.match(/<html[^>]+lang=["']?([a-zA-Z-]+)["']?/);
        const lang = langMatch ? langMatch[1].toLowerCase() : null;

        const faviconMatch = html.match(
          /<link[^>]+rel=["']?(icon|shortcut icon|apple-touch-icon)["']?[^>]*href=["']?([^"'>\s]+)/i
        );
        let faviconUrl = faviconMatch ? faviconMatch[2] : null;

        if (faviconUrl) {
          const base = `${url.protocol}//${url.hostname}`;
          if (faviconUrl.startsWith("http")) {
            // already absolute
          } else if (faviconUrl.startsWith("//")) {
            faviconUrl = `${url.protocol}${faviconUrl}`;
          } else if (faviconUrl.startsWith("/")) {
            faviconUrl = `${base}${faviconUrl}`;
          } else {
            faviconUrl = `${base}/${faviconUrl}`;
          }
        }

        const { error: updateError } = await supabase
          .from("tracked_stores")
          .update({
            lang,
            favicon_url: faviconUrl,
            updated_at: new Date().toISOString(),
          })
          .eq("id", store.id);

        if (updateError) {
          console.warn(`⚠️ Kon store ${store.store_url} niet updaten`, updateError);
        } else {
          updatedCount++;
          console.log(`✅ Metadata bijgewerkt voor ${store.store_url}`);
        }
      } catch (innerErr) {
        console.warn(`❌ Fout bij verwerken van ${store.store_url}:`, innerErr.message);
      }
    }

    return res.status(200).send(`Klaar! ${updatedCount} stores bijgewerkt.`);
  } catch (err) {
    console.error("❌ Algemene fout:", err.message);
    return res.status(500).send("Er ging iets mis.");
  }
});



// Minimalistische testfunctie om CORS en response handling te verifiëren

exports.testOptimizeShopifyCsv = functions.https.onRequest((req, res) => {
  cors(req, res, () => {
    // Directe response zonder async
    res.status(200).json({ success: true, message: "Testfunctie correct." });
  });
});






exports.addProductsFromFrontend = functions.https.onRequest(async (req, res) => {
  // CORS headers
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    return res.status(204).send("");
  }

  return cors(req, res, async () => {
    try {
      if (req.method !== "POST") {
        return res.status(405).json({ message: "Method Not Allowed" });
      }

      const {
        uid,
        groupedByHandle,
        bulkeditid, // ✅ nieuw
        editType = "manual",
        sourceType,
        sourcePlatform = "Shopify",
        sourceCountry = null,
      } = req.body;

      if (!uid || !groupedByHandle || typeof groupedByHandle !== "object") {
        return res.status(400).json({ message: "Ongeldige gegevens ontvangen" });
      }

      const supabase = await initSupabase();


      // ✅ Geef bulkeditid mee als import_id
      const result = await addProducts(uid, groupedByHandle, supabase, editType, sourceType, sourcePlatform, sourceCountry, bulkeditid);


      if (!result) {
        return res.status(500).json({ message: "Fout bij toevoegen van producten aan Supabase." });
      }

      return res.status(200).json({
        message: "✅ Producten succesvol toegevoegd.",
        count: result.length,
        result,
      });
    } catch (error) {
      console.error("❌ Fout in addProductsFromFrontend:", error);
      return res.status(500).json({ message: "Interne serverfout." });
    }
  });
});


exports.optimizeProductsByIds = functions.https.onRequest((req, res) => {
  return cors(req, res, async () => {
    try {
      const { productIds, edits, models, settings } = req.body;

      if (
        !productIds ||
        !Array.isArray(productIds) ||
        productIds.length === 0 ||
        !edits ||
        !models ||
        !settings ||
        !settings.UID
      ) {
        return res.status(400).json({ message: "Missing fields in request." });
      }

      const supabase = await initSupabase();

      const { data: products, error } = await supabase
        .from("products")
        .select("*")
        .in("id", productIds);

      if (error || !products || products.length === 0) {
        return res.status(500).json({ message: "Failed to fetch products from Supabase." });
      }

      // Maak de CSV
      const csvContent = generateShopifyCsvFromSupabaseProducts(products); // ← bestaande helper

      // Upload naar GCS
      const fileName = `ai-edit-${Date.now()}.csv`;
      const file = storage.bucket(bucketName).file(fileName);

      await file.save(csvContent, {
        metadata: { contentType: "text/csv" },
        resumable: false,
      });

      const [signedUrl] = await file.getSignedUrl({
        action: "read",
        expires: Date.now() + 7 * 24 * 60 * 60 * 1000,
      });

      // Maak history item aan
      const bulkeditid = Date.now().toString();
      settings.bulkeditid = bulkeditid;
      settings.startIndex = 0;
      settings.total_products = productIds.length;


      await createOrUpdateSupabaseHistory(settings.UID, bulkeditid, {
        type: "AI edit",
        name: settings.name || "AI-edit",
        output_file: signedUrl,
        status: "Processing",
        total_products: products.length,
        tokens: 0,
        products_processed: 0,
      });

      // Cloud Task starten
      const parent = tasksClient.queuePath(
        "ecomai-3730f",
        "us-central1",
        "my-queue"
      );

      const taskPayload = {
        csvUrl: signedUrl,
        csvContent,
        edits,
        models,
        settings,
      };

      const task = {
        httpRequest: {
          httpMethod: "POST",
          url: "https://us-central1-ecomai-3730f.cloudfunctions.net/processOptimizeShopifyCsvTask",
          headers: { "Content-Type": "application/json" },
          body: Buffer.from(JSON.stringify(taskPayload)).toString("base64"),
        },
      };

      const [cloudTask] = await tasksClient.createTask({ parent, task });

      console.log("✅ Cloud Task aangemaakt voor optimizeProductsByIds:", cloudTask.name);

      return res.status(200).json({
        success: true,
        bulkeditid,
        message: "AI-optimalisatie gestart.",
      });
    } catch (error) {
      console.error("❌ Fout in optimizeProductsByIds:", error);
      return res.status(500).json({ message: "Interne serverfout." });
    }
  });
});


function generateShopifyCsvFromSupabaseProducts(products) {
  const headers = [
    "Handle",
    "Title",
    "Body (HTML)",
    "Vendor",
    "Product Category",
    "Type",
    "Tags",
    "Published",
    "Option1 Name",
    "Option1 Value",
    "Option1 Linked To",
    "Option2 Name",
    "Option2 Value",
    "Option2 Linked To",
    "Option3 Name",
    "Option3 Value",
    "Option3 Linked To",
    "Variant SKU",
    "Variant Grams",
    "Variant Inventory Tracker",
    "Variant Inventory Policy",
    "Variant Fulfillment Service",
    "Variant Price",
    "Variant Compare At Price",
    "Variant Requires Shipping",
    "Variant Taxable",
    "Variant Barcode",
    "Image Src",
    "Image Position",
    "Image Alt Text",
    "Gift Card",
    "SEO Title",
    "SEO Description",
    "Google Shopping / Google Product Category",
    "Google Shopping / Gender",
    "Google Shopping / Age Group",
    "Google Shopping / MPN",
    "Google Shopping / Condition",
    "Google Shopping / Custom Product",
    "Google Shopping / Custom Label 0",
    "Google Shopping / Custom Label 1",
    "Google Shopping / Custom Label 2",
    "Google Shopping / Custom Label 3",
    "Google Shopping / Custom Label 4",
    "Variant Image",
    "Variant Weight Unit",
    "Variant Tax Code",
    "Cost per item",
    "Status",
    "Variant Shopify ID",          // ✅ voor consistentie in back-end
    "Image Variant IDs",           // ✅ nieuwe kolom voor mapping
  ];

  const rows = [headers.join(",")];

  products.forEach((product) => {
    const raw = typeof product.product_data === "string"
      ? JSON.parse(product.product_data)
      : product.product_data;

    const data = raw?.product;
    if (!data || !data.variants || !Array.isArray(data.variants)) return;

    const handle = data.handle;
    const optionNames = data.options?.map((opt) => opt.name) || [];
    const defaultImage = data.image?.src || "";
    const images = Array.isArray(data.images) ? data.images : [];

    // 🔁 Build variantId → image(s) map
    const variantImageMap = {};
    images.forEach((img) => {
      if (Array.isArray(img.variant_ids)) {
        img.variant_ids.forEach((variantId) => {
          if (!variantImageMap[variantId]) variantImageMap[variantId] = [];
          variantImageMap[variantId].push(img);
        });
      }
    });

    data.variants.forEach((variant, i) => {
      const isFirst = i === 0;
      const matchingImages = variantImageMap[variant.id] || [];
      const firstImage = matchingImages[0] || images[i] || null;

      const row = {
        "Handle": handle,
        "Title": isFirst ? data.title : "",
        "Body (HTML)": isFirst ? data.body_html : "",
        "Vendor": isFirst ? data.vendor : "",
        "Product Category": isFirst ? data.product_category || "" : "",
        "Type": isFirst ? data.product_type : "",
        "Tags": isFirst ? data.tags : "",
        "Published": isFirst ? "TRUE" : "",
        "Option1 Name": isFirst ? optionNames[0] || "" : "",
        "Option1 Value": variant.option1 || "",
        "Option1 Linked To": "",
        "Option2 Name": isFirst ? optionNames[1] || "" : "",
        "Option2 Value": variant.option2 || "",
        "Option2 Linked To": "",
        "Option3 Name": isFirst ? optionNames[2] || "" : "",
        "Option3 Value": variant.option3 || "",
        "Option3 Linked To": "",
        "Variant SKU": variant.sku || "",
        "Variant Grams": variant.grams || 0,
        "Variant Inventory Tracker": variant.inventory_management || "",
        "Variant Inventory Policy": "continue",
        "Variant Fulfillment Service": "manual",
        "Variant Price": parseFloat(variant.price || "0").toFixed(2),
        "Variant Compare At Price": variant.compare_at_price
          ? parseFloat(variant.compare_at_price).toFixed(2)
          : "",
        "Variant Requires Shipping": variant.requires_shipping ? "TRUE" : "FALSE",
        "Variant Taxable": variant.taxable ? "TRUE" : "FALSE",
        "Variant Barcode": variant.barcode || "",
        "Image Src": isFirst ? (firstImage?.src || defaultImage || "") : "",
        "Image Position": isFirst ? (firstImage?.position || 1) : "",
        "Image Alt Text": isFirst ? (firstImage?.alt || "") : "",
        "Gift Card": "FALSE",
        "SEO Title": isFirst ? data.title : "",
        "SEO Description":
          isFirst
            ? (data.body_html || "").replace(/<[^>]+>/g, "").substring(0, 160)
            : "",
        "Google Shopping / Google Product Category": "",
        "Google Shopping / Gender": "",
        "Google Shopping / Age Group": "",
        "Google Shopping / MPN": "",
        "Google Shopping / Condition": "",
        "Google Shopping / Custom Product": "",
        "Google Shopping / Custom Label 0": "",
        "Google Shopping / Custom Label 1": "",
        "Google Shopping / Custom Label 2": "",
        "Google Shopping / Custom Label 3": "",
        "Google Shopping / Custom Label 4": "",
        "Variant Image": matchingImages[0]?.src || defaultImage || "",
        "Variant Weight Unit": variant.weight_unit || "kg",
        "Variant Tax Code": "",
        "Cost per item": "",
        "Status": "active",
        "Variant Shopify ID": variant.id || `${handle}-${i}`,
        "Image Variant IDs": JSON.stringify(matchingImages.map(img => img.variant_ids || []).flat()),
      };

      const escaped = headers.map((key) =>
        `"${String(row[key] ?? "").replace(/"/g, '""')}"`
      );

      rows.push(escaped.join(","));
    });
  });

  return rows.join("\n");
}














exports.startImportFromCsv = functions.https.onRequest((req, res) => {
  return cors(req, res, async () => {
    try {
      const { csvUrl, userId } = req.body;

      console.log("📥 Ontvangen import-verzoek:");
      console.log("🔗 csvUrl:", csvUrl);
      console.log("👤 userId:", userId);

      // TODO: hier verder verwerken (csv ophalen + parse)
      res.status(200).json({ success: true, message: "Verzoek ontvangen." });
    } catch (err) {
      console.error("❌ Fout in startImportFromCsv:", err);
      res.status(500).json({ success: false, message: err.message });
    }
  });
});


exports.importProductsFromCsv = functions.https.onRequest((req, res) => {
  return cors(req, res, async () => {
    try {
      const { csvUrl, userId } = req.body;

      console.log("📥 CSV import gestart:", { csvUrl, userId });

      if (!csvUrl || !userId) {
        return res.status(400).json({ success: false, message: "csvUrl en userId zijn verplicht." });
      }

      const response = await fetch(csvUrl);
      const csvText = await response.text();

      const parsed = Papa.parse(csvText, { header: true });
      const rows = parsed.data.filter((r) => r && r["Handle"]);

      const grouped = {};
      rows.forEach((row) => {
        const handle = row["Handle"];
        if (!grouped[handle]) grouped[handle] = [];
        grouped[handle].push(row);
      });

      const supabase = await initSupabase();

      const bulkeditid = Date.now().toString();

      // Maak history item aan
      await createOrUpdateSupabaseHistory(userId, bulkeditid, {
        status: "Processing",
        type: "Import (Upload)",
        name: "CSV import",
        total_products: 0, // wordt later bijgewerkt
        products_processed: 0,
        tokens: 0,
        output_file: "",
      });


      const insertPayloads = Object.entries(grouped).map(([handle, group]) => {
        const base = group[0];

        const variants = group
          .filter((r) => r["Option1 Value"]?.trim())
          .map((r, i) => {

            const id = r["Variant Shopify ID"] || `${handle}-${i}`;
            return {
              id,
              title: `${r["Option1 Value"] || ""} / ${r["Option2 Value"] || ""}`.trim(),
              price: r["Variant Price"] || "0.00",
              sku: r["Variant SKU"],
              grams: Number(r["Variant Grams"] || 0),
              option1: r["Option1 Value"],
              option2: r["Option2 Value"],
              option3: r["Option3 Value"],
              taxable: r["Variant Taxable"] === "TRUE",
              requires_shipping: r["Variant Requires Shipping"] === "TRUE",
              barcode: r["Variant Barcode"] || null,
              fulfillment_service: "manual",
              inventory_management: "shopify",
              inventory_quantity: Number(r["Variant Inventory Qty"] || 0),
              compare_at_price: r["Variant Compare At Price"] || null,
              weight_unit: r["Variant Weight Unit"] || "kg",
              position: i + 1,
            };
          });

        const images = group
          .filter((r) => r["Image Src"] || r["Variant Image"])
          .map((r, i) => {
            const src = r["Variant Image"] || r["Image Src"];
            const variantId = r["Variant Shopify ID"] || `${handle}-${i}`;
            const isVariantImage = !!r["Variant Image"];

            return {
              src,
              alt: r["Image Alt Text"] || "",
              position: parseInt(r["Image Position"]) || i + 1,
              variant_ids: isVariantImage ? [variantId] : [],
              created_at: new Date().toISOString(),
              updated_at: new Date().toISOString(),
            };
          });

        const productJson = {
          product: {
            id: null,
            title: base["Title"],
            body_html: base["Body (HTML)"],
            vendor: base["Vendor"],
            product_category: base["Product Category"],
            product_type: base["Type"],
            handle,
            tags: base["Tags"],
            published_scope: "global",
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
            published_at: new Date().toISOString(),
            variants,
            options: [
              {
                name: base["Option1 Name"] || "Optie 1",
                position: 1,
                values: [...new Set(group.map((r) => r["Option1 Value"]).filter(Boolean))],
              },
              {
                name: base["Option2 Name"] || "Optie 2",
                position: 2,
                values: [...new Set(group.map((r) => r["Option2 Value"]).filter(Boolean))],
              },
              {
                name: base["Option3 Name"] || "Optie 3",
                position: 3,
                values: [...new Set(group.map((r) => r["Option3 Value"]).filter(Boolean))],
              },
            ].filter((opt) => opt.values.length > 0),
            images,
            image: images?.[0] || null,
          },
        };

        return {
          userid: userId,
          product_data: productJson,
          title: productJson.product.title,
          price: parseFloat(productJson.product.variants?.[0]?.price || "0"),
          image: productJson.product.image?.src || "",
          in_app_tags: [],
          source_type: "upload",
          edit_type: "Original",
          import_id: bulkeditid,
        };
      });

      const { data, error } = await supabase
        .from("products")
        .insert(insertPayloads)
        .select();

      if (error) {
        console.error("❌ Supabase insert error:", error);
        return res.status(500).json({ success: false, error });
      }

      console.log(`✅ ${data.length} producten succesvol geïmporteerd.`);

      // Update history item
      const { error: updateError } = await supabase
        .from("history_items")
        .update({
          status: "Completed",
          total_products: data.length,
          products_processed: data.length,
          updated_at: new Date().toISOString(),
        })
        .eq("user_id", userId)
        .eq("bulkeditid", bulkeditid);

      if (updateError) {
        console.error("❌ Fout bij updaten van history item:", updateError);
      }

      return res.status(200).json({ success: true, count: data.length, bulkeditid });

    } catch (err) {
      console.error("❌ Fout bij CSV import:", err);
      return res.status(500).json({ success: false, error: err.message });
    }
  });
});




exports.triggerSyncShopifyOrdersScheduled = onSchedule(
  {
    schedule: "5 2 * * *", // Elke dag om 02:05
    timeZone: "Europe/Amsterdam",
    region: "us-central1",
  },
  async () => {
    const supabase = await initSupabase();

    try {
      const { data: stores, error } = await supabase
        .from("stores")
        .select("id, store, access_token")
        .eq("status", "active");

      if (error || !stores?.length) {
        console.warn("📭 Geen actieve Shopify stores gevonden.");
        return;
      }

      const parent = tasksClient.queuePath(
        "ecomai-3730f",
        "us-central1",
        "my-queue"
      );
      const processUrl =
        "https://us-central1-ecomai-3730f.cloudfunctions.net/processSyncShopifyOrdersTask";

      for (const store of stores) {
        const payload = {
          store_id: store.id,
          shopify_store: store.store,
          access_token: store.access_token,
        };

        const task = {
          httpRequest: {
            httpMethod: "POST",
            url: processUrl,
            headers: { "Content-Type": "application/json" },
            body: Buffer.from(JSON.stringify(payload)).toString("base64"),
          },
        };

        await tasksClient.createTask({ parent, task });
        console.log(`✅ Task aangemaakt voor store: ${store.store}`);
      }
    } catch (err) {
      console.error("❌ triggerSyncShopifyOrdersScheduled error:", err);
    }
  }
);


exports.processSyncShopifyOrdersTask = functions.https.onRequest(async (req, res) => {
  const supabase = await initSupabase();
  const { store_id, shopify_store, access_token } = req.body;

  if (!store_id || !shopify_store || !access_token) {
    return res.status(400).send("store_id, shopify_store en access_token zijn verplicht.");
  }

  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const start = new Date(yesterday.setHours(0, 0, 0, 0)).toISOString();
  const end = new Date(yesterday.setHours(23, 59, 59, 999)).toISOString();

  const shopifyUrl = `https://${shopify_store}.myshopify.com/admin/api/2023-10/orders.json?created_at_min=${start}&created_at_max=${end}&status=any&limit=250`;

  try {
    const response = await fetch(shopifyUrl, {
      headers: {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
      },
    });

    if (!response.ok) {
      console.error("❌ Fout bij ophalen van Shopify orders:", response.statusText);
      return res.status(500).send("Fout bij ophalen Shopify orders");
    }

    const data = await response.json();
    const orders = data.orders;

    if (!orders?.length) {
      console.log(`📭 Geen orders gevonden voor store ${shopify_store}`);
      return res.status(200).send("Geen orders gevonden.");
    }

    const records = orders.map((order) => ({
      shopify_order_id: order.id,
      store_id: store_id,
      created_at: order.created_at,
      updated_at: order.updated_at,
      order_number: order.name,
      customer_name: `${order.customer?.first_name ?? ""} ${order.customer?.last_name ?? ""}`.trim(),
      total_price: order.total_price,
      currency: order.currency,
      financial_status: order.financial_status,
      fulfillment_status: order.fulfillment_status,
      raw_data: order,
    }));

    const { error: insertError } = await supabase
      .from("orders")
      .upsert(records, { onConflict: "shopify_order_id,store_id" });

    if (insertError) {
      console.error("❌ Fout bij upsert in Supabase:", insertError);
      return res.status(500).send("Upsert fout");
    }

    console.log(`✅ ${records.length} orders opgeslagen voor ${shopify_store}`);
    return res.status(200).send("Orders gesynchroniseerd");
  } catch (err) {
    console.error("❌ Fout in processSyncShopifyOrdersTask:", err);
    return res.status(500).send("Serverfout");
  }
});

exports.syncShopifyOrdersManualDev = functions.https.onRequest(async (req, res) => {
  const supabase = await initSupabase();

  // 🔒 Zet hier je test-store ID
  const store_id = "32b2928c-703e-4b22-9187-1ed0a74cfb0d";

  // Haal store info op
  const { data: store, error } = await supabase
    .from("stores")
    .select("id, store, access_token")
    .eq("id", store_id)
    .single();

  if (error || !store) {
    console.error("❌ Store niet gevonden:", error);
    return res.status(404).send("Store niet gevonden.");
  }

  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const start = new Date(yesterday.setHours(0, 0, 0, 0)).toISOString();
  const end = new Date(yesterday.setHours(23, 59, 59, 999)).toISOString();

  const shopifyUrl = `https://${store.store}.myshopify.com/admin/api/2023-10/orders.json?created_at_min=${start}&created_at_max=${end}&status=any&limit=250`;

  try {
    const response = await fetch(shopifyUrl, {
      headers: {
        "X-Shopify-Access-Token": store.access_token,
        "Content-Type": "application/json",
      },
    });

    if (!response.ok) {
      console.error("❌ Shopify API fout:", await response.text());
      return res.status(500).send("Fout bij ophalen van orders");
    }

    const data = await response.json();
    const orders = data.orders;

    if (!orders?.length) {
      console.log(`📭 Geen orders gevonden voor store ${store.store}`);
      return res.status(200).send("Geen orders gevonden.");
    }

    const records = orders.map((order) => ({
      shopify_order_id: order.id,
      store_id: store.id,
      created_at: order.created_at,
      updated_at: order.updated_at,
      order_number: order.name,
      customer_name: `${order.customer?.first_name ?? ""} ${order.customer?.last_name ?? ""}`.trim(),
      total_price: order.total_price,
      currency: order.currency,
      financial_status: order.financial_status,
      fulfillment_status: order.fulfillment_status,
      raw_data: order,
    }));

    const { error: upsertError } = await supabase
      .from("orders")
      .upsert(records, { onConflict: "shopify_order_id,store_id" });

    if (upsertError) {
      console.error("❌ Fout bij upsert:", upsertError);
      return res.status(500).send("Upsert fout");
    }

    console.log(`✅ ${records.length} orders handmatig opgeslagen voor ${store.store}`);
    return res.status(200).send(`${records.length} orders gesynchroniseerd`);
  } catch (err) {
    console.error("❌ syncShopifyOrdersManualDev error:", err);
    return res.status(500).send("Server error");
  }
});



exports.syncLast60DaysOrdersManual = functions.https.onRequest(async (req, res) => {
  const supabase = await initSupabase();

  try {
    const { data: stores, error } = await supabase
      .from("stores")
      .select("id, store, access_token")
      .eq("status", "active");

    if (error || !stores?.length) {
      console.warn("📭 Geen actieve stores gevonden.");
      return res.status(404).send("Geen stores gevonden.");
    }

    const fromDate = formatISO(subDays(new Date(), 60));
    const toDate = formatISO(new Date());

    for (const store of stores) {
      const url = `https://${store.store}.myshopify.com/admin/api/2023-10/orders.json?created_at_min=${fromDate}&created_at_max=${toDate}&status=any&limit=250`;

      const response = await fetch(url, {
        headers: {
          "X-Shopify-Access-Token": store.access_token,
          "Content-Type": "application/json",
        },
      });

      if (!response.ok) {
        console.error(`❌ Fout bij ophalen orders voor ${store.store}`, await response.text());
        continue;
      }

      const { orders } = await response.json();

      if (!orders || orders.length === 0) {
        console.log(`📭 Geen orders gevonden voor ${store.store}`);
        continue;
      }

      const records = orders.map((order) => ({
        shopify_order_id: order.id,
        store_id: store.id,
        created_at: order.created_at,
        updated_at: order.updated_at,
        order_number: order.name,
        customer_name: `${order.customer?.first_name ?? ""} ${order.customer?.last_name ?? ""}`.trim(),
        total_price: order.total_price,
        currency: order.currency,
        financial_status: order.financial_status,
        fulfillment_status: order.fulfillment_status,
        raw_data: order,
      }));

      const { error: insertError } = await supabase
        .from("orders")
        .upsert(records, { onConflict: "shopify_order_id,store_id" });

      if (insertError) {
        console.error(`❌ Insert error voor ${store.store}`, insertError);
      } else {
        console.log(`✅ ${records.length} orders opgeslagen voor ${store.store}`);
      }
    }

    return res.status(200).send("60 dagen sync voltooid.");
  } catch (err) {
    console.error("❌ syncLast60DaysOrdersManual error:", err);
    return res.status(500).send("Serverfout");
  }
});


exports.syncLast60DaysOrdersById = functions.https.onRequest({
  timeoutSeconds: 300,
  memory: "512MiB",
}, async (req, res) => {
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Headers", "Content-Type");

  const supabase = await initSupabaseWithRetry();

  const storeId = req.query.store_id;

  if (!storeId) {
    return res.status(400).send("❌ 'store_id' is verplicht als query param.");
  }

  try {
    const { data: store, error } = await supabase
      .from("stores")
      .select("id, store, access_token")
      .eq("id", storeId)
      .single();

    if (error || !store) {
      console.error("❌ Store niet gevonden:", JSON.stringify(error, null, 2));

      return res.status(404).send("Store niet gevonden.");
    }

    const fromDate = req.query.from_date || formatISO(subDays(new Date(), 60));
    const toDate = req.query.to_date || formatISO(new Date());


    const orders = await fetchAllShopifyOrders(store, fromDate, toDate);

    const refundRecords = [];

    for (const order of orders) {
      if (order.refunds?.length) {
        for (const refund of order.refunds) {
          const refundTotal = refund.transactions
            ?.filter((t) => t.kind === "refund" && t.status === "success")
            .reduce((sum, t) => sum + parseFloat(t.amount), 0) || 0;

          if (refundTotal > 0) {
            refundRecords.push({
              store_id: store.id,
              shopify_order_id: order.id,
              refunded_at: refund.created_at,
              amount: refundTotal,
            });
          }
        }
      }
    }

    if (refundRecords.length > 0) {
      const { error: refundError } = await supabase
        .from("refunds")
        .upsert(refundRecords, {
          onConflict: "store_id,shopify_order_id,refunded_at"
        });

      if (refundError) {
        console.error("❌ Fout bij upsert van refunds:", refundError);
      } else {
        console.log(`✅ ${refundRecords.length} refunds opgeslagen voor store ${store.store}`);
      }
    }



    if (!orders.length) {
      console.log(`📭 Geen orders gevonden voor ${store.store}`);
      return res.status(200).send("Geen orders gevonden.");
    }

    const records = orders.map((order) => ({
      shopify_order_id: order.id,
      store_id: store.id,
      created_at: order.created_at,
      updated_at: order.updated_at,
      order_number: order.name,
      customer_name: `${order.customer?.first_name ?? ""} ${order.customer?.last_name ?? ""}`.trim(),
      total_price: order.total_price,
      currency: order.currency,
      financial_status: order.financial_status,
      fulfillment_status: order.fulfillment_status,
      raw_data: order,
    }));

    try {
      await batchInsert(supabase, "orders", records);
      console.log(`✅ ${records.length} orders succesvol geüpload in batches.`);
    } catch (err) {
      console.error("❌ Fout bij batch-upsert:", err.message);
      return res.status(500).send("Batch upsert fout");
    }


    console.log(`✅ ${records.length} orders opgeslagen voor store ${store.store}`);
    return res.status(200).send(`${records.length} orders gesynchroniseerd`);
  } catch (err) {
    console.error("❌ Fout in functie:", err.message);
    return res.status(500).send("Server error");
  }
});


async function fetchAllShopifyOrders(store, fromDate, toDate) {
  let orders = [];
  let url = `https://${store.store}.myshopify.com/admin/api/2023-10/orders.json?updated_at_min=${fromDate}&updated_at_max=${toDate}&status=any&limit=250`;
  const headers = {
    "X-Shopify-Access-Token": store.access_token,
    "Content-Type": "application/json",
  };

  while (url) {
    const response = await fetch(url, { headers });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Shopify API fout: ${errorText}`);
    }

    const data = await response.json();
    orders = orders.concat(data.orders);

    const linkHeader = response.headers.get("link");

    if (linkHeader && linkHeader.includes('rel="next"')) {
      const match = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
      url = match ? match[1] : null;
    } else {
      url = null;
    }
  }

  return orders;
}




async function batchInsert(supabase, table, records, batchSize = 200) {
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    const { error } = await supabase
      .from(table)
      .upsert(batch, { onConflict: "shopify_order_id,store_id" });

    if (error) {
      throw new Error(`Upsert fout in batch ${i / batchSize + 1}: ${error.message}`);
    }
  }
}





exports.syncAdspend = functions.https.onRequest(async (req, res) => {

  console.log("🧾 Ontvangen payload:", JSON.stringify(req.body, null, 2));

  const supabase = await initSupabaseWithRetry();

  const dateFnsTz = require("date-fns-tz");
  const zonedTimeToUtc = dateFnsTz.default?.zonedTimeToUtc || dateFnsTz.zonedTimeToUtc;

  const { store_id, timezone, adspend } = req.body;

  if (!store_id || !timezone || !Array.isArray(adspend)) {
    console.warn("❌ Verplichte velden ontbreken:", { store_id, timezone });
    return res.status(400).json({ error: "store_id, timezone en adspend[] zijn verplicht" });
  }

  try {
    const records = [];

    for (const group of adspend) {
      for (const entry of group.results || []) {
        const micros = parseInt(entry?.metrics?.costMicros || "0", 10);
        const amount = micros / 1000000;

        const date = entry?.segments?.date;
        const hour = entry?.segments?.hour;

        if (!date || hour == null) continue;

        const localTimeString = `${date}T${String(hour).padStart(2, "0")}:00:00`;

        // ✅ Hier direct zonedTimeToUtc zonder new Date()
        const utcTimestamp = zonedTimeToUtc(localTimeString, timezone);

        records.push({
          store_id,
          platform: "Google",
          currency: entry?.customer?.currencyCode || "EUR",
          timestamp: utcTimestamp.toISOString(),
          amount,
        });
      }
    }


    if (records.length === 0) {
      console.log("📭 Geen geldige adspend records in payload.");
      return res.status(200).json({ success: true, inserted: 0 });
    }

    const { error } = await supabase
      .from("adspend")
      .upsert(records, { onConflict: "store_id,timestamp,platform" });

    if (error) {
      console.error("❌ Supabase upsert fout:", error);
      return res.status(500).json({ success: false, error });
    }

    console.log(`✅ ${records.length} adspend-records opgeslagen voor store ${store_id}`);
    return res.status(200).json({ success: true, inserted: records.length });
  } catch (err) {
    console.error("❌ Fout in syncAdspend:", err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
});


exports.scheduleSyncRecentOrders = onSchedule(
  {
    schedule: "11 1 * * *", // elke dag om 01:00
    timeZone: "Europe/Amsterdam",
    region: "us-central1"
  },
  async () => {
    const supabase = await initSupabaseWithRetry();

    // 🔁 Bereken datumrange
    const today = new Date();
    const from = subDays(today, 2); // 2 dagen terug
    const to = subDays(today, 1);   // gister

    const fromDate = formatISO(from, { representation: "complete" });
    const toDate = formatISO(
      new Date(to.getFullYear(), to.getMonth(), to.getDate(), 23, 59, 59),
      { representation: "complete" }
    );

    console.log(`🕐 Start sync van ${fromDate} tot ${toDate}`);

    // 📦 Haal alle actieve stores
    const { data: stores, error } = await supabase
      .from("stores")
      .select("id, store")
      .eq("status", "active");

    if (error || !stores?.length) {
      console.warn("❌ Geen actieve stores gevonden:", error);
      return;
    }

    for (const store of stores) {
      try {
        const url = new URL("https://us-central1-ecomai-3730f.cloudfunctions.net/syncLast60DaysOrdersById");
        url.searchParams.set("store_id", store.id);
        url.searchParams.set("from_date", fromDate);
        url.searchParams.set("to_date", toDate);

        const response = await fetch(url.toString());

        if (!response.ok) {
          console.error(`❌ Fout bij store ${store.store}:`, await response.text());
        } else {
          console.log(`✅ Sync gestart voor ${store.store}`);
        }
      } catch (err) {
        console.error(`❌ Request fout voor ${store.store}:`, err.message);
      }
    }
  }
);


exports.scheduleSyncAdspendDaily = onSchedule(
  {
    schedule: "14 1 * * *", // elke dag om 01:15
    timeZone: "Europe/Amsterdam",
    region: "us-central1"
  },
  async () => {
    const supabase = await initSupabaseWithRetry();

    const today = new Date();
    const from = subDays(today, 2); // 2 dagen geleden
    const to = subDays(today, 1);   // gisteren

    const fromDate = formatISO(from, { representation: "date" }); // "YYYY-MM-DD"
    const toDate = formatISO(to, { representation: "date" });     // "YYYY-MM-DD"

    const { data: stores, error } = await supabase
      .from("stores")
      .select("id, name, g_adspend_url, timezone")
      .eq("status", "active");

    if (error || !stores?.length) {
      console.warn("📭 Geen actieve stores gevonden voor adspend sync:", error);
      return;
    }

    for (const store of stores) {
      if (!store.g_adspend_url) {
        console.warn(`⚠️ Store ${store.name} heeft geen adspend webhook ingesteld`);
        continue;
      }

      const payload = {
        store_id: store.id,
        start_date: fromDate,
        end_date: toDate,
        timezone: store.timezone || "Europe/Amsterdam"
      };

      try {
        const response = await fetch(store.g_adspend_url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload)
        });

        const text = await response.text();
        if (!response.ok) {
          console.error(`❌ Fout bij adspend webhook voor ${store.name}: ${text}`);
        } else {
          console.log(`✅ Adspend sync gestart voor ${store.name}`);
        }
      } catch (err) {
        console.error(`❌ Request fout voor ${store.name}:`, err.message);
      }
    }
  }
);




exports.triggerSyncTrackedStoresScheduled = onSchedule(
  {
    schedule: "0 6 * * 0", // elke zondag om 01:00
    timeZone: "Europe/Amsterdam",
    region: "us-central1",
  },
  async () => {
    const supabase = await initSupabase();

    const { data: stores, error } = await supabase
      .from("tracked_stores")
      .select("id, store_url");

    if (error || !Array.isArray(stores) || stores.length === 0) {
      console.warn("📭 Geen stores gevonden.");
      return;
    }

    const batchSize = 25;
    const batches = Array.from(
      { length: Math.ceil(stores.length / batchSize) },
      (_, i) => stores.slice(i * batchSize, i * batchSize + batchSize)
    );

    const parent = tasksClient.queuePath("ecomai-3730f", "us-central1", "my-queue");
    const taskUrl = "https://us-central1-ecomai-3730f.cloudfunctions.net/processTrackedStoresBatch";

    for (let i = 0; i < batches.length; i++) {
      const task = {
        httpRequest: {
          httpMethod: "POST",
          url: taskUrl,
          headers: { "Content-Type": "application/json" },
          body: Buffer.from(JSON.stringify({ stores: batches[i] })).toString("base64"),
        },
        retryConfig: {
          maxAttempts: 3,
          minBackoff: { seconds: 10 },
          maxBackoff: { seconds: 60 },
          maxRetryDuration: { seconds: 3600 },
        },
      };

      await tasksClient.createTask({ parent, task });
      console.log(`✅ Batch ${i + 1}/${batches.length} task aangemaakt`);
    }

    console.log(`🚀 ${stores.length} stores verdeeld over ${batches.length} taken`);
  }
);


exports.processTrackedStoresBatch = onRequest({ timeoutSeconds: 300 }, async (req, res) => {
  const { stores } = req.body;

  if (!stores || !Array.isArray(stores) || stores.length === 0) {
    return res.status(400).send("Geen stores ontvangen.");
  }

  for (const store of stores) {
    try {
      const resp = await fetch("https://us-central1-ecomai-3730f.cloudfunctions.net/setStoreLangFromHTML", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          store_id: store.id,
          store_url: store.store_url,
        }),
      });

      if (!resp.ok) {
        console.warn(`⚠️ Fout voor ${store.store_url}: ${resp.status}`);
      } else {
        console.log(`✅ Verwerkt: ${store.store_url}`);
      }
    } catch (err) {
      console.warn(`❌ Fout bij request naar setStoreLangFromHTML voor ${store.store_url}:`, err.message);
    }
  }

  return res.status(200).send("Batch verwerkt");
});
