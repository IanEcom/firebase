module.exports = {
  env: {
    es2020: true, // Ondersteunt ES2020-features
    node: true,
    mocha: true, // Voor testbestanden
  },
  parserOptions: {
    ecmaVersion: 2020, // ES2020
    sourceType: "module", // Modules gebruiken
  },
  // Als je de google-stijl niet wilt gebruiken, verwijder je deze regel.
  // Je kunt dan alleen "eslint:recommended" extenden:
  extends: ["eslint:recommended"],
  rules: {
    "no-restricted-globals": ["error", "name", "length"],
    "prefer-arrow-callback": "error",
    "quotes": ["error", "double", { "allowTemplateLiterals": true, "avoidEscape": true }],
    // Pas de indentatie aan naar 2 spaties (of schakel uit als je dit niet wilt laten controleren)
    "indent": ["error", 2],
    // Zet de comma-dangle regel uit (of stel hem in op wat je prettig vindt)
    "comma-dangle": "off",
    // Schakel de object-curly-spacing regel uit zodat er geen melding komt over spaties
    "object-curly-spacing": "off",
  },
  overrides: [
    {
      files: ["**/*.spec.js", "**/*.test.js"],
      env: {
        mocha: true,
      },
      rules: {
        // Specifieke regels voor testbestanden indien nodig
      },
    },
  ],
  globals: {
    // Voeg hier globale variabelen toe als dat nodig is
  },
};
