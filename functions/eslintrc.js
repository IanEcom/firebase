module.exports = {
  env: {
    es2020: true, // Ondersteunt ES2020-features zoals optional chaining
    node: true,
    mocha: true, // Ondersteunt Mocha voor testbestanden
  },
  parserOptions: {
    ecmaVersion: 2020, // Verhoog naar 2020 of hoger
    sourceType: "module", // Gebruik
  },
  extends: [
    "eslint:recommended",
    "google",
  ],
  rules: {
    "no-restricted-globals": ["error", "name", "length"],
    "prefer-arrow-callback": "error",
    "quotes": ["error", "double", {"allowTemplateLiterals": true}],
  },
  overrides: [
    {
      files: ["**/*.spec.js", "**/*.test.js"], // Specificeer testbestanden
      env: {
        mocha: true,
      },
      rules: {
        // Voeg specifieke regels voor testbestanden toe indien nodig
      },
    },
  ],
  globals: {
    // Definieer hier globale variabelen als dat nodig is
  },
};
