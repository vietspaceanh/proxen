import js from "@eslint/js";
import tseslint from "typescript-eslint";
import reactCompiler from "eslint-plugin-react-compiler";
import globals from "globals";

export default tseslint.config(
  {
    ignores: [
      "node_modules/**",
      "proxen/dashboard/**",
      "frontend/dist/**",
    ],
  },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["frontend/src/**/*.{js,jsx,mjs,cjs,ts,tsx}"],
    languageOptions: {
      globals: { ...globals.browser },
      parserOptions: { ecmaFeatures: { jsx: true } },
    },
    plugins: { "react-compiler": reactCompiler },
    rules: {
      "react-compiler/react-compiler": "error",
      "no-empty": ["error", { allowEmptyCatch: true }],
      "no-unused-vars": "off",
      "@typescript-eslint/no-unused-vars": [
        "warn",
        { argsIgnorePattern: "^_", varsIgnorePattern: "^_", caughtErrorsIgnorePattern: "^_" },
      ],
      "@typescript-eslint/no-explicit-any": "off",
    },
  },
);
