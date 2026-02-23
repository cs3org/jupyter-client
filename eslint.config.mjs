// @ts-check

import eslint from "@eslint/js";
import { defineConfig, globalIgnores } from "eslint/config";
import tseslint from "typescript-eslint";

export default defineConfig(
  eslint.configs.recommended,
  tseslint.configs.recommended,
  globalIgnores(["node_modules/", "cs3_jupyter_client/", ".venv/", "lib/", "dist/"]),
);
