import { defineConfig } from "eslint/config";
import globals from "globals";
import tseslint from "typescript-eslint";

import sortDestructureKeys from "eslint-plugin-sort-destructure-keys";
import sortExports from "eslint-plugin-sort-exports";
import stylisticTs from "@stylistic/eslint-plugin-ts";

export default defineConfig([
	...tseslint.configs.recommended,
	{
		files: ["**/*.{js,mjs,cjs,ts}"],
		ignores: ["build/**", "dist/**", "node_modules/**"],
		languageOptions: {
			globals: globals.node,
		},
		plugins: {
			"@sort-destructure-keys": sortDestructureKeys,
			"@sort-exports": sortExports,
			"@stylistic/ts": stylisticTs,
		},
		rules: {
			"@sort-destructure-keys/sort-destructure-keys": ["error", { caseSensitive: true }],
			"@sort-exports/sort-exports": ["error", { sortDir: "asc", sortExportKindFirst: "type" }],
			"@stylistic/ts/member-delimiter-style": "error",
			"comma-dangle": ["error", "always-multiline"],
			"no-console": "error",
			"no-multiple-empty-lines": ["error", { max: 1, maxEOF: 0 }],
			"quotes": ["error", "double"],
			semi: ["error", "always"],
			"sort-imports": ["error", {
				"allowSeparatedGroups": true,
				"ignoreCase": false,
				"ignoreDeclarationSort": false,
				"ignoreMemberSort": false,
				"memberSyntaxSortOrder": ["none", "all", "multiple", "single"],
			}],
			"sort-keys": ["error", "asc", { allowLineSeparatedGroups: true }],
		},
	},
]);
