module.exports = {
  root: true,

  env: {
    node: true,
  },

  extends: ['plugin:vue/essential', 'plugin:vue/recommended', '@vue/prettier'],

  parserOptions: {
    parser: '@typescript-eslint/parser',
  },

  rules: {
    'no-console': process.env.NODE_ENV === 'production' ? 'error' : 'off',
    'no-debugger': process.env.NODE_ENV === 'production' ? 'error' : 'off',
    quotes: 'off',
    semi: 'off',
    'max-len': 'off',
    'linebreak-style': 'off',
    'comma-dangle': 'off',
    camelcase: [
      'error',
      { properties: 'never', ignoreDestructuring: true, ignoreImports: true },
    ],
    'arrow-parens': ['error', 'as-needed'],
    'vue/multiline-html-element-content-newline': 'off',
    'vue/max-attributes-per-line': 'off',
    'vue/component-tags-order': 'off',
    /*
    'vue/max-attributes-per-line': [
      2,
      {
        singleline: 20,
        multiline: {
          max: 1,
          allowFirstLine: false,
        },
      },
    ],
    */
  },

  extends: [
    'plugin:vue/essential',
    'plugin:vue/recommended',
    '@vue/prettier',
    '@vue/typescript',
  ],
};
