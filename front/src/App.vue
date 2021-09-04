<template>
  <div id="app" class="h-100" :class="[skinClasses]">
    <loading :active.sync="isLoading" :is-full-page="true">
      <template #after>
        &nbsp;&nbsp;<i>{{ loadingMessage }}</i>
      </template>
    </loading>
    <component :is="layout">
      <router-view />
    </component>
  </div>
</template>
<style lang="scss">
@import '@core/scss/vue/libs/vue-select.scss';
</style>
<script lang="ts">
// This will be populated in `beforeCreate` hook
import { $themeColors, $themeBreakpoints, $themeConfig } from '../themeConfig';

import { provideToast } from 'vue-toastification/composition';
import { watch } from '@vue/composition-api';
import useAppConfig from './@core/app-config/useAppConfig';
import { mapState } from 'vuex';
import { useWindowSize, useCssVar } from '@vueuse/core';
const Loading = require('vue-loading-overlay');
import 'vue-loading-overlay/dist/vue-loading.css';

import store from '@/store';

const LayoutVertical = () => import('@/layouts/vertical/LayoutVertical.vue');
const LayoutHorizontal = () =>
  import('@/layouts/horizontal/LayoutHorizontal.vue');
const LayoutFull = () => import('@/layouts/full/LayoutFull.vue');
import Vue from 'vue';

export default Vue.extend({
  components: {
    // Layouts
    LayoutHorizontal,
    LayoutVertical,
    LayoutFull,
    Loading,
  },
  setup() {
    const { skin, skinClasses } = useAppConfig();

    // If skin is dark when initialized => Add class to body
    if (skin.value === 'dark') document.body.classList.add('dark-layout');

    // Provide toast for Composition API usage
    // This for those apps/components which uses composition API
    // Demos will still use Options API for ease
    provideToast({
      hideProgressBar: true,
      closeOnClick: false,
      closeButton: false,
      icon: false,
      timeout: 3000,
      transition: 'Vue-Toastification__fade',
    });

    // Set Window Width in store
    store.commit('app/UPDATE_WINDOW_WIDTH', window.innerWidth);
    const { width: windowWidth } = useWindowSize();
    watch(windowWidth, val => {
      store.commit('app/UPDATE_WINDOW_WIDTH', val);
    });

    return {
      skinClasses,
    };
  },
  // ! We can move this computed: layout & contentLayoutType once we get to use Vue 3
  // Currently, router.currentRoute is not reactive and doesn't trigger any change
  computed: {
    ...mapState(['isLoading', 'loadingMessage']),
    layout() {
      let meta = this.$route.meta as any;
      if (meta.layout === 'full') return 'layout-full';
      return `layout-${this.contentLayoutType}`;
    },
    contentLayoutType() {
      return this.$store.state.appConfig.layout.type;
    },
  },
  beforeCreate() {
    // Set colors in theme
    const colors = [
      'primary',
      'secondary',
      'success',
      'info',
      'warning',
      'danger',
      'light',
      'dark',
    ];

    const themeColors: any = $themeColors;
    // eslint-disable-next-line no-plusplus
    for (let i = 0, len = colors.length; i < len; i++) {
      themeColors[colors[i]] = useCssVar(
        `--${colors[i]}`,
        document.documentElement,
      ).value.trim();
    }

    // Set Theme Breakpoints
    const breakpoints = ['xs', 'sm', 'md', 'lg', 'xl'];

    const themeBreakpoints: any = $themeBreakpoints;
    // eslint-disable-next-line no-plusplus
    for (let i = 0, len = breakpoints.length; i < len; i++) {
      themeBreakpoints[breakpoints[i]] = Number(
        useCssVar(
          `--breakpoint-${breakpoints[i]}`,
          document.documentElement,
        ).value.slice(0, -2),
      );
    }

    // Set RTL
    const { isRTL } = $themeConfig.layout;
    document.documentElement.setAttribute('dir', isRTL ? 'rtl' : 'ltr');
  },
});
</script>
