import Vue from 'vue';
import {
  BootstrapVue,
  IconsPlugin,
  ToastPlugin,
  ModalPlugin,
} from 'bootstrap-vue';
import VueCompositionAPI from '@vue/composition-api';
import axios from 'axios';
import moment from 'moment';
import Highcharts from 'highcharts';
import ex from 'highcharts/modules/exporting';
import csv from 'highcharts/modules/export-data';
import offlineExport from 'highcharts/modules/offline-exporting';
import Loading from 'vue-loading-overlay';
import VueFormWizard from 'vue-form-wizard';
import 'vue-form-wizard/dist/vue-form-wizard.min.css';

import router from './router';
import store from './store';
import App from './App.vue';
import { ValidationProvider } from 'vee-validate';
import vSelect from 'vue-select';

// Global Components
import './global-components';

// 3rd party plugins
import '@/libs/acl';
import '@/libs/portal-vue';
import '@/libs/toastification';

axios.defaults.baseURL =
  process.env.NODE_ENV === 'development' ? 'http://localhost:9000/' : '';
axios.defaults.withCredentials = true;

// Setup moment
moment.locale('zh_tw');

ex(Highcharts);
csv(Highcharts);
offlineExport(Highcharts);
Highcharts.setOptions({
  lang: {
    contextButtonTitle: '圖表功能表',
    downloadJPEG: '下載JPEG',
    downloadPDF: '下載PDF',
    downloadPNG: '下載PNG',
    downloadSVG: '下載SVG',
    downloadCSV: '下載CSV',
    downloadXLS: '下載XLS',
    drillUpText: '回到{series.name}.',
    noData: '無資料',
    months: [
      '1月',
      '2月',
      '3月',
      '4月',
      '5月',
      '6月',
      '7月',
      '8月',
      '9月',
      '10月',
      '11月',
      '12月',
    ],
    printChart: '列印圖表',
    resetZoom: '重設放大區間',
    resetZoomTitle: '回到原圖大小',
    shortMonths: [
      '1月',
      '2月',
      '3月',
      '4月',
      '5月',
      '6月',
      '7月',
      '8月',
      '9月',
      '10月',
      '11月',
      '12月',
    ],
    viewFullscreen: '全螢幕檢視',
    viewData: '檢視資料表',
    weekdays: [
      '星期日',
      '星期一',
      '星期二',
      '星期三',
      '星期四',
      '星期五',
      '星期六',
    ],
  },
});

Vue.use(VueFormWizard);
Vue.component('VSelect', vSelect);

Vue.component('ValidationProvider', ValidationProvider);

// BSV Plugin Registration
Vue.use(BootstrapVue);
Vue.use(IconsPlugin);
Vue.use(ToastPlugin);
Vue.use(ModalPlugin);
Vue.use(Loading);
// Composition API
Vue.use(VueCompositionAPI);

// import core styles
require('@core/scss/core.scss');

// import assets styles
require('@/assets/scss/style.scss');

require('@core/assets/fonts/feather/iconfont.css');

require('vue-loading-overlay/dist/vue-loading.css');

Vue.config.productionTip = false;

router.beforeEach((to, from, next) => {
  if (store.state.login || to.name === 'login') {
    next();
  } else {
    next({ name: 'login' });
  }
});

new Vue({
  router,
  store,
  render: h => h(App),
}).$mount('#app');
