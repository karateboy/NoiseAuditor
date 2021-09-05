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
import Loading from 'vue-loading-overlay';
import VueFormWizard from 'vue-form-wizard';
import 'vue-form-wizard/dist/vue-form-wizard.min.css';

import router from './router';
import store from './store';
import App from './App.vue';
import { ValidationProvider } from 'vee-validate';
import vSelect from 'vue-select';

import FeatherIcon from '@core/components/feather-icon/FeatherIcon.vue';

Vue.component(FeatherIcon.name, FeatherIcon);

// 3rd party plugins
import '@/libs/acl';
import '@/libs/portal-vue';
import '@/libs/toastification';

axios.defaults.baseURL =
  process.env.NODE_ENV === 'development' ? 'http://localhost:9000/' : '';
axios.defaults.withCredentials = true;

// Setup moment
moment.locale('zh_tw');

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
