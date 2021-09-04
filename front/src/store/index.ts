import Vue from 'vue';
import Vuex, { StoreOptions } from 'vuex';

// Modules
import app from './app';
import appConfig from './app-config';
import verticalMenu from './vertical-menu';
import monitorTypes from './monitorTypes';
import { monitors } from './monitors';
import user from './user';
import { RootState } from './types';

Vue.use(Vuex);

const store: StoreOptions<RootState> = {
  state: {
    isLoading: false,
    loadingMessage: '...',
    login: false,
  },
  mutations: {
    setLoading(state, param) {
      const { loading, message } = param;
      state.isLoading = loading;
      if (message) state.loadingMessage = message;
    },
    setLogin(state, login) {
      state.login = login;
    },
  },
  modules: {
    app,
    appConfig,
    verticalMenu,
    monitorTypes,
    monitors,
    user,
  },
  strict: process.env.DEV,
};
export default new Vuex.Store<RootState>(store);
