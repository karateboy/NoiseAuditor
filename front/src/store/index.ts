import Vue from 'vue';
import Vuex, { StoreOptions } from 'vuex';

// Modules
import app from './app';
import appConfig from './app-config';
import verticalMenu from './vertical-menu';
import user from './user';
import { ReportID, RootState } from './types';

Vue.use(Vuex);

const store: StoreOptions<RootState> = {
  state: {
    isLoading: false,
    loadingMessage: '...',
    login: false,
    activeReportIDs: Array<ReportID>(),
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
    setActiveReportIDs(state, param: Array<ReportID>) {
      state.activeReportIDs = param;
    },
  },
  modules: {
    app,
    appConfig,
    verticalMenu,
    user,
  },
  strict: process.env.DEV,
};
export default new Vuex.Store<RootState>(store);
