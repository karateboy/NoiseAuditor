import axios from 'axios';

export default {
  namespaced: true,
  state: {
    monitorTypes: [],
  },
  getters: {
    mtMap(state) {
      const map = new Map();
      for (const mt of state.monitorTypes) {
        map.set(mt._id, mt);
      }
      return map;
    },
  },
  mutations: {
    updateMonitorTypes(state, val) {
      state.monitorTypes = val;
    },
  },
  actions: {
    async fetchMonitorTypes({ commit }) {
      try {
        const res = await axios.get('/MonitorType');
        const payload = res && res.data;
        commit('updateMonitorTypes', payload);
      } catch (err) {
        throw new Error(err);
      }
    },
  },
};
