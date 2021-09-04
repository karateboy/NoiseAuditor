import axios from 'axios';
import { MonitorState, Monitor } from './types';
import { RootState } from '../types';
import { GetterTree } from 'vuex';
import { ActionTree } from 'vuex';
import { MutationTree } from 'vuex';
import { Module } from 'vuex';

const namespaced: boolean = true;
export const state: MonitorState = {
  monitors: [],
};

const getters: GetterTree<MonitorState, RootState> = {
  mMap(state): Map<string, Monitor> {
    const map = new Map<string, Monitor>();
    for (const m of state.monitors) {
      map.set(m._id, m);
    }
    return map;
  },
};

const actions: ActionTree<MonitorState, RootState> = {
  async fetchMonitors({ commit }) {
    try {
      const res = await axios.get('/Monitors');
      if (res.status === 200) {
        commit('setMonitors', res.data);
      }
    } catch (err) {
      throw new Error(err);
    }
  },
};

const mutations: MutationTree<MonitorState> = {
  setMonitors(state, val) {
    state.monitors = val;
  },
};

export const monitors: Module<MonitorState, RootState> = {
  namespaced,
  state,
  getters,
  actions,
  mutations,
};
