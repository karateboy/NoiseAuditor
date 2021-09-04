<template>
  <b-table :fields="fields" :items="paramObj.chs">
    <template #cell(index)="data">
      {{ data.index }}
    </template>
    <template #cell(enable)="row">
      <b-form-checkbox v-model="row.item.enable" @change="onChange" />
    </template>
    <template #cell(repairMode)="row">
      <b-form-checkbox v-model="row.item.repairMode" @change="onChange" />
    </template>
    <template #cell(mt)="row">
      <v-select
        id="monitorType"
        v-model="row.item.mt"
        label="desp"
        :reduce="mt => mt._id"
        :options="signalTypes"
        @input="onChange"
      />
    </template>
  </b-table>
</template>
<style lang="scss">
@import '@core/scss/vue/libs/vue-select.scss';
</style>
<script>
import Vue from 'vue';
import { mapState, mapGetters } from 'vuex';
import vSelect from 'vue-select';
import axios from 'axios';

export default Vue.extend({
  components: {
    vSelect,
  },
  props: {
    paramStr: {
      type: String,
      default: ``,
    },
  },
  data() {
    let chs = [];
    for (let i = 0; i < 6; i++) {
      chs.push({
        enable: false,
        mt: undefined,
        repairMode: false,
      });
    }
    let paramObj = { chs };

    if (this.paramStr !== '') paramObj = JSON.parse(this.paramStr);

    const fields = [
      'index',
      {
        key: 'enable',
        label: '啟用',
      },
      {
        key: 'repairMode',
        label: '維修模式',
      },
      {
        key: 'mt',
        label: '測項',
      },
    ];

    return {
      paramObj,
      fields,
      signalTypes: [],
    };
  },
  mounted() {
    this.getSignalTypes();
  },
  methods: {
    async getSignalTypes() {
      const res = await axios.get('/SignalTypes');
      this.signalTypes = res.data;
    },
    justify() {},
    onChange(evt) {
      this.justify();
      this.$emit('param-changed', JSON.stringify(this.paramObj));
    },
  },
});
</script>
