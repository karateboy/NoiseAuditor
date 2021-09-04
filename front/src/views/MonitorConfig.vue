<template>
  <div>
    <b-card title="測點管理" class="text-center">
      <b-table
        responsive
        :fields="columns"
        :items="monitors"
        bordered
        sticky-header
        style="min-height: 600px"
      >
        <template #cell(operation)="row">
          <p>
            <b-button variant="gradient-danger" @click="deleteMonitor(row)"
              >刪除</b-button
            >
          </p>
        </template>
        <template #cell(desc)="row">
          <b-form-input v-model="row.item.desc" @change="markDirty(row.item)" />
        </template>
        <template #cell(monitorTypes)="row">
          <v-select
            id="monitorType"
            v-model="row.item.monitorTypes"
            label="desp"
            :reduce="mt => mt._id"
            :options="monitorTypes"
            multiple
            @input="markDirty(row.item)"
          />
        </template>
      </b-table>
      <b-row>
        <b-col>
          <b-button
            v-ripple.400="'rgba(255, 255, 255, 0.15)'"
            variant="primary"
            class="mr-1"
            @click="save"
          >
            儲存
          </b-button>
          <b-button
            v-ripple.400="'rgba(186, 191, 199, 0.15)'"
            type="reset"
            variant="outline-secondary"
            @click="rollback"
          >
            取消
          </b-button>
        </b-col>
      </b-row>
    </b-card>
  </div>
</template>
<script lang="ts">
import Vue from 'vue';
const Ripple = require('vue-ripple-directive');
import { mapActions, mapState } from 'vuex';
import axios from 'axios';
/*
interface MonitorType {
  _id: string;
  desp: string;
  unit: string;
  prec: number;
  order: number;
  signalType: boolean;
  std_law?: number;
  std_internal?: number;
  zd_internal?: number;
  zd_law?: number;
  span?: number;
  span_dev_internal?: number;
  span_dev_law?: number;
  measuringBy?: Array<string>;
} */

export default Vue.extend({
  components: {},
  directives: {
    Ripple,
  },
  data() {
    const columns = [
      {
        key: 'operation',
        label: '',
      },
      {
        key: '_id',
        label: '代碼',
      },
      {
        key: 'desc',
        label: '名稱',
        sortable: true,
      },
      {
        key: 'monitorTypes',
        label: '測項',
        sortable: true,
      },
    ];

    return {
      display: false,
      columns,
    };
  },
  computed: {
    ...mapState('monitors', ['monitors']),
    ...mapState('monitorTypes', ['monitorTypes']),
  },
  async mounted() {
    await this.fetchMonitors();
    await this.fetchMonitorTypes();
  },
  methods: {
    ...mapActions('monitors', ['fetchMonitors']),
    ...mapActions('monitorTypes', ['fetchMonitorTypes']),
    save() {
      const all = Array<any>();
      for (const m of this.monitors) {
        if (m.dirty) {
          all.push(axios.put(`/Monitor/${m._id}`, m));
        }
      }

      Promise.all(all).then(() => {
        this.fetchMonitors();
        this.$bvModal.msgBoxOk('成功');
      });
    },
    rollback() {
      this.fetchMonitors();
    },
    async deleteMonitor(row: any) {
      const confirm = await this.$bvModal.msgBoxConfirm(
        `確定要刪除${row.item._id}?`,
        { okTitle: '確認', cancelTitle: '取消' },
      );

      if (!confirm) return;

      const _id = row.item._id;
      const res = await axios.delete(`/Monitor/${_id}`);
      if (res.status == 200) this.$bvModal.msgBoxOk('成功');
      this.fetchMonitors();
    },
    markDirty(item: any) {
      item.dirty = true;
    },
  },
});
</script>

<style></style>
