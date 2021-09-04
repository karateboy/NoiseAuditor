<template>
  <div>
    <b-card title="感測器管理" class="text-center">
      <b-row class="p-1">
        <b-col>
          <b-button variant="gradient-primary" @click="showAddSensorModal"
            >新增</b-button
          >
        </b-col>
      </b-row>
      <b-table
        responsive
        :fields="columns"
        :items="sensorList"
        bordered
        sticky-header
        style="min-height: 600px"
      >
        <template #cell(group)="row">
          <v-select
            id="group"
            v-model="row.item.group"
            label="name"
            :reduce="grp => grp._id"
            :options="groupList"
            @input="markDirty(row.item)"
          />
        </template>
        <template #cell(operation)="row">
          <p>
            <b-button variant="gradient-danger" @click="deleteSensor(row)"
              >刪除</b-button
            >
          </p>
        </template>
        <template #cell(topic)="row">
          <b-form-input
            v-model="row.item.topic"
            @change="markDirty(row.item)"
          />
        </template>
      </b-table>
      <b-row>
        <b-col>
          <b-button variant="gradient-primary" class="mr-1" @click="save"
            >儲存
          </b-button>
          <b-button variant="outline-secondary" @click="rollback"
            >取消
          </b-button>
        </b-col>
      </b-row>
    </b-card>
    <b-modal
      id="addSensorModal"
      title="新增感測器"
      size="lg"
      modal-class="modal-primary"
      ok-title="確認"
      no-close-on-backdrop
      ok-only
      :ok-disabled="!canAddSensor"
      @ok="addSensor"
    >
      <b-form @submit.prevent>
        <b-form-group label="感測器代碼" label-for="id" label-cols-md="3">
          <b-form-input
            id="id"
            v-model="form.id"
            @change="sensorIDchanged"
          ></b-form-input>
        </b-form-group>
        <b-form-group label="主題" label-for="topic" label-cols-md="3">
          <b-form-input id="topic" v-model="form.topic"></b-form-input>
        </b-form-group>
        <b-form-group label="群組" label-for="group" label-cols-md="3">
          <v-select
            id="group"
            v-model="form.group"
            label="name"
            :reduce="grp => grp._id"
            :options="groupList"
          />
        </b-form-group>
      </b-form>
    </b-modal>
  </div>
</template>
<script lang="ts">
import Vue from 'vue';
const Ripple = require('vue-ripple-directive');
import axios from 'axios';
import { mapGetters, mapActions } from 'vuex';
import { Sensor, Group } from './types';

interface EditSensor extends Sensor {
  dirty: boolean | undefined;
  monitorCase: undefined;
}

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
        key: 'id',
        label: '代碼',
      },
      {
        key: 'topic',
        label: '主題',
        sortable: true,
      },
      {
        key: 'monitor',
        label: '測點代碼',
      },
      {
        key: 'monitorCase.desc',
        label: '測點名稱',
      },
      {
        key: 'group',
        label: '群組',
        sortable: true,
      },
    ];

    let sensorList = Array<EditSensor>();
    let groupList = Array<Group>();
    let form: Sensor = {
      id: '',
      topic: '',
      monitor: '',
      group: '',
    };
    return {
      sensorList,
      columns,
      groupList,
      form,
    };
  },
  computed: {
    ...mapGetters('monitors', ['mMap']),
    canAddSensor(): boolean {
      if (this.form.id && this.form.group) return true;
      else return false;
    },
  },
  async mounted() {
    await this.fetchMonitors();
    await this.getGroupList();
    this.getSensorList();
  },
  methods: {
    ...mapActions('monitors', ['fetchMonitors']),
    async getSensorList() {
      const res = await axios.get('/Sensors');
      if (res.status === 200) {
        this.sensorList = res.data;
        for (let sensor of this.sensorList) {
          sensor.monitorCase = this.mMap.get(sensor.monitor);
        }
      }
    },
    async getGroupList() {
      const res = await axios.get('/Groups');
      if (res.status === 200) {
        this.groupList = res.data;
      }
    },
    showAddSensorModal() {
      this.$bvModal.show('addSensorModal');
    },
    async addSensor() {
      const sensor: Sensor = this.form;
      sensor.monitor = sensor.id;
      const res = await axios.post(`/Sensor/${sensor.id}`, sensor);
      if (res.status == 200) this.$bvModal.msgBoxOk('成功');
      await this.fetchMonitors();
      this.getSensorList();
    },
    sensorIDchanged(id: string) {
      this.form.topic = `WECC/SAQ210/${id}/sensor`;
    },
    async deleteSensor(row: any) {
      const confirm = await this.$bvModal.msgBoxConfirm(
        `確定要刪除${row.item.id}?`,
        { okTitle: '確認', cancelTitle: '取消' },
      );

      if (!confirm) return;

      const id = row.item.id;
      const res = await axios.delete(`/Sensor/${id}`);
      if (res.status == 200) this.$bvModal.msgBoxOk('成功');
      this.getSensorList();
    },
    save() {
      const all = Array<any>();
      for (const sensor of this.sensorList) {
        if (sensor.dirty) {
          all.push(axios.put(`/Sensor/${sensor.id}`, sensor));
        }
      }
      Promise.all(all).then(() => {
        this.$bvModal.msgBoxOk('成功');
        this.getSensorList();
      });
    },
    rollback() {
      this.$bvModal.msgBoxOk('成功');
    },
    markDirty(item: EditSensor) {
      item.dirty = true;
    },
  },
});
</script>
