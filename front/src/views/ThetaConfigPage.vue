<template>
  <div>
    <h3>校正項目</h3>
    <b-table-simple>
      <b-thead>
        <b-tr>
          <b-th>測項</b-th>
          <b-th>校正值</b-th>
          <b-th>單位</b-th>
          <b-th></b-th>
        </b-tr>
      </b-thead>
      <b-tbody>
        <b-tr>
          <b-td
            ><v-select
              id="monitorType"
              v-model="calibrationConfig.monitorType"
              label="text"
              :options="thetaMonitorTypes"
              :reduce="mt => mt.value"
          /></b-td>
          <b-td>
            <b-form-input
              v-model.number="calibrationConfig.value"
              type="number"
            />
          </b-td>
          <b-td>
            {{ getUnit(calibrationConfig.monitorType) }}
          </b-td>
          <b-td
            ><b-button
              variant="gradient-primary"
              :disabled="!canAddConfig"
              @click="addCalibrationConfig"
              >新增</b-button
            ></b-td
          >
        </b-tr>
      </b-tbody>
    </b-table-simple>
    <b-table :fields="fields" :items="config.calibrations">
      <template #cell(delete)="row">
        <b-button variant="gradient-danger" @click="deleteRow(row)"
          >刪除</b-button
        >
      </template>
    </b-table>
  </div>
</template>
<style lang="scss">
@import '@core/scss/vue/libs/vue-select.scss';
</style>
<script lang="ts">
import Vue from 'vue';
import { mapGetters, mapActions } from 'vuex';
import { ThetaConfig, CalibrationConfig, TextStrValue } from './types';

export default Vue.extend({
  components: {},
  props: {
    paramStr: {
      type: String,
      default: ``,
    },
  },
  data() {
    let config: ThetaConfig = {
      calibrations: [],
    };

    let calibrationConfig: CalibrationConfig = {
      monitorType: '',
      value: 0,
    };

    if (this.paramStr !== '{}') {
      config = JSON.parse(this.paramStr);
    } else {
      this.$emit('param-changed', JSON.stringify(config));
    }

    let me = this as any;
    return {
      fields: [
        {
          key: 'monitorType',
          label: '測項',
        },
        {
          key: 'value',
          label: '校正值',
        },
        {
          key: 'unit',
          label: '單位',
          formatter: me.formatter,
        },
        {
          key: 'delete',
          label: '',
        },
      ],
      calibrationConfig,
      config,
    };
  },
  computed: {
    ...mapGetters('monitorTypes', ['mtMap']),
    canAddConfig(): boolean {
      if (this.calibrationConfig.monitorType)
        return this.calibrationConfig.value !== 0;

      return false;
    },
    thetaMonitorTypes(): Array<TextStrValue> {
      const mtList = [
        'WD_SPEED',
        'WD_DIR',
        'HUMID',
        'TEMP',
        'PRESS',
        'RAIN',
        'SOLAR',
        'PM25',
        'PM10',
        'CH2O',
        'TVOC',
        'CO2',
        'CO',
        'SO2',
        'NO2',
        'O3',
        'NO',
        'H2S',
        'H2',
        'NH3',
      ];
      if (this.mtMap.size === 0) return [];

      return mtList.map(mt => {
        let ret: TextStrValue = {
          text: this.mtMap.get(mt).desp,
          value: mt,
        };
        return ret;
      });
    },
  },
  async mounted() {
    await this.fetchMonitorTypes();
  },
  methods: {
    ...mapActions('monitorTypes', ['fetchMonitorTypes']),
    onChange(): void {
      this.$emit('param-changed', JSON.stringify(this.config));
    },
    addCalibrationConfig(): void {
      this.config.calibrations.push(Object.assign({}, this.calibrationConfig));
      this.calibrationConfig.monitorType = 'PM25';
      this.calibrationConfig.value = 0;
      this.onChange();
    },
    deleteRow(row: any): void {
      this.config.calibrations.splice(row.index, 1);
      this.onChange();
    },
    getUnit(mt: string): string {
      if (this.mtMap.get(mt)) return this.mtMap.get(mt).unit;
      else return '';
    },
    formatter(value: string, key: string, item: any): string {
      return this.mtMap.get(item.monitorType).unit;
    },
  },
});
</script>
