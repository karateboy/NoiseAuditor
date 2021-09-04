<template>
  <div>
    <b-card>
      <b-form @submit.prevent>
        <b-row>
          <b-col cols="12">
            <b-form-group label="測點" label-for="monitor" label-cols-md="3">
              <v-select
                id="monitor"
                v-model="form.monitors"
                label="desc"
                :reduce="mt => mt._id"
                :options="monitors"
                multiple
              />
            </b-form-group>
          </b-col>
          <b-col cols="12">
            <b-form-group
              label="測項"
              label-for="monitorType"
              label-cols-md="3"
            >
              <v-select
                id="monitorType"
                v-model="form.monitorTypes"
                label="desp"
                :reduce="mt => mt._id"
                :options="monitorTypes"
                multiple
              />
            </b-form-group>
          </b-col>
          <b-col cols="12">
            <b-form-group
              label="資料種類"
              label-for="dataType"
              label-cols-md="3"
            >
              <v-select
                id="dataType"
                v-model="form.dataType"
                label="txt"
                :reduce="dt => dt.id"
                :options="dataTypes"
              />
            </b-form-group>
          </b-col>
          <b-col cols="12">
            <b-form-group
              label="資料區間"
              label-for="dataRange"
              label-cols-md="3"
            >
              <date-picker
                id="dataRange"
                v-model="form.range"
                :range="true"
                type="datetime"
                format="YYYY-MM-DD HH:mm"
                value-type="timestamp"
                :show-second="false"
              />
            </b-form-group>
          </b-col>
          <!-- submit and reset -->
          <b-col offset-md="3">
            <b-button
              v-ripple.400="'rgba(255, 255, 255, 0.15)'"
              type="submit"
              variant="primary"
              class="mr-1"
              @click="query"
            >
              查詢
            </b-button>
            <b-button
              v-ripple.400="'rgba(186, 191, 199, 0.15)'"
              type="reset"
              variant="outline-secondary"
            >
              取消
            </b-button>
          </b-col>
        </b-row>
      </b-form>
    </b-card>
    <b-card v-show="display" :title="resultTitle">
      <b-table
        striped
        hover
        :fields="columns"
        :items="rows"
        show-empty
        :per-page="15"
        :current-page="currentPage"
        responsive
        sticky-header="800px"
      >
        <template #thead-top>
          <b-tr>
            <b-th></b-th>
            <b-th
              v-for="mt in form.monitorTypes"
              :key="mt"
              :colspan="form.monitors.length"
              class="text-center"
              style="text-transform: none"
              >{{ getMtDesc(mt) }}</b-th
            >
          </b-tr>
        </template>
      </b-table>
      <b-pagination
        v-model="currentPage"
        :total-rows="rows.length"
        :per-page="15"
        first-text="⏮"
        prev-text="⏪"
        next-text="⏩"
        last-text="⏭"
        class="mt-4"
      ></b-pagination>
    </b-card>
  </div>
</template>
<script lang="ts">
import Vue from 'vue';
import DatePicker from 'vue2-datepicker';
import 'vue2-datepicker/index.css';
import 'vue2-datepicker/locale/zh-tw';
const Ripple = require('vue-ripple-directive');
import { mapState, mapGetters, mapActions, mapMutations } from 'vuex';
import moment from 'moment';
import axios from 'axios';

export default Vue.extend({
  components: {
    DatePicker,
  },
  directives: {
    Ripple,
  },

  data() {
    const range = [moment().subtract(1, 'days').valueOf(), moment().valueOf()];
    return {
      dataTypes: [
        { txt: '小時資料', id: 'hour' },
        { txt: '分鐘資料', id: 'min' },
        // { txt: '秒資料', id: 'second' },
      ],
      form: {
        monitors: Array<any>(),
        monitorTypes: Array<any>(),
        dataType: 'min',
        range,
      },
      display: false,
      columns: Array<any>(),
      rows: Array<any>(),
      currentPage: 1,
    };
  },
  computed: {
    ...mapState('monitorTypes', ['monitorTypes']),
    ...mapState('monitors', ['monitors']),
    ...mapGetters('monitorTypes', ['mtMap']),
    ...mapGetters('monitors', ['mMap']),
    resultTitle(): string {
      return `總共${this.rows.length}筆`;
    },
  },
  watch: {},
  async mounted() {
    await this.fetchMonitorTypes();
    await this.fetchMonitors();

    if (this.monitors.length !== 0) {
      this.form.monitors.push(this.monitors[0]._id);
    }

    if (this.monitorTypes.length !== 0) {
      this.form.monitorTypes.push('PM25');
    }
  },
  methods: {
    ...mapActions('monitorTypes', ['fetchMonitorTypes']),
    ...mapActions('monitors', ['fetchMonitors']),
    ...mapMutations(['setLoading']),
    async query() {
      this.setLoading({ loading: true });
      this.display = true;
      this.rows = [];
      this.columns = this.getColumns();
      const monitors = this.form.monitors.join(':');
      const monitorTypes = this.form.monitorTypes.join(':');
      const url = `/HistoryReport/${monitors}/${monitorTypes}/${this.form.dataType}/${this.form.range[0]}/${this.form.range[1]}`;

      const ret = await axios.get(url);
      this.setLoading({ loading: false });
      for (const row of ret.data.rows) {
        row.date = moment(row.date).format('lll');
      }

      this.rows = ret.data.rows;
    },
    cellDataTd(i: number) {
      return (_value: any, _key: any, item: any) =>
        item.cellData[i].cellClassName;
    },
    getMtDesc(mt: string) {
      const mtCase = this.mtMap.get(mt);
      return `${mtCase.desp}(${mtCase.unit})`;
    },
    getColumns() {
      const ret = [];
      ret.push({
        key: 'date',
        label: '時間',
        stickyColumn: true,
      });
      let i = 0;
      for (const mt of this.form.monitorTypes) {
        const mtCase = this.mtMap.get(mt);
        for (const m of this.form.monitors) {
          const mCase = this.mMap.get(m);
          ret.push({
            key: `cellData[${i}].v`,
            label: `${mCase.desc}`,
            tdClass: this.cellDataTd(i),
          });
          i++;
        }
      }

      return ret;
    },
  },
});
</script>

<style></style>
