<template>
  <div>
    <b-card>
      <b-form @submit.prevent>
        <b-row>
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
    <b-card v-show="display">
      <b-table
        striped
        hover
        :fields="columns"
        :items="rows"
        empty-text="無資料"
        show-empty
      />
    </b-card>
  </div>
</template>
<style lang="scss">
@import '@core/scss/vue/libs/vue-select.scss';
</style>
<script lang="ts">
import Vue from 'vue';
import DatePicker from 'vue2-datepicker';
import 'vue2-datepicker/index.css';
import 'vue2-datepicker/locale/zh-tw';
const Ripple = require('vue-ripple-directive');
import moment from 'moment';
import { mapState, mapGetters } from 'vuex';
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
      display: false,
      columns: [
        {
          key: 'dataTime',
          label: '資料時間',
          sortable: true,
        },
        {
          key: 'mt',
          label: '測項',
          sortable: true,
        },
        {
          key: 'modifiedTime',
          label: '修改時間',
          sortable: true,
        },
        {
          key: 'operator',
          label: '註記人員',
          sortable: true,
        },
        {
          key: 'changedStatus',
          label: '註記內容',
          sortable: true,
        },
        {
          key: 'reason',
          label: '註記理由',
          sortable: true,
        },
      ],
      rows: [],
      form: {
        range,
      },
    };
  },
  computed: {
    ...mapState('monitorTypes', ['monitorTypes']),
    ...mapGetters('monitorTypes', ['mtMap']),
  },
  methods: {
    handleReport(data: any) {
      for (const log of data) {
        log.dataTime = moment(log.dataTime).format('lll');
        log.modifiedTime = moment(log.modifiedTime).format('lll');
        log.mt = this.mtMap.get(log.mt).desp;
      }
    },
    async query() {
      this.display = true;
      const url = `/ManualAuditHistory/${this.form.range[0]}/${this.form.range[1]}`;
      const res = await axios.get(url);
      this.handleReport(res.data);
      const ret = res.data;
      this.rows = ret;
    },
  },
});
</script>

<style></style>
