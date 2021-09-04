<template>
  <div>
    <b-card>
      <b-form @submit.prevent>
        <b-row>
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
              label="資料來源"
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
    <b-card v-show="display">
      <b-form @submit.prevent>
        <b-row>
          <b-col cols="12">
            <b-form-group label="註記理由" label-for="reason" label-cols-md="3">
              <b-form-input v-model="form2.reason" />
            </b-form-group>
          </b-col>
          <b-col cols="12">
            <b-form-group
              label="註記代碼"
              label-for="statusCode"
              label-cols-md="3"
            >
              <v-select
                id="statusCode"
                v-model="form2.statusCode"
                label="txt"
                :reduce="dt => dt.id"
                :options="statusCodes"
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
              :disabled="!canAudit"
              @click="audit"
            >
              註記
            </b-button>
          </b-col>
        </b-row>
      </b-form>
      <br />
      <b-table striped hover :fields="columns" :items="rows" show-empty>
        <template #cell(include)="data">
          <b-form-checkbox
            v-model="data.item.include"
            :disabled="!canInclude(data.item)"
          />
        </template>
      </b-table>
    </b-card>
  </div>
</template>
<style lang="scss">
@import '@core/scss/vue/libs/vue-select.scss';
</style>
<script>
import Vue from 'vue';
import vSelect from 'vue-select';
import DatePicker from 'vue2-datepicker';
import 'vue2-datepicker/index.css';
import 'vue2-datepicker/locale/zh-tw';
const Ripple = require('vue-ripple-directive');
import { mapState, mapGetters } from 'vuex';
import moment from 'moment';
import axios from 'axios';

export default Vue.extend({
  components: {
    vSelect,
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
        { txt: '秒資料', id: 'second' },
      ],
      form: {
        monitorTypes: [],
        dataType: 'hour',
        range,
      },
      form2: {
        statusCode: '0',
        reason: '',
      },
      statusCodes: [
        {
          id: '0',
          txt: '復原註記',
        },
        {
          id: 'm',
          txt: '人工註記:有效資料',
        },
        {
          id: 'M',
          txt: '人工註記:無效資料',
        },
      ],
      display: false,
      columns: [],
      rows: [],
    };
  },
  computed: {
    ...mapState('monitorTypes', ['monitorTypes', 'mtMap']),
    ...mapGetters('monitorTypes', ['mtMap']),
    canAudit() {
      let auditCount = 0;
      for (const item of this.rows) {
        if (item.include) {
          auditCount++;
        }
      }
      if (auditCount === 0) return false;

      if (this.form2.reason === '' && this.form2.statusCode !== '0') {
        return false;
      }

      return true;
    },
  },
  mounted() {
    if (this.monitorTypes.length !== 0) {
      // eslint-disable-next-line no-underscore-dangle
      this.form.monitorTypes.push(this.monitorTypes[0]._id);
    }
  },
  methods: {
    async query() {
      this.display = true;
      this.rows = [];
      this.columns = this.getColumns();
      const url = `/HistoryReport/${this.form.monitorTypes.join(':')}/${
        this.form.dataType
      }/${this.form.range[0]}/${this.form.range[1]}`;
      const ret = await axios.get(url);
      this.rows = ret.data.rows;
    },
    cellDataTd(i) {
      return (_value, _key, item) => item.cellData[i].cellClassName;
    },
    dateFormatter(value) {
      return moment(value).format('lll');
    },
    getColumns() {
      const ret = [
        {
          key: 'include',
          label: '',
        },
        {
          key: 'date',
          label: '時間',
          formatter: this.dateFormatter,
        },
      ];
      for (let i = 0; i < this.form.monitorTypes.length; i += 1) {
        const mtCase = this.mtMap.get(this.form.monitorTypes[i]);
        ret.push({
          key: `cellData[${i}].v`,
          label: `${mtCase.desp}(${mtCase.unit})`,
          tdClass: this.cellDataTd(i),
        });
      }
      return ret;
    },
    audit() {
      // case class ManualAuditParam(reason: String, updateList: Seq[UpdateRecordParam])
      // case class UpdateRecordParam(time: Long, mt:String, status: String)
      const updateList = [];
      for (const item of this.rows) {
        if (item.include) {
          for (let i = 0; i < item.cellData.length; i++) {
            const cellData = item.cellData[i];
            if (cellData.v !== '-') {
              const status = this.form2.statusCode + cellData.status.substr(1);
              updateList.push({
                time: item.date,
                mt: this.form.monitorTypes[i],
                status,
              });
            }
          }
        }
      }
      const param = {
        reason: this.form2.reason,
        updateList,
      };
      axios.put(`/Record/${this.form.dataType}`, param).then(res => {
        const ret = res.data;
        if (ret.ok) {
          this.$bvModal.msgBoxOk('成功');
          this.query();
        }
      });
    },
    canInclude(item) {
      for (const cellData of item.cellData) {
        if (cellData.v !== '-') return true;
      }

      return false;
    },
  },
});
</script>

<style></style>
