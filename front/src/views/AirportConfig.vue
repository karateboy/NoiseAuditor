<template>
  <div>
    <b-card title="機場清單設定">
      <b-form @submit.prevent>
        <b-row>
          <b-col cols="12">
            <b-form-group
              label="機場名稱:"
              label-for="airport"
              label-cols-md="3"
            >
              <v-select
                id="airport"
                v-model="form._id.airportID"
                label="name"
                :reduce="airport => airport._id"
                :options="airportList"
              />
            </b-form-group>
          </b-col>
          <b-col cols="12">
            <b-form-group label="年度:" label-for="year" label-cols-md="3">
              <b-form-spinbutton
                v-model.number="form._id.year"
                :max="yearMax"
                :min="yearMax - 3"
              />
            </b-form-group>
          </b-col>
          <b-col cols="12">
            <b-form-group label="季度:" label-for="quarter" label-cols-md="3">
              <b-form-spinbutton
                v-model.number="form._id.quarter"
                max="4"
                min="1"
              />
            </b-form-group>
          </b-col>
        </b-row>
        <b-row>
          <b-col offset-md="3">
            <b-button
              v-b-tooltip.hover
              title="載入清單或最近一季清單"
              type="submit"
              variant="gradient-primary"
              class="mr-1"
              :disabled="!canEdit"
              @click="edit"
            >
              載入編輯
            </b-button>
          </b-col>
        </b-row>
      </b-form>
    </b-card>
    <b-card v-if="startEdit" :title="airportName">
      <b-table :items="form.terminals" :fields="fields">
        <template #thead-top>
          <b-tr>
            <b-td>
              <b-button
                variant="gradient-primary"
                class="mr-1"
                @click="newTerminal"
              >
                新增測站
              </b-button>
              <b-button
                variant="gradient-success"
                class="mr-1"
                :disabled="!canSave"
                @click="save"
              >
                儲存清單
              </b-button>
            </b-td>
          </b-tr>
        </template>
        <template #cell(no)="row">
          <b-form-input v-model.number="row.item.no" />
        </template>
        <template #cell(name)="row">
          <b-form-input
            v-model="row.item.name"
            :state="Boolean(row.item.name)"
          />
        </template>
        <template #cell(operation)="row">
          <b-button variant="gradient-danger" @click="deleteTerminal(row)"
            >刪除</b-button
          >
        </template>
        <template #custom-foot>
          <b-tr>
            <b-td>
              <b-button
                variant="gradient-primary"
                class="mr-1"
                @click="newTerminal"
              >
                新增測站
              </b-button>
              <b-button
                variant="gradient-success"
                class="mr-1"
                :disabled="!canSave"
                @click="save"
              >
                儲存清單
              </b-button>
            </b-td>
          </b-tr>
        </template>
      </b-table>
    </b-card>
  </div>
</template>
<script lang="ts">
import Vue from 'vue';
import axios from 'axios';
import moment from 'moment';
import Swal from 'sweetalert2';
import { Airport, Terminal, AirportInfo } from './types';

export default Vue.extend({
  data() {
    let now = moment();
    let form: AirportInfo = {
      _id: {
        year: now.year() - 1911,
        quarter: now.month() / 4 + 1,
        airportID: 0,
      },
      terminals: Array<Terminal>(),
    };
    let fields = [
      {
        key: 'no',
        label: '測站編號',
        sortable: true,
      },
      {
        key: 'name',
        label: '測站名稱',
        sortable: true,
      },
      {
        key: 'operation',
        label: '',
      },
    ];
    return {
      airportList: Array<Airport>(),
      form,
      fields,
      startEdit: false,
    };
  },
  computed: {
    yearMax(): number {
      return moment().year() - 1911;
    },
    airportName(): string {
      if (this.form._id.airportID === 0) return '';
      else {
        let airport = this.airportList.find(
          p => p._id == this.form._id.airportID,
        );
        if (airport !== undefined) return airport?.name;
        else return '';
      }
    },
    canEdit(): boolean {
      return this.form._id.airportID !== 0;
    },
    canSave(): boolean {
      return this.form.terminals.length !== 0;
    },
  },
  async mounted() {
    this.getAirportList();
  },
  methods: {
    async getAirportList() {
      try {
        const res = await axios.get('/Airports');
        if (res.status === 200) {
          this.airportList = res.data;
        }
      } catch (ex) {
        throw new Error(ex);
      }
    },
    async edit() {
      try {
        const res = await axios.get(
          `/LatestAirportInfo/${this.form._id.airportID}/${this.form._id.year}/${this.form._id.quarter}`,
        );
        if (res.status === 200) {
          const ret = res.data;
          if (ret.result) {
            let airportInfo: AirportInfo = ret.result;
            this.form.terminals = airportInfo.terminals;
            if (ret.ok)
              Swal.fire({
                title: '成功',
                text: `成功載入${this.airportName}${airportInfo._id.year}年${airportInfo._id.quarter}季清單`,
                icon: 'success',
                confirmButtonText: '確定',
              });
            else {
              Swal.fire({
                title: '注意',
                text: `載入最近${this.airportName}${airportInfo._id.year}年${airportInfo._id.quarter}季清單`,
                icon: 'info',
                confirmButtonText: '確定',
              });
            }
          } else {
            this.form.terminals.splice(0, this.form.terminals.length);
            Swal.fire({
              title: '注意',
              text: `資料庫沒有機場清單紀錄, 請重新編輯.`,
              icon: 'info',
              confirmButtonText: '確定',
            });
          }
        }
      } catch (err) {
        throw new Error(err);
      } finally {
        this.startEdit = true;
      }
    },
    newTerminal() {
      let num = this.form.terminals.length + 1;
      this.form.terminals.push({ no: num, name: '' });
    },
    deleteTerminal(row: any) {
      this.form.terminals.splice(row.index, 1);
    },
    async save() {
      for (let terminal of this.form.terminals) {
        if (terminal.name === '') {
          Swal.fire({
            title: '錯誤',
            text: '測站名稱不能是空的!',
            icon: 'error',
            confirmButtonText: '繼續',
          });
          return;
        }
      }
      try {
        const res = await axios.post('/AirportInfo', this.form);
        if (res.status === 200)
          Swal.fire({
            title: '成功',
            text: `${this.airportName}${this.form._id.year}年${this.form._id.quarter}季清單成功儲存`,
            icon: 'success',
            confirmButtonText: '確定',
          });
      } catch (ex) {
        throw new Error(ex);
      }
    },
  },
});
</script>
