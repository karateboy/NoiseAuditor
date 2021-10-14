<template>
  <div>
    <b-card title="報表列表">
      <b-form @submit.prevent>
        <b-row>
          <b-col cols="12">
            <b-form-group
              label="報表:"
              label-for="reportInfos"
              label-cols-md="3"
            >
              <v-select
                id="reportInfo"
                v-model="form._id"
                label="desc"
                :reduce="report => report._id"
                :options="reportList"
              />
            </b-form-group>
          </b-col>
        </b-row>
      </b-form>
    </b-card>
    <b-card v-if="reportInfo" title="報表進度">
      <b-form @submit.prevent>
        <b-row>
          <b-col cols="12">
            <b-form-group label="處理狀態:" label-for="state" label-cols-md="3">
              <b-input :value="state" readonly />
            </b-form-group>
          </b-col>
          <b-col offset="2" cols="12">
            <b-button
              class="mr-2"
              variant="outline-primary"
              :disabled="!canReaudit"
              @click="reauditReport"
              >重新稽核</b-button
            >
            <b-button class="mr-2" variant="outline-primary"
              >下載資料格式錯誤表</b-button
            >
            <b-button class="mr-2" variant="outline-primary"
              >下載稽核報表</b-button
            >
          </b-col>
        </b-row>
      </b-form>
    </b-card>
  </div>
</template>
<script lang="ts">
import Vue from 'vue';
import { mapState } from 'vuex';
import { AirportInfoID, ReportID } from '../store/types';
import { Airport, ReportInfo, SubTask } from './types';
import axios from 'axios';
import Swal from 'sweetalert2';
import { mapMutations } from 'vuex';

interface ReportDesc {
  _id: ReportID;
  desc: string;
}

export default Vue.extend({
  data() {
    let _id: ReportID | undefined;
    let reportInfo: ReportInfo | undefined;
    let reportIdList = Array<ReportID>();
    return {
      form: {
        _id,
      },
      airportList: Array<Airport>(),
      reportInfo,
      reportIdList,
    };
  },
  computed: {
    ...mapState(['activeReportIDs']),
    state(): string {
      if (this.reportInfo == undefined) return '';
      else {
        return this.reportInfo.state;
      }
    },
    airportMap(): Map<number, Airport> {
      let ret = new Map<number, Airport>();
      for (let airport of this.airportList) {
        ret.set(airport._id, airport);
      }

      return ret;
    },
    reportList(): Array<ReportDesc> {
      let ret = Array<ReportDesc>();

      for (let reportID of this.reportIdList) {
        let airportName = `${reportID.airpotInfoID.airportID}`;
        let airport = this.airportMap.get(reportID.airpotInfoID.airportID);
        if (airport !== undefined) airportName = airport.name;

        ret.push({
          _id: reportID,
          desc: `${airportName}${reportID.airpotInfoID.year}年${reportID.airpotInfoID.quarter}季第${reportID.version}版`,
        });
      }
      return ret;
    },
    reportTasks(): Array<SubTask> {
      if (this.reportInfo) {
        let ret = Array<SubTask>();
        for (let task of this.reportInfo.tasks) {
          ret.push(task);
        }
        return ret;
      } else return Array<SubTask>();
    },
    canReaudit(): boolean {
      if (this.reportInfo == undefined) return false;

      if (this.reportInfo.state === '稽核完成') return true;
      else return false;
    },
  },
  watch: {
    'form._id': function () {
      this.getReportInfo();
    },
  },
  async mounted() {
    await this.getAirportList();
    await this.getReportInfoIdList();
  },
  methods: {
    ...mapMutations(['setLoading', 'setActiveReportIDs']),
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
    async getReportInfo() {
      if (this.form._id === undefined) return;

      let year = this.form._id.airpotInfoID.year;
      let quarter = this.form._id.airpotInfoID.quarter;
      let airportID = this.form._id.airpotInfoID.airportID;
      let version = this.form._id.version;
      try {
        const res = await axios.get(
          `/ReportInfo/${year}/${quarter}/${airportID}/${version}`,
        );
        if (res.status === 200) {
          const ret = res.data as Array<ReportInfo>;
          this.reportInfo = ret[0];
        }
      } catch (err) {
        throw new Error(err);
      }
    },
    async getReportInfoIdList() {
      try {
        const res = await axios.get('/ReportIDs');
        if (res.status === 200) {
          this.reportIdList = res.data;
        }
      } catch (err) {
        throw new Error(err);
      }
    },
    async reauditReport() {
      try {
        if (this.form._id === undefined) return;

        let reportID = this.form._id;
        let airportInfoID = reportID.airpotInfoID as AirportInfoID;
        console.log(airportInfoID);
        const res = await axios.post('/Reaudit', this.form._id);
        if (res.status == 200) {
          let airportName = '';
          let airport = this.airportList.find(
            p => p._id == airportInfoID.airportID,
          );
          if (airport) airportName = airport.name;

          let text = `重新稽核${airportName}${airportInfoID.year}年${airportInfoID.quarter}第${reportID.version}版監測資料`;
          Swal.fire({
            title: '成功',
            text,
            icon: 'success',
            confirmButtonText: '確定',
          });
          this.setActiveReportIDs([reportID]);
          this.$router.push({ name: 'import-progress' });
        }
      } catch (err) {
        throw new Error(err);
      }
    },
  },
});
</script>
