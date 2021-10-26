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
            <b-button
              class="mr-2"
              variant="outline-primary"
              @click="downloadImportErrorLog"
              >下載資料格式錯誤表</b-button
            >
            <b-button
              class="mr-2"
              variant="outline-primary"
              @click="downloadReport"
              >下載稽核報表</b-button
            >
            <b-button
              class="mr-2"
              variant="outline-info"
              @click="setReportTolerance"
              >設定誤差範圍</b-button
            >
            <b-button
              class="mr-2"
              variant="outline-danger"
              @click="clearReportData"
              >刪除資料</b-button
            >
          </b-col>
        </b-row>
      </b-form>
      <b-modal
        id="reportToleranceModal"
        :title="reportToleranceModelTitle"
        hide-footer
        size="xl"
        modal-class="modal-primary"
        no-close-on-backdrop
      >
        <report-tolerance-page
          :report-tolerance="reportTolerance"
          @rt-changed="handleRTchanged"
          @rt-end="endModal"
        ></report-tolerance-page>
      </b-modal>
    </b-card>
  </div>
</template>
<script lang="ts">
import Vue from 'vue';
import { mapState } from 'vuex';
import { AirportInfoID, ReportID } from '../store/types';
import { Airport, ReportInfo, ReportTolerance, SubTask } from './types';
import axios from 'axios';
import Swal from 'sweetalert2';
import { mapMutations } from 'vuex';
import ReportTolerancePage from './ReportTolerancePage.vue';

interface ReportDesc {
  _id: ReportID;
  desc: string;
}

export default Vue.extend({
  components: {
    ReportTolerancePage,
  },
  data() {
    let _id: ReportID | undefined;
    let reportInfo: ReportInfo | undefined;
    let reportIdList = Array<ReportID>();
    let reportToleranceModelTitle = '';
    let reportTolerance: ReportTolerance | undefined;
    return {
      form: {
        _id,
      },
      airportList: Array<Airport>(),
      reportInfo,
      reportIdList,
      reportToleranceModelTitle,
      reportTolerance,
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

      return true;
      /*
      if (this.reportInfo.state === '稽核完成') return true;
      else return false;
      */
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
    if (this.reportIdList.length !== 0)
      this.form._id = this.reportIdList[this.reportIdList.length - 1];
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
        throw new Error('getAirportList failed' + ex);
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
        throw new Error('getReportInfo failed' + err);
      }
    },
    async getReportInfoIdList() {
      try {
        const res = await axios.get('/ReportIDs');
        if (res.status === 200) {
          this.reportIdList = res.data;
        }
      } catch (err) {
        throw new Error('getReportInfoIdList failed' + err);
      }
    },
    async reauditReport() {
      try {
        if (this.form._id === undefined) return;

        let reportID = this.form._id;
        let airportInfoID = reportID.airpotInfoID as AirportInfoID;
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
        throw new Error('無法重新稽核' + err);
      }
    },
    async downloadReport() {
      const baseUrl =
        process.env.NODE_ENV === 'development' ? 'http://localhost:9000/' : '';
      const year = this.form._id?.airpotInfoID.year;
      const quarter = this.form._id?.airpotInfoID.quarter;
      const airportID = this.form._id?.airpotInfoID.airportID;
      const version = this.form._id?.version;
      const url = `${baseUrl}AuditReport/${year}/${quarter}/${airportID}/${version}`;
      window.open(url);
    },
    async clearReportData() {
      try {
        if (this.form._id === undefined) return;

        let year = this.form._id.airpotInfoID.year;
        let quarter = this.form._id.airpotInfoID.quarter;
        let airportID = this.form._id.airpotInfoID.airportID;
        let version = this.form._id.version;
        const res = await axios.delete(
          `/ReportInfo/${year}/${quarter}/${airportID}/${version}`,
        );
        if (res.status == 200) {
          let text = `刪除監測資料`;
          Swal.fire({
            title: '成功',
            text,
            icon: 'success',
            confirmButtonText: '確定',
          });
        }
        await this.getReportInfoIdList();
        if (this.reportIdList.length !== 0)
          this.form._id = this.reportIdList[this.reportIdList.length - 1];
      } catch (err) {
        throw new Error('無法清除報表資料' + err);
      }
    },
    setReportTolerance(): void {
      let reportID = this.form._id;
      if (reportID == undefined) return;

      let airportInfoID = reportID.airpotInfoID as AirportInfoID;
      let airportName = '';
      let airport = this.airportList.find(
        p => p._id == airportInfoID.airportID,
      );
      if (airport) airportName = airport.name;

      this.reportToleranceModelTitle = `${airportName}${airportInfoID.year}年${airportInfoID.quarter}第${reportID.version}版`;
      this.reportTolerance = this.reportInfo?.reportTolerance;
      this.$bvModal.show('reportToleranceModal');
    },
    async handleRTchanged(v: ReportTolerance) {
      this.reportTolerance = v;
      try {
        const year = this.form._id?.airpotInfoID.year;
        const quarter = this.form._id?.airpotInfoID.quarter;
        const airportID = this.form._id?.airpotInfoID.airportID;
        const version = this.form._id?.version;
        const url = `/ReportInfo/ReportTolerance/${year}/${quarter}/${airportID}/${version}`;
        const res = await axios.post(url, v);
        if (res.status === 200) {
          let text = `設定報表誤差`;
          Swal.fire({
            title: '成功',
            text,
            icon: 'success',
            confirmButtonText: '確定',
          });
          this.$bvModal.hide('reportToleranceModal');
          await this.getReportInfo();
        }
      } catch (err) {
        throw new Error('failed to handle RT');
      }
    },
    endModal() {
      this.$bvModal.hide('reportToleranceModal');
    },
    downloadImportErrorLog() {
      const baseUrl =
        process.env.NODE_ENV === 'development' ? 'http://localhost:9000/' : '/';
      const year = this.form._id?.airpotInfoID.year;
      const quarter = this.form._id?.airpotInfoID.quarter;
      const airportID = this.form._id?.airpotInfoID.airportID;
      const version = this.form._id?.version;
      const url = `${baseUrl}ImportErrorLog/${year}/${quarter}/${airportID}/${version}`;
      window.open(url);
    },
  },
});
</script>
