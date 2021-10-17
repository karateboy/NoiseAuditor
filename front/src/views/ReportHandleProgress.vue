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
    <b-card v-if="reportInfo" title="報表處理進度">
      <b-form @submit.prevent>
        <b-row>
          <b-col cols="12">
            <b-form-group label="處理狀態:" label-for="state" label-cols-md="3">
              <b-input :value="state" readonly />
            </b-form-group>
          </b-col>
          <b-col v-for="task in reportTasks" :key="task.name" cols="12">
            <h5>{{ task.name }}</h5>
            <b-progress :max="task.total" height="2rem" class="m-1">
              <b-progress-bar :value="task.current" variant="success">
                <span>
                  <strong
                    >{{ task.current }} /
                    {{
                      `${task.total}(${(
                        (task.current / task.total) *
                        100
                      ).toFixed(0)}%)`
                    }}</strong
                  ></span
                >
              </b-progress-bar>
            </b-progress>
          </b-col>
        </b-row>
      </b-form>
    </b-card>
  </div>
</template>
<script lang="ts">
import Vue from 'vue';
import { mapState } from 'vuex';
import { ReportID } from '../store/types';
import { Airport, ReportInfo, SubTask } from './types';
import axios from 'axios';

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
      timerID: 0,
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
  },
  async mounted() {
    await this.getAirportList();
    await this.getReportInfoIdList();
    let activeReportIDs = this.activeReportIDs as Array<ReportID>;
    if (activeReportIDs.length !== 0)
      this.form._id = activeReportIDs[activeReportIDs.length - 1];

    this.getReportInfo();
    let me = this;
    this.timerID = setInterval(() => {
      me.getReportInfo();
    }, 3000);
  },
  beforeDestroy() {
    clearInterval(this.timerID);
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
        if (
          this.reportInfo?.tasks.length == 0 &&
          this.reportInfo.state == '稽核完成'
        ) {
          this.$router.push({ name: 'report-management' });
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
  },
});
</script>
