<template>
  <div>
    <b-card title="檔案上傳">
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
        <b-row v-if="form._id.airportID !== 0 && !airportInfoReady">
          <b-col offset-md="3">
            <h3 class="text-danger">尚未設定此季機場清單</h3>
            <b-button
              type="submit"
              variant="gradient-primary"
              class="mr-1"
              @click="goAirportInfoConfig"
            >
              前往設定
            </b-button></b-col
          >
        </b-row>
        <b-row v-else-if="form._id.airportID !== 0 && airportInfoReady">
          <b-col offset-md="3">
            <b-form-file
              v-model="uploadFile"
              :state="Boolean(uploadFile)"
              accept=".zip"
              browse-text="..."
              placeholder="選擇上傳檔案..."
              drop-placeholder="拖曳檔案至此..."
            ></b-form-file>
          </b-col>
          <b-col offset-md="3">
            <b-button
              variant="gradient-primary"
              type="submit"
              class="mr-1"
              :disabled="!Boolean(uploadFile)"
              @click="upload"
            >
              上傳
            </b-button>
          </b-col>
        </b-row>
      </b-form>
    </b-card>
  </div>
</template>
<script lang="ts">
import Vue from 'vue';
import axios from 'axios';
import moment from 'moment';
import Swal from 'sweetalert2';
import { Airport, Terminal, AirportInfo } from './types';
import { mapMutations } from 'vuex';
import { ReportID } from '@/store/types';

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

    let uploadFile: File | undefined;
    return {
      airportList: Array<Airport>(),
      form,
      airportInfoReady: false,
      uploadFile,
      actorName: '',
      version: 0,
      timer: 0,
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
    canUpload(): boolean {
      if (this.form._id.airportID === 0) return false;

      return this.airportInfoReady;
    },
  },
  watch: {
    'form._id.airportID': function () {
      this.checkAirportInfo();
    },
    'form._id.year': function () {
      this.checkAirportInfo();
    },
    'form._id.quarter': function () {
      this.checkAirportInfo();
    },
  },
  async mounted() {
    this.getAirportList();
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
    async checkAirportInfo() {
      try {
        const res = await axios.get(
          `/LatestAirportInfo/${this.form._id.airportID}/${this.form._id.year}/${this.form._id.quarter}`,
        );
        if (res.status === 200) {
          const ret = res.data;
          if (ret.result) {
            let airportInfo: AirportInfo = ret.result;
            this.form.terminals = airportInfo.terminals;
            if (ret.ok) this.airportInfoReady = true;
            else this.airportInfoReady = false;
          } else this.airportInfoReady = false;
        }
      } catch (err) {
        throw new Error(err);
      }
    },
    goAirportInfoConfig() {
      this.$router.push({ name: 'airport-config' });
    },
    async upload() {
      var formData = new FormData();

      formData.append('data', this.uploadFile as File);
      this.setLoading({ loading: true, message: '資料上傳中' });
      try {
        const res = await axios.post(
          `/ReportData/${this.form._id.airportID}/${this.form._id.year}/${this.form._id.quarter}`,
          formData,
          {
            headers: {
              'Content-Type': 'multipart/form-data',
            },
          },
        );
        if (res.status === 200) {
          this.actorName = res.data.actorName;
          this.version = res.data.version;
          this.setLoading({ loading: true, message: '檔案解壓縮中' });
          this.timer = setTimeout(this.checkFinished, 1000);
        } else {
          this.setLoading({ loading: false });
          Swal.fire({
            title: '錯誤',
            text: `上傳失敗${res.status} - ${res.statusText}`,
            icon: 'error',
            confirmButtonText: '確定',
          });
        }
      } catch (err) {
        throw new Error(err);
      }
    },
    async checkFinished() {
      const res = await axios.get(`/UploadProgress/${this.actorName}`);
      if (res.data.finished) {
        this.setLoading({ loading: false });
        Swal.fire({
          title: '成功',
          text: `成功上傳${this.airportName}${this.form._id.year}年${this.form._id.quarter}監測資料`,
          icon: 'success',
          confirmButtonText: '確定',
        });
        const reportID: ReportID = {
          airpotInfoID: {
            year: this.form._id.year,
            quarter: this.form._id.quarter,
            airportID: this.form._id.airportID,
          },
          version: this.version,
        };
        this.setActiveReportIDs([reportID]);
        this.$router.push({ name: 'import-progress' });
      } else {
        this.timer = setTimeout(this.checkFinished, 1000);
      }
    },
    beforeDestroy() {
      clearTimeout(this.timer);
    },
  },
});
</script>
