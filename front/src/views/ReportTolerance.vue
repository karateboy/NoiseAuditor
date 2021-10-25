<template>
  <b-card title="誤差範圍設定">
    <b-form @submit.prevent>
      <b-row>
        <b-col cols="12">
          <b-form-group label="總計均能音量 Total_Leq:" label-cols-md="3">
            <b-form-input v-model.number="rt.totalLeq" />
          </b-form-group>
        </b-col>
        <b-col cols="12">
          <b-form-group label="事件均能音量 Event_Leq:" label-cols-md="3">
            <b-form-input v-model.number="rt.eventLeq" />
          </b-form-group>
        </b-col>
        <b-col cols="12">
          <b-form-group label="總計均能音量 Total_Leq:" label-cols-md="3">
            <b-form-input v-model.number="rt.totalLeq" />
          </b-form-group>
        </b-col>
        <b-col cols="12">
          <b-form-group label="總計均能音量 Total_Leq:" label-cols-md="3">
            <b-form-input v-model.number="rt.totalLeq" />
          </b-form-group>
        </b-col>
        <b-col cols="12">
          <b-form-group label="總計均能音量 Total_Leq:" label-cols-md="3">
            <b-form-input v-model.number="rt.totalLeq" />
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
</template>
<script lang="ts">
import Vue, { PropType } from 'vue';
import { ReportTolerance } from './types';
export default Vue.extend({
  props: {
    reportTolerance: {
      type: Object as PropType<ReportTolerance>,
      default: () => {
        let ret: ReportTolerance = {
          totalLeq: 1,
          eventLeq: 1,
          backLeq: 1,
          totalLdn: 1,
          eventLdn: 1,
          backLdn: 1,
          totalEventSel: 1,
          numOfEvent: 1,
          duration: 1,
        };
        return ret;
      },
    },
  },
  data() {
    let rt: ReportTolerance = {
      totalLeq: this.reportTolerance.totalLeq,
      eventLeq: this.reportTolerance.eventLeq,
      backLeq: this.reportTolerance.backLeq,
      totalLdn: this.reportTolerance.totalLdn,
      eventLdn: this.reportTolerance.eventLdn,
      backLdn: this.reportTolerance.backLdn,
      totalEventSel: this.reportTolerance.totalEventSel,
      numOfEvent: this.reportTolerance.numOfEvent,
      duration: this.reportTolerance.duration,
    };
    return {
      rt,
    };
  },
});
</script>
