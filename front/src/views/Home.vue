<template>
  <b-row class="match-height">
    <b-col lg="9" md="12">
      <b-card title="即時監測資訊">
        <div id="realtimeChart"></div>
      </b-card>
      <b-row>
        <b-col v-for="mt in userInfo.monitorTypeOfInterest" :key="mt" cols="3">
          <b-card>
            <div :id="`history_${mt}`"></div>
          </b-card>
        </b-col>
      </b-row>
    </b-col>
    <b-col lg="3" class="text-center">
      <b-card>
        <b-table :fields="fields" :items="realTimeStatus" small> </b-table>
      </b-card>
    </b-col>
  </b-row>
</template>
<style scoped></style>
<script lang="ts">
import Vue from 'vue';
import { mapActions, mapGetters, mapState } from 'vuex';
import axios from 'axios';
import { MonitorTypeStatus } from './types';
import highcharts from 'highcharts';
import darkTheme from 'highcharts/themes/dark-unica';
import useAppConfig from '../@core/app-config/useAppConfig';

export default Vue.extend({
  data() {
    const fields = [
      {
        key: 'desp',
        label: '測項',
        sortable: true,
      },
      {
        key: 'value',
        label: '測值',
        formatter: (value: string, key: string, item: MonitorTypeStatus) => {
          const v = parseFloat(item.value);
          if (isNaN(v)) return `-`;
          else return `${item.value}`;
        },
        tdClass: (value: string, key: string, item: MonitorTypeStatus) => {
          return item.classStr;
        },
        sortable: true,
      },
      {
        key: 'unit',
        label: '單位',
      },
      {
        key: 'status',
        label: '狀態',
      },
    ];
    let chart: any;
    chart = null;
    return {
      maxPoints: 30,
      fields,
      refreshTimer: 0,
      mtInterestTimer: 0,
      realTimeStatus: Array<MonitorTypeStatus>(),
      chartSeries: Array<highcharts.SeriesOptionsType>(),
      chart,
    };
  },
  computed: {
    ...mapState('user', ['userInfo']),
    ...mapGetters('monitorTypes', ['mtMap']),
    skin() {
      const { skin } = useAppConfig();
      return skin;
    },
  },
  async mounted() {
    const { skin } = useAppConfig();
    if (skin.value == 'dark') {
      darkTheme(highcharts);
    }

    await this.fetchMonitorTypes();
    await this.getUserInfo();
    const me = this;
    for (const mt of this.userInfo.monitorTypeOfInterest) this.query(mt);
    this.mtInterestTimer = setInterval(() => {
      for (const mt of me.userInfo.monitorTypeOfInterest) me.query(mt);
    }, 60000);

    await this.initRealtimeChart();
  },
  beforeDestroy() {
    clearInterval(this.refreshTimer);
    clearInterval(this.mtInterestTimer);
  },
  methods: {
    ...mapActions('monitorTypes', ['fetchMonitorTypes']),
    ...mapActions('monitors', ['fetchMonitors']),
    ...mapActions('user', ['getUserInfo']),
    async refresh(): Promise<void> {
      this.plotLatestData();
    },
    async plotLatestData(): Promise<void> {
      await this.getRealtimeStatus();
      const now = new Date().getTime();

      let chart = this.chart as highcharts.Chart;
      for (const mtStatus of this.realTimeStatus) {
        const series = chart.series.find(s => {
          return s.name === mtStatus.desp;
        });

        if (series) {
          let value = parseFloat(mtStatus.value);
          if (!isNaN(value)) {
            series.addPoint([now, value], false, false, true);
            while (series.data.length >= this.maxPoints) {
              series.removePoint(0, false);
            }
          }
        }
      }

      chart.redraw();
    },
    async getRealtimeStatus(): Promise<void> {
      const ret = await axios.get('/MonitorTypeStatusList');
      this.realTimeStatus = ret.data;
    },
    async initRealtimeChart(): Promise<boolean> {
      await this.getRealtimeStatus();

      for (const mtStatus of this.realTimeStatus) {
        let data = Array<{ x: number; y: number }>();
        const wind = ['WD_SPEED', 'WD_DIR'];
        const selectedMt = ['PM25'];
        const visible = selectedMt.indexOf(mtStatus._id) !== -1;
        if (wind.indexOf(mtStatus._id) === -1) {
          let series: highcharts.SeriesSplineOptions = {
            id: mtStatus._id,
            name: mtStatus.desp,
            type: 'spline',
            data: data,
            tooltip: {
              valueDecimals: this.mtMap.get(mtStatus._id).prec,
            },
            visible,
          };
          this.chartSeries.push(series);
        } else {
          let series: highcharts.SeriesScatterOptions = {
            name: mtStatus.desp,
            type: 'scatter',
            data,
            tooltip: {
              valueDecimals: this.mtMap.get(mtStatus._id).prec,
            },
            visible,
          };
          this.chartSeries.push(series);
        }
      }

      const me = this;

      return new Promise(function (resolve, reject) {
        const chartOption: highcharts.Options = {
          chart: {
            type: 'spline',
            marginRight: 10,
            height: 300,
            events: {
              load: () => {
                me.refreshTimer = setInterval(() => {
                  me.refresh();
                }, 3000);
                resolve(true);
              },
            },
          },
          navigation: {
            buttonOptions: {
              enabled: true,
            },
          },
          credits: {
            enabled: false,
          },

          title: {
            text: '',
          },
          xAxis: {
            type: 'datetime',
            tickPixelInterval: 150,
          },
          yAxis: {
            title: {
              text: 'value',
            },
            plotLines: [
              {
                value: 0,
                width: 1,
                color: '#808080',
              },
            ],
          },
          time: {
            timezoneOffset: -480,
          },
          exporting: {
            enabled: false,
          },
          series: me.chartSeries,
        };
        me.chart = highcharts.chart('realtimeChart', chartOption);
      });
    },
    async query(mt: string) {
      const now = new Date().getTime();
      const oneHourBefore = now - 60 * 60 * 1000;
      const monitors = 'me';
      const url = `/HistoryTrend/${monitors}/${mt}/Min/all/${oneHourBefore}/${now}`;
      const res = await axios.get(url);
      const ret: highcharts.Options = res.data;

      ret.chart = {
        type: 'spline',
        zoomType: 'x',
        panning: {
          enabled: true,
        },
        panKey: 'shift',
        alignTicks: false,
      };

      ret.title!.text = '分鐘趨勢圖';

      ret.colors = [
        '#7CB5EC',
        '#434348',
        '#90ED7D',
        '#F7A35C',
        '#8085E9',
        '#F15C80',
        '#E4D354',
        '#2B908F',
        '#FB9FA8',
        '#91E8E1',
        '#7CB5EC',
        '#80C535',
        '#969696',
      ];

      ret.tooltip = { valueDecimals: 2 };
      ret.legend = { enabled: true };
      ret.credits = {
        enabled: false,
        href: 'http://www.wecc.com.tw/',
      };

      let xAxis: highcharts.XAxisOptions = ret.xAxis as highcharts.XAxisOptions;
      xAxis.type = 'datetime';

      xAxis!.dateTimeLabelFormats = {
        day: '%b%e日',
        week: '%b%e日',
        month: '%y年%b',
      };

      ret.plotOptions = {
        spline: {
          tooltip: {
            valueDecimals: this.mtMap.get(mt).prec,
          },
        },
        scatter: {
          tooltip: {
            valueDecimals: this.mtMap.get(mt).prec,
          },
        },
      };
      ret.time = {
        timezoneOffset: -480,
      };
      highcharts.chart(`history_${mt}`, ret);
    },
  },
});
</script>
