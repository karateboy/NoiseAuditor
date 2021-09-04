<template>
  <b-card title="即時資訊">
    <b-table striped hover :fields="fields" :items="items" />
  </b-card>
</template>

<script>
import axios from 'axios';

export default {
  data() {
    return {
      fields: [
        {
          key: 'desp',
          label: '測項',
          sortable: true,
        },
        {
          key: 'value',
          label: '測值',
          sortable: true,
        },
        {
          key: 'unit',
          label: '單位',
          sortable: true,
        },
        {
          key: 'instrument',
          label: '設備',
          sortable: true,
        },
        {
          key: 'status',
          label: '狀態',
          sortable: true,
        },
      ],
      items: [],
      timer: 0,
    };
  },
  mounted() {
    this.getRealtimeData();
    this.timer = setInterval(this.getRealtimeData, 3000);
  },
  beforeDestroy() {
    clearInterval(this.timer);
  },
  methods: {
    async getRealtimeData() {
      const ret = await axios.get('/MonitorTypeStatusList');
      this.items.splice(0, this.items.length);
      this.items = ret.data;
    },
  },
};
</script>

<style></style>
