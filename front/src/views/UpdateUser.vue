<template>
  <div class="m-3 p-3 form_class">
    <user :is-new="false" :current-user="userinfo" />
  </div>
</template>
<style scoped>
.form_class {
  background-color: whitesmoke;
}
</style>
<script lang="ts">
import Vue from 'vue';
import { mapState } from 'vuex';
import User from './User.vue';

export default Vue.extend({
  components: {
    User,
  },
  data() {
    return {};
  },
  computed: {
    ...mapState(['userinfo']),
  },
});
</script>
