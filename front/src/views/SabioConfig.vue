<template>
  <b-form @submit.prevent @change="onChange">
    <b-row>
      <b-col cols="12">
        <validation-provider v-slot="{ errors }" name="Topic" rules="required">
          <b-form-group label="位址" label-for="topic" label-cols-md="3">
            <b-form-input v-model="paramObj.address"></b-form-input>
            <small class="text-danger">{{ errors[0] }}</small>
          </b-form-group>
        </validation-provider>
      </b-col>
    </b-row>
  </b-form>
</template>
<script>
import Vue from 'vue';

export default Vue.extend({
  props: {
    paramStr: {
      type: String,
      default: ``,
    },
  },
  data() {
    let paramObj = {
      address: '',
    };

    if (this.paramStr !== '{}') paramObj = JSON.parse(this.paramStr);

    return {
      paramObj,
    };
  },
  methods: {
    justify() {},
    onChange(evt) {
      this.justify();
      this.$emit('param-changed', JSON.stringify(this.paramObj));
    },
  },
});
</script>
