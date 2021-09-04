<template>
  <div>
    <b-form @submit.prevent>
      <b-form-group label="帳號:" label-for="account" label-cols="3">
        <b-input
          id="account"
          v-model="user._id"
          :state="Boolean(user._id)"
          aria-describedby="account-feedback"
          :readonly="!isNew"
        ></b-input>
        <b-form-invalid-feedback>帳號不能是空的</b-form-invalid-feedback>
      </b-form-group>
      <b-form-group
        label="顯示名稱:"
        label-for="name"
        :state="Boolean(user.name)"
        label-cols="3"
      >
        <b-input
          id="name"
          v-model="user.name"
          :state="Boolean(user.name)"
          aria-describedby="displayName-feedback"
        ></b-input>
        <b-form-invalid-feedback>顯示名稱不能是空的</b-form-invalid-feedback>
      </b-form-group>
      <b-form-group
        :label="passwordLabel"
        label-for="password"
        :state="isPasswordValid"
        label-cols="3"
      >
        <b-input
          id="password"
          v-model="user.password"
          type="password"
          :state="isPasswordValid"
          aria-describedby="password-feedback"
        ></b-input>
        <b-form-invalid-feedback id="password-feedback">{{
          passwordInvalidReason
        }}</b-form-invalid-feedback>
      </b-form-group>
      <b-form-group label="重新輸入密碼:" label-for="password2" label-cols="3">
        <b-input
          id="password2"
          v-model="user.confirmPassword"
          type="password"
          :state="isPasswordValid"
          aria-describedby="password-feedback"
        ></b-input>
      </b-form-group>
      <b-form-group label="管理者:" label-for="admin" label-cols="3">
        <b-form-checkbox id="admin" v-model="user.isAdmin"></b-form-checkbox>
      </b-form-group>
      <b-form-group label="群組:" label-for="group" label-cols="3">
        <v-select
          id="group"
          v-model="user.group"
          label="name"
          :reduce="g => g._id"
          :options="groupList"
        />
      </b-form-group>
      <b-form-group label="關注測項:" label-for="monitorTypes" label-cols="3">
        <b-form-checkbox-group
          id="monitorTypes"
          v-model="user.monitorTypeOfInterest"
          :options="monitorTypeOptions"
        >
        </b-form-checkbox-group>
      </b-form-group>
      <b-row>
        <b-col offset-md="3">
          <b-button
            v-ripple.400="'rgba(255, 255, 255, 0.15)'"
            type="submit"
            variant="primary"
            class="mr-1"
            :disabled="!canUpsert"
            @click="upsert"
          >
            {{ btnTitle }}
          </b-button>
          <b-button
            v-ripple.400="'rgba(186, 191, 199, 0.15)'"
            variant="outline-secondary"
            @click="reset"
          >
            取消
          </b-button>
        </b-col>
      </b-row>
    </b-form>
  </div>
</template>

<script>
import Vue from 'vue';
import { mapState, mapActions } from 'vuex';
import axios from 'axios';
const Ripple = require('vue-ripple-directive');
import ToastificationContent from '@core/components/toastification/ToastificationContent.vue';
const emptyPassword = '';
export default Vue.extend({
  directives: {
    Ripple,
  },

  props: {
    isNew: Boolean,
    currentUser: {
      type: Object,
      default: () => {},
    },
  },
  data() {
    const user = {
      _id: '',
      name: '',
      password: '',
      confirmPassword: '',
      isAdmin: false,
    };

    this.copyUser(user);
    const groupList = [];
    return {
      user,
      groupList,
    };
  },
  computed: {
    ...mapState('monitorTypes', ['monitorTypes']),
    ...mapState('user', ['userInfo']),
    passwordLabel() {
      if (this.isNew) return '密碼:';
      return '變更密碼:';
    },
    isPasswordValid() {
      if (!this.isNew) return true;

      if (this.user.password === this.user.confirmPassword) return true;
      return false;
    },
    passwordInvalidReason() {
      if (this.user.password !== this.user.confirmPassword) {
        return '密碼和重新輸入必須一致';
      }
      return '';
    },
    canUpsert() {
      return this.isPasswordValid;
    },
    btnTitle() {
      if (this.isNew) return '新增';
      return '更新';
    },
    isMyself() {
      if (this.isNew) return false;

      if (this.user._id === this.userInfo._id) return true;
      return false;
    },
    isAdmin() {
      return this.userinfo.isAdmin;
    },
    monitorTypeOptions() {
      let ret = [];
      for (const mt of this.monitorTypes)
        ret.push({ text: mt.desp, value: mt._id });
      return ret;
    },
  },
  async mounted() {
    await this.fetchMonitorTypes();
    await this.fetchMonitors();
    this.getGroupList();
  },
  methods: {
    ...mapActions('monitorTypes', ['fetchMonitorTypes']),
    ...mapActions('monitors', ['fetchMonitors']),
    getGroupList() {
      axios
        .get('/Groups')
        .then(res => {
          this.groupList = res.data;
        })
        .catch(err => {
          throw new Error(err);
        });
    },
    copyUser(user) {
      if (!this.isNew) {
        const self = this.currentUser;
        user._id = self._id;
        user.name = self.name;
        user.isAdmin = self.isAdmin;
        user.group = self.group;
        user.monitorTypeOfInterest = self.monitorTypeOfInterest;
      }
    },
    reset() {
      this.copyUser(this.user);
    },

    upsert() {
      if (this.isNew) {
        axios.post(`/User`, this.user).then(res => {
          if (res.status === 200) {
            this.$bvModal.msgBoxOk('成功', { headerBgVariant: 'primary' });
          } else {
            this.$bvModal.msgBoxOk('失敗', { headerBgVariant: 'danger' });
          }
          this.$emit('created');
        });
      } else {
        if (this.user.Password === emptyPassword) {
          this.user.Password = '';
          this.user.ConfirmPassword = '';
        }

        axios.put(`/User/${this.currentUser.Id}`, this.user).then(res => {
          if (res.status === 200) {
            this.$toast({
              component: ToastificationContent,
              props: {
                title: '成功',
                icon: 'UserIcon',
              },
            });
          } else {
            this.$bvModal.msgBoxOk('失敗', { headerBgVariant: 'danger' });
          }
          this.$emit('updated');
        });
      }
    },
  },
});
</script>
