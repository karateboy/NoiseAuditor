<template>
  <div class="auth-wrapper auth-v2">
    <b-row class="auth-inner m-0">
      <!-- Brand logo-->
      <b-link class="brand-logo">
        <vuexy-logo />
        <h2 class="brand-text text-primary ml-1">Vuexy</h2>
      </b-link>
      <!-- /Brand logo-->

      <!-- Left Text-->
      <b-col lg="8" class="d-none d-lg-flex align-items-center p-5">
        <div
          class="w-100 d-lg-flex align-items-center justify-content-center px-5"
        >
          <b-img fluid :src="imgUrl" alt="Login V2" />
        </div>
      </b-col>
      <!-- /Left Text-->

      <!-- Login-->
      <b-col lg="4" class="d-flex align-items-center auth-bg px-2 p-lg-5">
        <b-col sm="8" md="6" lg="12" class="px-xl-2 mx-auto">
          <b-card-title title-tag="h2" class="font-weight-bold mb-1">
            歡迎使用航空噪音監測資料判讀系統👋
          </b-card-title>
          <b-card-text class="mb-2"> 請登入您的帳號 </b-card-text>

          <!-- form -->
          <validation-observer ref="loginValidation">
            <b-form class="auth-login-form mt-2" @submit.prevent>
              <!-- email -->
              <b-form-group label="Email" label-for="login-email">
                <validation-provider
                  v-slot="{ errors }"
                  name="Email"
                  rules="required|email"
                >
                  <b-form-input
                    id="login-email"
                    v-model="userEmail"
                    :state="errors.length > 0 ? false : null"
                    name="login-email"
                    placeholder="john@example.com"
                  />
                  <small class="text-danger">{{ errors[0] }}</small>
                </validation-provider>
              </b-form-group>

              <!-- forgot password -->
              <b-form-group>
                <div class="d-flex justify-content-between">
                  <label for="login-password">密碼</label>
                  <b-link :to="{ name: 'auth-forgot-password-v2' }">
                    <small>忘記密碼?</small>
                  </b-link>
                </div>
                <validation-provider
                  v-slot="{ errors }"
                  name="Password"
                  rules="required"
                >
                  <b-input-group
                    class="input-group-merge"
                    :class="errors.length > 0 ? 'is-invalid' : null"
                  >
                    <b-form-input
                      id="login-password"
                      v-model="password"
                      :state="errors.length > 0 ? false : null"
                      class="form-control-merge"
                      :type="passwordFieldType"
                      name="login-password"
                      placeholder="············"
                    />
                    <b-input-group-append is-text>
                      <feather-icon
                        class="cursor-pointer"
                        :icon="passwordToggleIcon"
                        @click="togglePasswordVisibility"
                      />
                    </b-input-group-append>
                  </b-input-group>
                  <small class="text-danger">{{ errors[0] }}</small>
                </validation-provider>
              </b-form-group>

              <!-- submit buttons -->
              <b-button
                type="submit"
                variant="primary"
                block
                @click="validationForm"
              >
                登入
              </b-button>
            </b-form>
          </validation-observer>
        </b-col>
      </b-col>
      <!-- /Login-->
    </b-row>
  </div>
</template>

<script>
/* eslint-disable global-require */
import { ValidationProvider, ValidationObserver } from 'vee-validate';
import VuexyLogo from '@core/layouts/components/Logo.vue';
import jscookie from 'js-cookie';

import { required, email } from '@validations';
import { togglePasswordVisibility } from '@core/mixins/ui/forms';
import axios from 'axios';
import store from '@/store/index';
import { mapMutations, mapActions } from 'vuex';
import ToastificationContent from '@core/components/toastification/ToastificationContent.vue';

export default {
  components: {
    VuexyLogo,
    ValidationProvider,
    ValidationObserver,
  },
  mixins: [togglePasswordVisibility],
  data() {
    return {
      status: '',
      password: 'abc123',
      userEmail: 'sales@wecc.com.tw',
      sideImg: require('@/assets/images/pages/login-v2.svg'),
      // validation rulesimport store from '@/store/index'
      required,
      email,
    };
  },
  computed: {
    passwordToggleIcon() {
      return this.passwordFieldType === 'password' ? 'EyeIcon' : 'EyeOffIcon';
    },
    imgUrl() {
      if (store.state.appConfig.layout.skin === 'dark') {
        // eslint-disable-next-line vue/no-side-effects-in-computed-properties
        this.sideImg = require('@/assets/images/pages/login-v2-dark.svg');
        return this.sideImg;
      }
      return this.sideImg;
    },
  },
  mounted() {
    this.validationForm();
  },
  methods: {
    ...mapMutations(['setLogin']),
    ...mapMutations('user', ['setUserInfo']),
    validationForm() {
      this.$refs.loginValidation.validate().then(success => {
        if (success) {
          const cred = { user: this.userEmail, password: this.password };
          axios
            .post('/login', cred)
            .then(res => {
              const ret = res.data;
              if (ret.ok) {
                const userData = ret.userData;
                const userInfo = userData.user;
                this.setUserInfo(userInfo);
                this.setLogin(true);
                if (userInfo.isAdmin) {
                  this.$ability.update([
                    {
                      action: 'manage',
                      subject: 'all',
                    },
                  ]);
                } else {
                  this.$ability.update(userData.group.abilities);
                }
                this.$router.push('/');
              } else {
                this.$toast({
                  component: ToastificationContent,
                  props: {
                    title: '帳號或密碼錯誤',
                    icon: 'UserIcon',
                  },
                });
              }
            })
            .catch(err => {
              this.$toast({
                component: ToastificationContent,
                props: {
                  title: '帳號或密碼錯誤',
                  icon: 'UserIcon',
                },
              });
            });
        }
      });
    },
  },
};
</script>

<style lang="scss">
@import '@core/scss/vue/pages/page-auth.scss';
</style>
