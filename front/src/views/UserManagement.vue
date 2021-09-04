<template>
  <b-card>
    <b-table
      :items="userList"
      :fields="fields"
      select-mode="single"
      selectable
      responsive
      @row-selected="onUserSelected"
    >
      <template #cell(selected)="{ rowSelected }">
        <template v-if="rowSelected">
          <span aria-hidden="true">&check;</span>
          <span class="sr-only">Selected</span>
        </template>
        <template v-else>
          <span aria-hidden="true">&nbsp;</span>
          <span class="sr-only">Not selected</span>
        </template>
      </template>
      <template #custom-foot>
        <b-tr>
          <b-td colspan="4">
            <b-button
              v-ripple.400="'rgba(255, 255, 255, 0.15)'"
              variant="primary"
              class="mr-1"
              @click="newUser"
            >
              新增
            </b-button>
            <b-button
              v-ripple.400="'rgba(255, 255, 255, 0.15)'"
              variant="primary"
              class="mr-1"
              :disabled="selected.length === 0"
              @click="updateUser"
            >
              更新
            </b-button>
            <b-button
              v-ripple.400="'rgba(255, 255, 255, 0.15)'"
              variant="primary"
              class="mr-1"
              :disabled="selected.length === 0"
              @click="deleteUser"
            >
              刪除
            </b-button>
          </b-td>
        </b-tr>
      </template>
    </b-table>

    <b-modal
      id="userModal"
      :title="modalTitle"
      modal-class="modal-primary"
      hide-footer
      no-close-on-backdrop
      size="lg"
    >
      <user
        :is-new="isNew"
        :current-user="user"
        @updated="onUpdate"
        @created="onRefresh"
      ></user>
    </b-modal>
  </b-card>
</template>
<script>
import Vue from 'vue';
import axios from 'axios';
const Ripple = require('vue-ripple-directive');
import User from './User.vue';
import ToastificationContent from '@core/components/toastification/ToastificationContent.vue';

export default Vue.extend({
  components: {
    User,
  },
  directives: {
    Ripple,
  },
  data() {
    const userList = [];
    const fields = [
      {
        key: 'selected',
        label: '選擇',
      },
      {
        key: '_id',
        label: '帳號',
        sortable: true,
      },
      {
        key: 'name',
        label: '顯示名稱',
        sortable: true,
      },
      {
        key: 'isAdmin',
        label: '管理員',
        sortable: true,
      },
      {
        key: 'group',
        label: '群組',
        sortable: true,
        formatter: this.groupFormatter,
      },
    ];

    const groupList = [];

    return {
      fields,
      userList,
      isNew: true,
      selected: [],
      groupList,
      groupMap: new Map(),
    };
  },
  computed: {
    modalTitle() {
      if (this.isNew) return '新增使用者';

      return '更新使用者';
    },
    user() {
      return this.selected[0];
    },
  },
  async mounted() {
    await this.getGroupList();
    await this.getUserList();
  },

  methods: {
    newUser() {
      this.isNew = true;
      this.$bvModal.show('userModal');
    },
    updateUser() {
      this.isNew = false;
      this.$bvModal.show('userModal');
    },
    deleteUser() {
      this.$bvModal
        .msgBoxConfirm('是否要刪除使用者?')
        .then(ret => {
          if (ret) {
            this.delUser(this.selected[0]._id);
          }
        })
        .catch(err => {
          throw Error(err);
        });
    },
    async getUserList() {
      const res = await axios.get('/User');
      this.userList = res.data;
    },
    async getGroupList() {
      const res = await axios.get('/Groups');
      this.groupList = res.data;
      for (const g of this.groupList) {
        this.groupMap.set(g._id, g);
      }
    },
    groupFormatter(value, key, item) {
      if (this.groupMap.has(value)) return this.groupMap.get(value).name;
      return value;
    },
    onUserSelected(items) {
      this.selected = items;
    },
    delUser(id) {
      axios
        .delete(`/User/${id}`)
        .then(res => {
          const ret = res.data;
          if (ret.ok) {
            this.$toast({
              component: ToastificationContent,
              props: {
                title: '成功',
                icon: 'UserIcon',
              },
            });
          } else {
            this.$toast({
              component: ToastificationContent,
              props: {
                title: '刪除失敗',
                icon: 'UserIcon',
              },
            });
          }
          this.getUserList();
        })
        .catch(err => {
          throw new Error(err);
        });
    },
    onUpdate() {
      this.$bvModal.hide('userModal');
      this.getUserList();
    },
    onRefresh() {
      this.getUserList();
    },
    onDeleted() {
      this.getUserList();
    },
  },
});
</script>
