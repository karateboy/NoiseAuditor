<template>
  <b-card>
    <b-table
      :items="groupList"
      :fields="fields"
      select-mode="single"
      selectable
      responsive
      @row-selected="onGroupSelected"
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
              @click="newGroup"
            >
              新增
            </b-button>
            <b-button
              v-ripple.400="'rgba(255, 255, 255, 0.15)'"
              variant="primary"
              class="mr-1"
              :disabled="selected.length === 0"
              @click="updateGroup"
            >
              更新
            </b-button>
            <b-button
              v-ripple.400="'rgba(255, 255, 255, 0.15)'"
              variant="primary"
              class="mr-1"
              :disabled="selected.length === 0"
              @click="deleteGroup"
            >
              刪除
            </b-button>
          </b-td>
        </b-tr>
      </template>
    </b-table>

    <b-modal
      id="groupModal"
      :title="modalTitle"
      modal-class="modal-primary"
      hide-footer
      no-close-on-backdrop
      size="lg"
    >
      <group
        :is-new="isNew"
        :current-group="group"
        @updated="onUpdate"
        @created="onRefresh"
      ></group>
    </b-modal>
  </b-card>
</template>
<script>
import Vue from 'vue';
import axios from 'axios';
const Ripple = require('vue-ripple-directive');
import Group from './Group.vue';
import ToastificationContent from '@core/components/toastification/ToastificationContent.vue';

export default Vue.extend({
  components: {
    Group,
  },
  directives: {
    Ripple,
  },
  data() {
    const groupList = [];
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
    ];

    return {
      fields,
      groupList,
      isNew: true,
      selected: [],
    };
  },
  computed: {
    modalTitle() {
      if (this.isNew) return '新增群組';

      return '更新群組';
    },
    group() {
      return this.selected[0];
    },
  },
  mounted() {
    this.getGroupList();
  },

  methods: {
    newGroup() {
      this.isNew = true;
      this.$bvModal.show('groupModal');
    },
    updateGroup() {
      this.isNew = false;
      this.$bvModal.show('groupModal');
    },
    deleteGroup() {
      this.$bvModal
        .msgBoxConfirm('是否要刪除群組?')
        .then(ret => {
          if (ret) {
            this.delGroup(this.selected[0]._id);
          }
        })
        .catch(err => {
          throw Error(err);
        });
    },
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
    onGroupSelected(items) {
      this.selected = items;
    },
    delGroup(id) {
      axios
        .delete(`/Group/${id}`)
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
          this.getGroupList();
        })
        .catch(err => {
          throw new Error(err);
        });
    },
    onUpdate() {
      this.$bvModal.hide('groupModal');
      this.getGroupList();
    },
    onRefresh() {
      this.getGroupList();
    },
    onDeleted() {
      this.getGroupList();
    },
  },
});
</script>
