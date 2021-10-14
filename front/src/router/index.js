import Vue from 'vue';
import VueRouter from 'vue-router';

Vue.use(VueRouter);

const router = new VueRouter({
  mode: 'hash',
  base: process.env.BASE_URL,
  scrollBehavior() {
    return { x: 0, y: 0 };
  },
  routes: [
    {
      path: '/',
      name: 'home',
      redirect: { name: 'airport-config' },
    },
    {
      path: '/upload',
      name: 'upload',
      component: () => import('@/views/ReportUpload.vue'),
      meta: {
        pageTitle: '檔案上傳',
      },
    },
    {
      path: '/data-management',
      name: 'data-management',
      component: () => import('@/views/SecondPage.vue'),
      meta: {
        pageTitle: '資料管理',
      },
    },
    {
      path: '/import-progress',
      name: 'import-progress',
      component: () => import('@/views/ReportInfo.vue'),
      meta: {
        pageTitle: '資料處理進度',
      },
    },
    {
      path: '/report-management',
      name: 'report-management',
      component: () => import('@/views/ReportManagement.vue'),
      meta: {
        pageTitle: '報表匯出',
      },
    },
    {
      path: '/airport-config',
      name: 'airport-config',
      component: () => import('@/views/AirportConfig.vue'),
      meta: {
        pageTitle: '機場清單設定',
      },
    },
    {
      path: '/export-report',
      name: 'export-report',
      component: () => import('@/views/SecondPage.vue'),
      meta: {
        pageTitle: '報告匯出',
      },
    },
    {
      path: '/user-management',
      name: 'user-management',
      component: () => import('@/views/UserManagement.vue'),
      meta: {
        pageTitle: '使用者管理',
        breadcrumb: [
          {
            text: '系統管理',
            active: true,
          },
          {
            text: '使用者管理',
            active: true,
          },
        ],
      },
    },
    {
      path: '/group-management',
      name: 'group-management',
      component: () => import('@/views/GroupManagement.vue'),
      meta: {
        pageTitle: '群組管理',
        breadcrumb: [
          {
            text: '系統管理',
            active: true,
          },
          {
            text: '群組管理',
            active: true,
          },
        ],
      },
    },
    {
      path: '/login',
      name: 'login',
      component: () => import('@/views/Login.vue'),
      meta: {
        layout: 'full',
      },
    },
    {
      path: '/error-404',
      name: 'error-404',
      component: () => import('@/views/error/Error404.vue'),
      meta: {
        layout: 'full',
      },
    },
    {
      path: '*',
      redirect: 'error-404',
    },
  ],
});

export default router;
