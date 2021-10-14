export default [
  //檔案上傳
  {
    title: '檔案上傳',
    icon: 'UploadCloudIcon',
    route: 'upload',
  },
  {
    title: '資料管理',
    icon: 'DatabaseIcon',
    route: 'data-management',
  },
  {
    title: '資料處理進度',
    icon: 'CoffeeIcon',
    route: 'import-progress',
  },
  {
    title: '機場清單設定',
    icon: 'ListIcon',
    route: 'airport-config',
  },
  {
    title: '報告管理',
    icon: 'DownloadIcon',
    route: 'report-management',
  },
  {
    title: '系統管理',
    icon: 'SettingsIcon',
    children: [
      {
        title: '使用者管理',
        route: 'user-management',
      },
      {
        title: '群組管理',
        route: 'group-management',
      },
    ],
  },
];
