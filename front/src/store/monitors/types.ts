export interface Monitor {
  _id: string;
  desc: string;
  monitorTypes: Array<string>;
}

export interface MonitorState {
  monitors: Array<Monitor>;
}
