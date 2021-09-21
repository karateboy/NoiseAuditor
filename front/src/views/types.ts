/* eslint-disable camelcase */
import { ReportID } from '@/store/types';

export interface Sensor {
  id: string;
  topic: string;
  monitor: string;
  group: string;
}

export interface Ability {
  action: string;
  subject: string;
}

export interface Group {
  _id: string;
  name: string;
  monitors: Array<string>;
  monitorTypes: Array<string>;
  admin: boolean;
  abilities: Array<Ability>;
  parent: undefined | string;
}

export interface TextStrValue {
  text: string;
  value: string;
}

export interface MonitorTypeStatus {
  _id: string;
  desp: string;
  value: string;
  unit: string;
  instrument: string;
  status: string;
  classStr: Array<string>;
  order: number;
}

export interface ThresholdConfig {
  elapseTime: number;
}

export interface MonitorType {
  _id: string;
  desp: string;
  unit: string;
  prec: number;
  order: number;
  signalType: boolean;
  std_law?: number;
  std_internal?: number;
  zd_internal?: number;
  zd_law?: number;
  span?: number;
  span_dev_internal?: number;
  span_dev_law?: number;
  measuringBy?: Array<string>;
  thresholdConfig?: ThresholdConfig;
}

export interface CellData {
  v: string;
  cellClassName: Array<string>;
  status?: string;
}

export interface RowData {
  date: number;
  cellData: Array<CellData>;
}

export interface StatRow {
  name: string;
  cellData: Array<CellData>;
}

export interface DailyReport {
  columnNames: Array<String>;
  hourRows: Array<RowData>;
  statRows: Array<StatRow>;
}

export interface CalibrationConfig {
  monitorType: string;
  value: number;
}

export interface ThetaConfig {
  calibrations: Array<CalibrationConfig>;
}

export interface Airport {
  _id: number;
  name: string;
}

export interface Terminal {
  no: number;
  name: string;
}

export interface AirportInfoID {
  year: number;
  quarter: number;
  airportID: number;
}

export interface AirportInfo {
  _id: AirportInfoID;
  terminals: Array<Terminal>;
}

export interface SubTask {
  name: string;
  current: number;
  total: number;
}

export interface ReportInfo {
  _id: ReportID;
  year: number;
  quarter: number;
  version: number;
  state: string;
  importLog: string;
  auditLog: string;
  tasks: Array<SubTask>;
}
