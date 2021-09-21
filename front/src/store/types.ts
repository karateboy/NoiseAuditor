export interface AirportInfoID {
  year: number;
  quarter: number;
  airportID: number;
}

export interface ReportID {
  airpotInfoID: AirportInfoID;
  version: number;
}

export interface RootState {
  isLoading: boolean;
  loadingMessage: string;
  login: boolean;
  activeReportIDs: Array<ReportID>;
}
