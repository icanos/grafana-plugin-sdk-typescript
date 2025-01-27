#!/usr/bin/env node

import { DataFrame } from '@grafana/data';
import * as proto from './proto/backend_grpc_pb';
import {
  CheckHealthRequest,
  CheckHealthResponse,
  QueryDataRequest,
  QueryDataResponse,
  DataSourceInstanceSettings as InternalDataSourceInstanceSettings,
  CollectMetricsRequest,
  CollectMetricsResponse,
  CallResourceRequest,
  CallResourceResponse,
  DataResponse,
  DataQuery as InternalDataQuery,
  PluginContext as InternalPluginContext,
} from './proto/backend_pb';
import * as grpc from 'grpc';
import { Logger } from './logging';
import { API } from './api';

export {
  CheckHealthRequest,
  CheckHealthResponse,
  QueryDataRequest,
  QueryDataResponse,
  CollectMetricsRequest,
  CollectMetricsResponse,
  CallResourceRequest,
  CallResourceResponse,
  DataResponse,
} from './proto/backend_pb';

import { BackendSrvImpl } from './services/BackendSrvImpl';
import { marshalArrow } from './arrow';
import { RecordBatchFileWriter } from 'apache-arrow';
//import { grafanaDataFrameToArrowTable } from './arrowFrameExtensions';
export { BackendSrvImpl }

export class ApiConnectionManager {
  apiMap: Record<string, API>;
  constructor() {
    this.apiMap = {}
  }

  getAPI(settings: InternalDataSourceInstanceSettings.AsObject): API {
    if (!this.apiMap[settings.url]) {
      this.apiMap[settings.url] = new API(settings);
    }

    return this.apiMap[settings.url];
  }
}

export abstract class DiagnosticsService implements proto.IDiagnosticsServer {
  abstract CheckHealth(request: CheckHealthRequest): Promise<CheckHealthResponse>;
  abstract CollectMetrics(request: CollectMetricsRequest): Promise<CollectMetricsResponse>;
  
  checkHealth = async (call: grpc.ServerUnaryCall<CheckHealthRequest>, callback: grpc.sendUnaryData<CheckHealthResponse>) => {
    let response: CheckHealthResponse = new CheckHealthResponse();
    let err: grpc.ServiceError | null = null;

    try {
      response = await this.CheckHealth(call.request);
    } catch(ex) {
      logger.warn("exception", ex);
      response.setStatus(CheckHealthResponse.HealthStatus.ERROR);
      response.setMessage(ex);
      err = ex;
    }

    callback(err, response);
  }

  collectMetrics = async (call: grpc.ServerUnaryCall<CollectMetricsRequest>, callback: grpc.sendUnaryData<CollectMetricsResponse>) => {
    let response: CollectMetricsResponse = new CollectMetricsResponse();
    let err: grpc.ServiceError | null = null;
    
    try {
      response = await this.CollectMetrics(call.request);
    } catch (ex) {
      logger.warn("exception", ex);
      err = ex;
    }
    callback(err, response);    
  };
}

export interface DataQuery<T> extends InternalDataQuery.AsObject {
  query: T;
}

export interface DataSourceInstanceSettings<T> extends InternalDataSourceInstanceSettings.AsObject {
  json: T;
}

export interface PluginContext<T> extends InternalPluginContext.AsObject {
  datasourceinstancesettings?: DataSourceInstanceSettings<T>;
}

export abstract class DataService<Q,O> implements proto.IDataServer {
  abstract QueryData(request: DataQuery<Q>, pluginContext?: PluginContext<O>): Promise<DataFrame[]>;

  async queryData(call: grpc.ServerUnaryCall<QueryDataRequest>, callback: grpc.sendUnaryData<QueryDataResponse>) {
    const request: QueryDataRequest.AsObject = call.request.toObject();
    const context: InternalPluginContext.AsObject = call.request.toObject().plugincontext!;
    const response: QueryDataResponse = new QueryDataResponse();
    let err: grpc.ServiceError | null = null;

    try {
      if (request.plugincontext?.datasourceinstancesettings) {
        for (let query of request.queriesList) {
          const dataResponse: DataResponse = new DataResponse();
          const jsonString: string = Buffer.from(query.json as string, 'base64').toString('ascii');
          const queryAsQ: Q = JSON.parse(jsonString);
          const contextJson: string = Buffer.from(context.datasourceinstancesettings?.jsondata as string, 'base64').toString('ascii');
          const contextAsO: O = JSON.parse(contextJson);
          const dataFrames: DataFrame[] = await this.QueryData({
            ...query,
            query: queryAsQ,
          }, {
            ...context,
            datasourceinstancesettings: {
              ...context.datasourceinstancesettings!,
              json: contextAsO,
            }
          });

          const frames: Uint8Array[] = [];

          dataFrames.forEach((frame: DataFrame) => {
            const writer = new RecordBatchFileWriter();
            const table = marshalArrow(frame);
            writer.write(table);

            writer.finish();
            const record = writer.toUint8Array(true);
            writer.close();

            frames.push(record);
          });
        
          dataResponse.setFramesList(frames);
          response.getResponsesMap().set(query.refid, dataResponse);
        }
        
      }
    } catch(ex) {
      logger.error("Query data exception", ex);
      err = ex;
    }

    callback(err, response);
  }
}

export abstract class ResourceService implements proto.IResourceServer {
  abstract CallResource(request: CallResourceRequest): Promise<CallResourceResponse>;

  callResource: grpc.handleServerStreamingCall<CallResourceRequest, CallResourceResponse> = async (call: grpc.ServerWritableStream<CallResourceRequest>) => {
    let response: CallResourceResponse = new CallResourceResponse();
    let err: grpc.ServiceError | null = null;

    try {
      response = await this.CallResource(call.request);
    } catch(ex) {
      logger.error("CallResource exception", ex);
      err = ex;
    }

    call.write(response, writeErr => {
      if (err) {
        logger.error('error', err);
        call.emit('error', err)
      }
      if (writeErr) {
        logger.error('write error', writeErr);
        call.emit('write error', writeErr);
      }
      call.end()
    });
  }
}

export class BackendServer {
  server: grpc.Server;
  port: number;
  constructor() {
    this.server = new grpc.Server();
    this.port = this.server.bind('0.0.0.0:0', grpc.ServerCredentials.createInsecure());
  }

  public addDiagnosticsService = (instance: proto.IDiagnosticsServer) => {
    this.server.addService(proto.DiagnosticsService, instance);  
  }

  public addResourceService = (instance: proto.IResourceServer) => {
    this.server.addService(proto.ResourceService, instance);
  }

  public addDataService = (instance: proto.IDataServer) => {
    this.server.addService(proto.DataService, instance);
  }
  
  public run() {
    this.server.start();
    console.log(`1|2|tcp|localhost:${this.port}|grpc`);
  }
}

const logger = new Logger();
grpc.setLogger(logger);
grpc.setLogVerbosity(grpc.logVerbosity.INFO);  

export {
  logger,
  Logger,
  DataFrame,
}