import { Context } from "aws-lambda";
import Tracer, { SpanContext, SpanOptions, TraceOptions } from "dd-trace";

import { extractTraceContext } from "./context";
import { patchHttp, unpatchHttp } from "./patch-http";
import { TraceContextService } from "./trace-context-service";

export interface TraceConfig {
  /**
   * Whether to automatically patch all outgoing http requests with Datadog's hybrid tracing headers.
   * @default true.
   */
  autoPatchHTTP: boolean;
}

const EARLY_TIMEOUT_THRESHOLD = 50;

export class TraceListener {
  private contextService = new TraceContextService();
  private context?: Context;
  private coldstart = true;
  private timedOut = false;

  public get currentTraceHeaders() {
    return this.contextService.currentTraceHeaders;
  }

  constructor(private config: TraceConfig, private handlerName: string) {}

  public onStartInvocation(event: any, context: Context) {
    if (this.config.autoPatchHTTP) {
      patchHttp(this.contextService);
    }
    this.context = context;

    this.contextService.rootTraceContext = extractTraceContext(event);
  }

  public async onCompleteInvocation() {
    if (this.config.autoPatchHTTP) {
      unpatchHttp();
    }
    this.coldstart = false;
  }

  public onWrap<T = (...args: any[]) => any>(func: T): T {
    const rootTraceContext = this.contextService.currentTraceHeaders;
    const spanContext: SpanContext | null = Tracer.extract("http_headers", rootTraceContext);
    const options: SpanOptions & TraceOptions = {};
    if (this.context) {
      options.tags = {
        cold_start: this.coldstart,
        function_arn: this.context.invokedFunctionArn,
        request_id: this.context.awsRequestId,
        resource_names: this.context.functionName,
      };
    }

    if (spanContext !== null) {
      options.childOf = spanContext;
    }
    const handlerName = this.handlerName;

    const listener = this;

    return async function(this: any, ...args: any[]) {
      const localThis: any = this;
      try {
        return await Tracer.trace(handlerName, options, () => {
          const timeout = listener.timeout();
          const result = (func as any).apply(localThis, args);
          return Promise.race([timeout, result]);
        });
      } finally {
        if (listener.timedOut) {
          // Just wait for the whole lambda process to time out, instead of letting
          // this return. That way, the function will still be recorded as a timeout by
          // X-Ray
          await new Promise(() => {});
        }
      }
    } as any;
  }

  private timeout() {
    const { context } = this;
    if (context === undefined || typeof context.getRemainingTimeInMillis !== "function") {
      // NOOP, never times out
      return new Promise(() => {});
    }
    return new Promise((_, reject) => {
      const timeToWait = context.getRemainingTimeInMillis() - EARLY_TIMEOUT_THRESHOLD;
      setTimeout(() => {
        this.timedOut = true;
        reject("Function timed out");
      }, timeToWait);
    });
  }
}
